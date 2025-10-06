"""Enhanced logging, monitoring, and error tracking for NBA scraper."""

import logging
import sys
import time
import uuid
from contextvars import ContextVar
from functools import wraps
from typing import Any, Dict, Optional, Callable, Union
from datetime import datetime, timedelta, UTC
from collections import defaultdict, deque
import asyncio
import threading

import structlog
from structlog.stdlib import LoggerFactory

from .config import get_settings

# Context variables for request tracking
trace_id_var: ContextVar[Optional[str]] = ContextVar("trace_id", default=None)
request_start_time: ContextVar[Optional[float]] = ContextVar("request_start_time", default=None)

# In-memory metrics storage (for basic monitoring without external dependencies)
class MetricsCollector:
    """Thread-safe in-memory metrics collector."""
    
    def __init__(self):
        self._lock = threading.Lock()
        self._counters: Dict[str, int] = defaultdict(int)
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self._timers: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        
    def increment(self, metric_name: str, value: int = 1, tags: Optional[Dict[str, str]] = None):
        """Increment a counter metric."""
        with self._lock:
            key = self._format_metric_key(metric_name, tags)
            self._counters[key] += value
    
    def gauge(self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None):
        """Set a gauge metric."""
        with self._lock:
            key = self._format_metric_key(metric_name, tags)
            self._gauges[key] = value
    
    def histogram(self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None):
        """Record a histogram value."""
        with self._lock:
            key = self._format_metric_key(metric_name, tags)
            self._histograms[key].append(value)
    
    def timer(self, metric_name: str, duration: float, tags: Optional[Dict[str, str]] = None):
        """Record a timing metric."""
        with self._lock:
            key = self._format_metric_key(metric_name, tags)
            self._timers[key].append(duration)
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get all collected metrics."""
        with self._lock:
            return {
                'counters': dict(self._counters),
                'gauges': dict(self._gauges),
                'histograms': {k: list(v) for k, v in self._histograms.items()},
                'timers': {k: list(v) for k, v in self._timers.items()}
            }
    
    def _format_metric_key(self, metric_name: str, tags: Optional[Dict[str, str]]) -> str:
        """Format metric key with tags."""
        if not tags:
            return metric_name
        tag_string = ','.join(f"{k}={v}" for k, v in sorted(tags.items()))
        return f"{metric_name},{tag_string}"

# Global metrics collector
metrics = MetricsCollector()

class AlertManager:
    """Manages alerting for critical errors and threshold breaches."""
    
    def __init__(self):
        self.settings = get_settings()
        self._alert_cooldowns: Dict[str, datetime] = {}
        self._error_counts: Dict[str, int] = defaultdict(int)
        self._last_reset = datetime.now(UTC)
        
    async def send_alert(self, 
                        alert_type: str, 
                        message: str, 
                        severity: str = "warning",
                        context: Optional[Dict[str, Any]] = None):
        """Send an alert through configured channels."""
        # Implement cooldown to prevent alert spam
        cooldown_key = f"{alert_type}:{severity}"
        cooldown_period = timedelta(minutes=15 if severity == "warning" else 5)
        
        now = datetime.now(UTC)
        if cooldown_key in self._alert_cooldowns:
            if now - self._alert_cooldowns[cooldown_key] < cooldown_period:
                return  # Skip alert due to cooldown
        
        self._alert_cooldowns[cooldown_key] = now
        
        # Send to Slack if configured
        if self.settings.api_keys.slack_webhook_url:
            await self._send_slack_alert(alert_type, message, severity, context)
        
        # Send to PagerDuty for critical alerts
        if severity == "critical" and self.settings.api_keys.pagerduty_integration_key:
            await self._send_pagerduty_alert(alert_type, message, context)
    
    async def _send_slack_alert(self, alert_type: str, message: str, severity: str, context: Optional[Dict[str, Any]]):
        """Send alert to Slack webhook."""
        try:
            import httpx
            
            color = {
                "info": "#36a64f",      # Green
                "warning": "#ff9500",   # Orange  
                "error": "#ff0000",     # Red
                "critical": "#8B0000"   # Dark red
            }.get(severity, "#808080")  # Gray default
            
            emoji = {
                "info": "ℹ️",
                "warning": "⚠️", 
                "error": "❌",
                "critical": "🚨"
            }.get(severity, "📢")
            
            payload = {
                "attachments": [{
                    "color": color,
                    "title": f"{emoji} NBA Scraper Alert - {alert_type}",
                    "text": message,
                    "fields": [
                        {"title": "Severity", "value": severity.upper(), "short": True},
                        {"title": "Environment", "value": self.settings.environment.value, "short": True},
                        {"title": "Timestamp", "value": datetime.now(UTC).isoformat(), "short": True}
                    ]
                }]
            }
            
            if context:
                for key, value in context.items():
                    payload["attachments"][0]["fields"].append({
                        "title": key.replace('_', ' ').title(),
                        "value": str(value),
                        "short": True
                    })
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self.settings.api_keys.slack_webhook_url.get_secret_value(),
                    json=payload,
                    timeout=10.0
                )
                response.raise_for_status()
                
        except Exception as e:
            # Log alert failure but don't raise to avoid recursive errors
            structlog.get_logger("alerting").error(
                "Failed to send Slack alert", 
                error=str(e), 
                alert_type=alert_type
            )
    
    async def _send_pagerduty_alert(self, alert_type: str, message: str, context: Optional[Dict[str, Any]]):
        """Send critical alert to PagerDuty."""
        try:
            import httpx
            
            payload = {
                "routing_key": self.settings.api_keys.pagerduty_integration_key.get_secret_value(),
                "event_action": "trigger",
                "payload": {
                    "summary": f"NBA Scraper Critical Alert - {alert_type}",
                    "source": "nba-scraper",
                    "severity": "critical",
                    "custom_details": {
                        "message": message,
                        "environment": self.settings.environment.value,
                        "context": context or {}
                    }
                }
            }
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    "https://events.pagerduty.com/v2/enqueue",
                    json=payload,
                    timeout=10.0
                )
                response.raise_for_status()
                
        except Exception as e:
            structlog.get_logger("alerting").error(
                "Failed to send PagerDuty alert",
                error=str(e),
                alert_type=alert_type
            )
    
    def track_error(self, error_type: str):
        """Track error occurrence for threshold monitoring."""
        now = datetime.now(UTC)
        
        # Reset counters every hour
        if now - self._last_reset > timedelta(hours=1):
            self._error_counts.clear()
            self._last_reset = now
        
        self._error_counts[error_type] += 1
        
        # Check if error rate threshold is breached
        if self._error_counts[error_type] > 10:  # 10 errors per hour threshold
            asyncio.create_task(self.send_alert(
                "High Error Rate",
                f"Error type '{error_type}' has occurred {self._error_counts[error_type]} times in the last hour",
                "error",
                {"error_type": error_type, "count": self._error_counts[error_type]}
            ))

# Global alert manager
alert_manager = AlertManager()

class HealthChecker:
    """Application health monitoring."""
    
    def __init__(self):
        self.settings = get_settings()
        self._health_checks: Dict[str, Callable] = {}
        self._last_check_results: Dict[str, Dict[str, Any]] = {}
    
    def register_check(self, name: str, check_func: Callable):
        """Register a health check function."""
        self._health_checks[name] = check_func
    
    async def run_all_checks(self) -> Dict[str, Any]:
        """Run all registered health checks."""
        results = {}
        overall_healthy = True
        
        for name, check_func in self._health_checks.items():
            try:
                start_time = time.time()
                if asyncio.iscoroutinefunction(check_func):
                    result = await check_func()
                else:
                    result = check_func()
                
                duration = time.time() - start_time
                
                check_result = {
                    "healthy": result.get("healthy", True),
                    "message": result.get("message", "OK"),
                    "duration_ms": round(duration * 1000, 2),
                    "timestamp": datetime.now(UTC).isoformat()
                }
                
                if not check_result["healthy"]:
                    overall_healthy = False
                
                results[name] = check_result
                self._last_check_results[name] = check_result
                
            except Exception as e:
                check_result = {
                    "healthy": False,
                    "message": f"Health check failed: {str(e)}",
                    "duration_ms": 0,
                    "timestamp": datetime.now(UTC).isoformat()
                }
                results[name] = check_result
                self._last_check_results[name] = check_result
                overall_healthy = False
        
        return {
            "status": "healthy" if overall_healthy else "unhealthy",
            "timestamp": datetime.now(UTC).isoformat(),
            "checks": results
        }

# Global health checker
health_checker = HealthChecker()

def get_trace_id() -> str:
    """Get or generate a trace ID for request tracking."""
    trace_id = trace_id_var.get()
    if trace_id is None:
        trace_id = str(uuid.uuid4())[:8]
        trace_id_var.set(trace_id)
    return trace_id

def add_trace_id(logger: Any, method_name: str, event_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Add trace ID and timing info to log events."""
    event_dict["trace_id"] = get_trace_id()
    
    # Add request timing if available
    start_time = request_start_time.get()
    if start_time:
        event_dict["request_duration_ms"] = round((time.time() - start_time) * 1000, 2)
    
    return event_dict

def setup_sentry_integration():
    """Set up Sentry error tracking if configured."""
    settings = get_settings()
    
    if not settings.api_keys.sentry_dsn:
        return
    
    try:
        import sentry_sdk
        from sentry_sdk.integrations.logging import LoggingIntegration
        from sentry_sdk.integrations.asyncio import AsyncioIntegration
        
        sentry_logging = LoggingIntegration(
            level=logging.INFO,        # Capture info and above as breadcrumbs
            event_level=logging.ERROR  # Send errors as events
        )
        
        sentry_sdk.init(
            dsn=settings.api_keys.sentry_dsn.get_secret_value(),
            environment=settings.environment.value,
            release=settings.app_version,
            integrations=[sentry_logging, AsyncioIntegration()],
            traces_sample_rate=0.1 if settings.is_production() else 1.0,
            profiles_sample_rate=0.1 if settings.is_production() else 1.0,
        )
        
        structlog.get_logger("monitoring").info("Sentry integration enabled")
        
    except ImportError:
        structlog.get_logger("monitoring").warning(
            "Sentry SDK not installed, error tracking disabled"
        )
    except Exception as e:
        structlog.get_logger("monitoring").error(
            "Failed to initialize Sentry", error=str(e)
        )

def configure_logging() -> None:
    """Configure enhanced structured logging with monitoring."""
    settings = get_settings()
    
    # Set up processors based on format
    processors = [
        structlog.contextvars.merge_contextvars,
        add_trace_id,
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]
    
    # Add appropriate renderer
    if settings.log_format.value == "json":
        processors.append(structlog.processors.JSONRenderer())
    elif settings.log_format.value == "structured":
        processors.append(structlog.dev.ConsoleRenderer(colors=True))
    else:  # text format
        processors.append(structlog.dev.ConsoleRenderer(colors=False))
    
    # Configure structlog
    structlog.configure(
        processors=processors,
        logger_factory=LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Configure stdlib logging
    log_config = {
        "format": "%(message)s",
        "stream": sys.stdout,
        "level": getattr(logging, settings.log_level.upper()),
    }
    
    # Add file handler if log file path is specified
    if settings.log_file_path:
        from logging.handlers import RotatingFileHandler
        
        # Create log directory if it doesn't exist
        settings.log_file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Configure rotating file handler
        file_handler = RotatingFileHandler(
            settings.log_file_path,
            maxBytes=_parse_size(settings.log_max_size),
            backupCount=settings.log_backup_count
        )
        file_handler.setLevel(getattr(logging, settings.log_level.upper()))
        file_handler.setFormatter(logging.Formatter("%(message)s"))
        
        # Configure root logger with both console and file handlers
        logging.basicConfig(
            level=getattr(logging, settings.log_level.upper()),
            handlers=[
                logging.StreamHandler(sys.stdout),
                file_handler
            ],
            format="%(message)s"
        )
    else:
        logging.basicConfig(**log_config)
    
    # Silence noisy third-party loggers
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    
    # Set up Sentry integration
    setup_sentry_integration()
    
    # Register basic health checks
    _register_basic_health_checks()

def _parse_size(size_str: str) -> int:
    """Parse size string like '100MB' into bytes."""
    size_str = size_str.upper()
    if size_str.endswith('KB'):
        return int(size_str[:-2]) * 1024
    elif size_str.endswith('MB'):
        return int(size_str[:-2]) * 1024 * 1024
    elif size_str.endswith('GB'):
        return int(size_str[:-2]) * 1024 * 1024 * 1024
    else:
        return int(size_str)

def _register_basic_health_checks():
    """Register basic application health checks."""
    
    async def database_health():
        """Check database connectivity."""
        try:
            from .db import check_connection
            is_connected = await check_connection()
            return {
                "healthy": is_connected,
                "message": "Database connection OK" if is_connected else "Database connection failed"
            }
        except Exception as e:
            return {"healthy": False, "message": f"Database check failed: {str(e)}"}
    
    def memory_health():
        """Check memory usage."""
        try:
            import psutil
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            
            return {
                "healthy": memory_percent < 90,
                "message": f"Memory usage: {memory_percent:.1f}%",
                "memory_percent": memory_percent
            }
        except ImportError:
            return {"healthy": True, "message": "Memory monitoring not available (psutil not installed)"}
    
    def disk_health():
        """Check disk space."""
        try:
            import psutil
            disk = psutil.disk_usage('/')
            disk_percent = (disk.used / disk.total) * 100
            
            return {
                "healthy": disk_percent < 90,
                "message": f"Disk usage: {disk_percent:.1f}%",
                "disk_percent": disk_percent
            }
        except ImportError:
            return {"healthy": True, "message": "Disk monitoring not available (psutil not installed)"}
    
    health_checker.register_check("database", database_health)
    health_checker.register_check("memory", memory_health)
    health_checker.register_check("disk", disk_health)

def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Get a structured logger instance."""
    return structlog.get_logger(name)

def set_trace_id(trace_id: str) -> None:
    """Set the trace ID for the current context."""
    trace_id_var.set(trace_id)

def clear_trace_id() -> None:
    """Clear the trace ID from the current context."""
    trace_id_var.set(None)

def start_request_timer() -> None:
    """Start timing a request."""
    request_start_time.set(time.time())

def monitor_function(metric_name: Optional[str] = None, 
                    alert_on_error: bool = False,
                    error_threshold: int = 5):
    """Decorator to monitor function execution with metrics and alerting."""
    def decorator(func: Callable) -> Callable:
        function_name = metric_name or f"{func.__module__}.{func.__name__}"
        
        if asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                start_time = time.time()
                logger = get_logger(func.__module__)
                
                try:
                    result = await func(*args, **kwargs)
                    duration = time.time() - start_time
                    
                    # Record success metrics
                    metrics.increment(f"{function_name}.calls", tags={"status": "success"})
                    metrics.timer(f"{function_name}.duration", duration)
                    
                    logger.debug(
                        f"Function {function_name} completed",
                        duration_ms=round(duration * 1000, 2),
                        function=function_name
                    )
                    
                    return result
                    
                except Exception as e:
                    duration = time.time() - start_time
                    
                    # Record error metrics
                    metrics.increment(f"{function_name}.calls", tags={"status": "error"})
                    metrics.increment(f"{function_name}.errors")
                    metrics.timer(f"{function_name}.duration", duration, tags={"status": "error"})
                    
                    # Track error for alerting
                    if alert_on_error:
                        alert_manager.track_error(function_name)
                    
                    logger.error(
                        f"Function {function_name} failed",
                        error=str(e),
                        duration_ms=round(duration * 1000, 2),
                        function=function_name,
                        exc_info=True
                    )
                    
                    raise
            return async_wrapper
        else:
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                start_time = time.time()
                logger = get_logger(func.__module__)
                
                try:
                    result = func(*args, **kwargs)
                    duration = time.time() - start_time
                    
                    # Record success metrics
                    metrics.increment(f"{function_name}.calls", tags={"status": "success"})
                    metrics.timer(f"{function_name}.duration", duration)
                    
                    logger.debug(
                        f"Function {function_name} completed",
                        duration_ms=round(duration * 1000, 2),
                        function=function_name
                    )
                    
                    return result
                    
                except Exception as e:
                    duration = time.time() - start_time
                    
                    # Record error metrics
                    metrics.increment(f"{function_name}.calls", tags={"status": "error"})
                    metrics.increment(f"{function_name}.errors")
                    metrics.timer(f"{function_name}.duration", duration, tags={"status": "error"})
                    
                    # Track error for alerting
                    if alert_on_error:
                        alert_manager.track_error(function_name)
                    
                    logger.error(
                        f"Function {function_name} failed",
                        error=str(e),
                        duration_ms=round(duration * 1000, 2),
                        function=function_name,
                        exc_info=True
                    )
                    
                    raise
            return sync_wrapper
    return decorator