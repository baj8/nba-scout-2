"""Monitoring endpoints for health checks and metrics exposure."""

import asyncio
import json
import time
from datetime import datetime, UTC
from typing import Dict, Any, Optional
from urllib.parse import parse_qs, urlparse

from .config import get_settings
from .nba_logging import health_checker, metrics, get_logger

logger = get_logger(__name__)

class MonitoringServer:
    """Simple HTTP server for monitoring endpoints."""
    
    def __init__(self):
        self.settings = get_settings()
        self._server: Optional[asyncio.Server] = None
        
    async def start(self):
        """Start the monitoring server."""
        if not self.settings.monitoring.enable_health_checks:
            logger.info("Health checks disabled, monitoring server not started")
            return
            
        try:
            self._server = await asyncio.start_server(
                self._handle_request,
                '0.0.0.0',
                self.settings.monitoring.health_check_port
            )
            
            logger.info(
                "Monitoring server started",
                port=self.settings.monitoring.health_check_port
            )
            
        except Exception as e:
            logger.error("Failed to start monitoring server", error=str(e))
            raise
    
    async def stop(self):
        """Stop the monitoring server."""
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            logger.info("Monitoring server stopped")
    
    async def _handle_request(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle incoming HTTP requests."""
        try:
            # Read HTTP request
            request_line = await reader.readline()
            if not request_line:
                return
                
            request_line = request_line.decode('utf-8').strip()
            method, path, _ = request_line.split(' ', 2)
            
            # Read headers (we don't need them for monitoring endpoints)
            while True:
                header = await reader.readline()
                if not header or header == b'\r\n':
                    break
            
            # Route request
            if path == '/health':
                await self._handle_health(writer)
            elif path == '/health/ready':
                await self._handle_readiness(writer)
            elif path == '/health/live':
                await self._handle_liveness(writer)
            elif path == '/metrics':
                await self._handle_metrics(writer)
            elif path.startswith('/metrics/'):
                await self._handle_specific_metrics(writer, path)
            else:
                await self._handle_not_found(writer)
                
        except Exception as e:
            logger.error("Error handling monitoring request", error=str(e))
            await self._handle_error(writer, str(e))
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except:
                pass
    
    async def _handle_health(self, writer: asyncio.StreamWriter):
        """Handle /health endpoint - comprehensive health check."""
        try:
            health_status = await health_checker.run_all_checks()
            
            status_code = 200 if health_status['status'] == 'healthy' else 503
            response_body = json.dumps(health_status, indent=2)
            
            await self._send_response(writer, status_code, response_body, 'application/json')
            
        except Exception as e:
            await self._handle_error(writer, f"Health check failed: {str(e)}")
    
    async def _handle_readiness(self, writer: asyncio.StreamWriter):
        """Handle /health/ready endpoint - readiness probe."""
        try:
            # Check if application is ready to serve traffic
            # For NBA scraper, this means database is accessible
            from .db import check_connection
            
            is_ready = await check_connection()
            
            if is_ready:
                response = {
                    "status": "ready",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "message": "Application is ready to serve traffic"
                }
                await self._send_response(writer, 200, json.dumps(response), 'application/json')
            else:
                response = {
                    "status": "not_ready",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "message": "Database connection not available"
                }
                await self._send_response(writer, 503, json.dumps(response), 'application/json')
                
        except Exception as e:
            await self._handle_error(writer, f"Readiness check failed: {str(e)}")
    
    async def _handle_liveness(self, writer: asyncio.StreamWriter):
        """Handle /health/live endpoint - liveness probe."""
        # Simple liveness check - if we can respond, we're alive
        response = {
            "status": "alive",
            "timestamp": datetime.now(UTC).isoformat(),
            "uptime_seconds": time.time() - self._start_time if hasattr(self, '_start_time') else 0
        }
        await self._send_response(writer, 200, json.dumps(response), 'application/json')
    
    async def _handle_metrics(self, writer: asyncio.StreamWriter):
        """Handle /metrics endpoint - all metrics in JSON format."""
        try:
            all_metrics = metrics.get_metrics()
            
            # Add timestamp and metadata
            metrics_response = {
                "timestamp": datetime.now(UTC).isoformat(),
                "metrics": all_metrics,
                "metadata": {
                    "environment": self.settings.environment.value,
                    "app_name": self.settings.app_name,
                    "app_version": self.settings.app_version
                }
            }
            
            response_body = json.dumps(metrics_response, indent=2)
            await self._send_response(writer, 200, response_body, 'application/json')
            
        except Exception as e:
            await self._handle_error(writer, f"Metrics collection failed: {str(e)}")
    
    async def _handle_specific_metrics(self, writer: asyncio.StreamWriter, path: str):
        """Handle specific metrics endpoints like /metrics/counters."""
        try:
            metric_type = path.split('/')[-1]  # Extract the metric type from path
            all_metrics = metrics.get_metrics()
            
            if metric_type in all_metrics:
                response = {
                    "timestamp": datetime.now(UTC).isoformat(),
                    "metric_type": metric_type,
                    "data": all_metrics[metric_type]
                }
                response_body = json.dumps(response, indent=2)
                await self._send_response(writer, 200, response_body, 'application/json')
            else:
                await self._handle_not_found(writer)
                
        except Exception as e:
            await self._handle_error(writer, f"Specific metrics failed: {str(e)}")
    
    async def _handle_not_found(self, writer: asyncio.StreamWriter):
        """Handle 404 responses."""
        response = {
            "error": "Not Found",
            "message": "Available endpoints: /health, /health/ready, /health/live, /metrics",
            "timestamp": datetime.now(UTC).isoformat()
        }
        await self._send_response(writer, 404, json.dumps(response), 'application/json')
    
    async def _handle_error(self, writer: asyncio.StreamWriter, error_message: str):
        """Handle error responses."""
        response = {
            "error": "Internal Server Error",
            "message": error_message,
            "timestamp": datetime.now(UTC).isoformat()
        }
        await self._send_response(writer, 500, json.dumps(response), 'application/json')
    
    async def _send_response(self, writer: asyncio.StreamWriter, status_code: int, body: str, content_type: str = 'text/plain'):
        """Send HTTP response."""
        status_text = {
            200: 'OK',
            404: 'Not Found',
            500: 'Internal Server Error',
            503: 'Service Unavailable'
        }.get(status_code, 'Unknown')
        
        response = (
            f"HTTP/1.1 {status_code} {status_text}\r\n"
            f"Content-Type: {content_type}\r\n"
            f"Content-Length: {len(body.encode('utf-8'))}\r\n"
            f"Connection: close\r\n"
            f"\r\n"
            f"{body}"
        )
        
        writer.write(response.encode('utf-8'))
        await writer.drain()

# Global monitoring server instance
monitoring_server = MonitoringServer()

class PrometheusMetricsExporter:
    """Export metrics in Prometheus format."""
    
    def __init__(self):
        self.settings = get_settings()
    
    def export_metrics(self) -> str:
        """Export all metrics in Prometheus format."""
        all_metrics = metrics.get_metrics()
        prometheus_lines = []
        
        # Add metadata
        prometheus_lines.append(f"# NBA Scraper Metrics - {datetime.now(UTC).isoformat()}")
        prometheus_lines.append(f"# Environment: {self.settings.environment.value}")
        prometheus_lines.append("")
        
        # Export counters
        for metric_name, value in all_metrics.get('counters', {}).items():
            clean_name = self._clean_metric_name(metric_name)
            prometheus_lines.append(f"# TYPE {clean_name} counter")
            prometheus_lines.append(f"{clean_name} {value}")
        
        # Export gauges
        for metric_name, value in all_metrics.get('gauges', {}).items():
            clean_name = self._clean_metric_name(metric_name)
            prometheus_lines.append(f"# TYPE {clean_name} gauge")
            prometheus_lines.append(f"{clean_name} {value}")
        
        # Export histogram summaries
        for metric_name, values in all_metrics.get('histograms', {}).items():
            if values:
                clean_name = self._clean_metric_name(metric_name)
                prometheus_lines.append(f"# TYPE {clean_name} histogram")
                
                # Calculate basic statistics
                sorted_values = sorted(values)
                count = len(values)
                total = sum(values)
                
                prometheus_lines.append(f"{clean_name}_count {count}")
                prometheus_lines.append(f"{clean_name}_sum {total}")
                
                # Add quantiles
                if count > 0:
                    p50_idx = int(0.5 * count)
                    p95_idx = int(0.95 * count)
                    p99_idx = int(0.99 * count)
                    
                    prometheus_lines.append(f'{clean_name}{{quantile="0.5"}} {sorted_values[min(p50_idx, count-1)]}')
                    prometheus_lines.append(f'{clean_name}{{quantile="0.95"}} {sorted_values[min(p95_idx, count-1)]}')
                    prometheus_lines.append(f'{clean_name}{{quantile="0.99"}} {sorted_values[min(p99_idx, count-1)]}')
        
        # Export timer summaries (similar to histograms)
        for metric_name, values in all_metrics.get('timers', {}).items():
            if values:
                clean_name = self._clean_metric_name(metric_name)
                prometheus_lines.append(f"# TYPE {clean_name}_duration_seconds summary")
                
                sorted_values = sorted(values)
                count = len(values)
                total = sum(values)
                
                prometheus_lines.append(f"{clean_name}_duration_seconds_count {count}")
                prometheus_lines.append(f"{clean_name}_duration_seconds_sum {total}")
        
        return "\n".join(prometheus_lines)
    
    def _clean_metric_name(self, name: str) -> str:
        """Clean metric name for Prometheus format."""
        # Replace invalid characters with underscores
        import re
        clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        
        # Ensure it starts with a letter or underscore
        if clean_name and clean_name[0].isdigit():
            clean_name = f"metric_{clean_name}"
        
        return clean_name.lower()

# Global Prometheus exporter
prometheus_exporter = PrometheusMetricsExporter()

async def start_monitoring():
    """Start all monitoring services."""
    settings = get_settings()
    
    if settings.monitoring.enable_health_checks:
        monitoring_server._start_time = time.time()
        await monitoring_server.start()
        logger.info("Monitoring services started")
    else:
        logger.info("Monitoring services disabled")

async def stop_monitoring():
    """Stop all monitoring services."""
    await monitoring_server.stop()
    logger.info("Monitoring services stopped")

def get_health_status() -> Dict[str, Any]:
    """Get current health status synchronously."""
    try:
        # This is a simplified sync version for CLI commands
        return {
            "status": "unknown",
            "message": "Use async health_checker.run_all_checks() for full status",
            "timestamp": datetime.now(UTC).isoformat()
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Health check failed: {str(e)}",
            "timestamp": datetime.now(UTC).isoformat()
        }

def get_metrics_summary() -> Dict[str, Any]:
    """Get a summary of current metrics."""
    try:
        all_metrics = metrics.get_metrics()
        
        summary = {
            "counters_count": len(all_metrics.get('counters', {})),
            "gauges_count": len(all_metrics.get('gauges', {})),
            "histograms_count": len(all_metrics.get('histograms', {})),
            "timers_count": len(all_metrics.get('timers', {})),
            "timestamp": datetime.now(UTC).isoformat()
        }
        
        # Add some key metrics if available
        counters = all_metrics.get('counters', {})
        if counters:
            summary['sample_counters'] = dict(list(counters.items())[:5])  # First 5 counters
        
        return summary
        
    except Exception as e:
        return {
            "error": f"Failed to get metrics summary: {str(e)}",
            "timestamp": datetime.now(UTC).isoformat()
        }