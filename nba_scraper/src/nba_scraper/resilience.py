"""Resilience patterns for external API calls - circuit breaker, retry logic, and failure handling."""

import asyncio
import time
from collections import deque
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, Optional, TypeVar, Union
from functools import wraps

from .logging import get_logger, metrics, alert_manager

logger = get_logger(__name__)

T = TypeVar('T')

class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject calls
    HALF_OPEN = "half_open"  # Testing if service recovered

class CircuitBreaker:
    """Circuit breaker pattern implementation for external service calls."""
    
    def __init__(self, 
                 failure_threshold: int = 5,
                 recovery_timeout: float = 60.0,
                 expected_exception: type = Exception,
                 name: str = "circuit_breaker"):
        """
        Initialize circuit breaker.
        
        Args:
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Time to wait before attempting recovery
            expected_exception: Exception type that triggers circuit breaker
            name: Name for metrics and logging
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.name = name
        
        self._failure_count = 0
        self._last_failure_time: Optional[float] = None
        self._state = CircuitState.CLOSED
        self._half_open_calls = 0
        self._max_half_open_calls = 3  # Number of test calls in half-open state
        
    @property
    def state(self) -> CircuitState:
        """Get current circuit breaker state."""
        return self._state
    
    def __call__(self, func: Callable[..., T]) -> Callable[..., T]:
        """Decorator to wrap functions with circuit breaker."""
        if asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs) -> T:
                return await self._call_async(func, *args, **kwargs)
            return async_wrapper
        else:
            @wraps(func)
            def sync_wrapper(*args, **kwargs) -> T:
                return self._call_sync(func, *args, **kwargs)
            return sync_wrapper
    
    async def _call_async(self, func: Callable, *args, **kwargs) -> T:
        """Execute async function with circuit breaker protection."""
        if self._should_reject_call():
            raise CircuitBreakerOpenException(
                f"Circuit breaker '{self.name}' is open. "
                f"Failure threshold ({self.failure_threshold}) exceeded."
            )
        
        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result
            
        except self.expected_exception as e:
            self._on_failure()
            raise
        except Exception as e:
            # Don't count unexpected exceptions towards circuit breaker
            logger.warning(
                "Unexpected exception in circuit breaker",
                circuit_name=self.name,
                error=str(e),
                exception_type=type(e).__name__
            )
            raise
    
    def _call_sync(self, func: Callable, *args, **kwargs) -> T:
        """Execute sync function with circuit breaker protection."""
        if self._should_reject_call():
            raise CircuitBreakerOpenException(
                f"Circuit breaker '{self.name}' is open. "
                f"Failure threshold ({self.failure_threshold}) exceeded."
            )
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
            
        except self.expected_exception as e:
            self._on_failure()
            raise
        except Exception as e:
            # Don't count unexpected exceptions towards circuit breaker
            logger.warning(
                "Unexpected exception in circuit breaker",
                circuit_name=self.name,
                error=str(e),
                exception_type=type(e).__name__
            )
            raise
    
    def _should_reject_call(self) -> bool:
        """Determine if call should be rejected based on circuit state."""
        current_time = time.time()
        
        if self._state == CircuitState.CLOSED:
            return False
            
        elif self._state == CircuitState.OPEN:
            # Check if recovery timeout has passed
            if (self._last_failure_time and 
                current_time - self._last_failure_time >= self.recovery_timeout):
                self._state = CircuitState.HALF_OPEN
                self._half_open_calls = 0
                logger.info(
                    "Circuit breaker transitioning to half-open",
                    circuit_name=self.name
                )
                metrics.increment(f"circuit_breaker.state_change", 
                                tags={"circuit": self.name, "to_state": "half_open"})
                return False
            return True
            
        elif self._state == CircuitState.HALF_OPEN:
            # Allow limited test calls
            if self._half_open_calls >= self._max_half_open_calls:
                return True
            self._half_open_calls += 1
            return False
            
        return False
    
    def _on_success(self):
        """Handle successful call."""
        if self._state == CircuitState.HALF_OPEN:
            # If half-open and success, close the circuit
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            logger.info(
                "Circuit breaker closed after successful recovery",
                circuit_name=self.name
            )
            metrics.increment(f"circuit_breaker.state_change",
                            tags={"circuit": self.name, "to_state": "closed"})
        elif self._state == CircuitState.CLOSED:
            # Reset failure count on success
            self._failure_count = 0
        
        metrics.increment(f"circuit_breaker.calls", 
                        tags={"circuit": self.name, "result": "success"})
    
    def _on_failure(self):
        """Handle failed call."""
        self._failure_count += 1
        self._last_failure_time = time.time()
        
        metrics.increment(f"circuit_breaker.calls",
                        tags={"circuit": self.name, "result": "failure"})
        
        if self._state == CircuitState.CLOSED and self._failure_count >= self.failure_threshold:
            self._state = CircuitState.OPEN
            logger.error(
                "Circuit breaker opened due to failure threshold",
                circuit_name=self.name,
                failure_count=self._failure_count,
                threshold=self.failure_threshold
            )
            metrics.increment(f"circuit_breaker.state_change",
                            tags={"circuit": self.name, "to_state": "open"})
            
            # Send alert for circuit breaker opening
            asyncio.create_task(alert_manager.send_alert(
                "Circuit Breaker Opened",
                f"Circuit breaker '{self.name}' opened after {self._failure_count} failures",
                "error",
                {
                    "circuit_name": self.name,
                    "failure_count": self._failure_count,
                    "threshold": self.failure_threshold
                }
            ))
        
        elif self._state == CircuitState.HALF_OPEN:
            # If half-open and failure, go back to open
            self._state = CircuitState.OPEN
            logger.warning(
                "Circuit breaker reopened during recovery test",
                circuit_name=self.name
            )
            metrics.increment(f"circuit_breaker.state_change",
                            tags={"circuit": self.name, "to_state": "open"})

class CircuitBreakerOpenException(Exception):
    """Exception raised when circuit breaker is open."""
    pass

class RetryConfig:
    """Configuration for retry behavior."""
    
    def __init__(self,
                 max_attempts: int = 3,
                 base_delay: float = 1.0,
                 max_delay: float = 60.0,
                 exponential_base: float = 2.0,
                 jitter: bool = True,
                 backoff_strategy: str = "exponential"):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
        self.backoff_strategy = backoff_strategy

class EnhancedRetry:
    """Enhanced retry mechanism with multiple backoff strategies."""
    
    def __init__(self, config: RetryConfig, name: str = "retry"):
        self.config = config
        self.name = name
    
    def __call__(self, 
                 exceptions: Union[type, tuple] = Exception,
                 on_retry: Optional[Callable] = None) -> Callable:
        """Decorator for adding retry logic to functions."""
        def decorator(func: Callable) -> Callable:
            if asyncio.iscoroutinefunction(func):
                @wraps(func)
                async def async_wrapper(*args, **kwargs):
                    return await self._retry_async(func, exceptions, on_retry, *args, **kwargs)
                return async_wrapper
            else:
                @wraps(func)
                def sync_wrapper(*args, **kwargs):
                    return self._retry_sync(func, exceptions, on_retry, *args, **kwargs)
                return sync_wrapper
        return decorator
    
    async def _retry_async(self, 
                          func: Callable, 
                          exceptions: Union[type, tuple],
                          on_retry: Optional[Callable],
                          *args, **kwargs):
        """Execute async function with retry logic."""
        last_exception = None
        
        for attempt in range(1, self.config.max_attempts + 1):
            try:
                result = await func(*args, **kwargs)
                
                # Log successful retry if this wasn't the first attempt
                if attempt > 1:
                    logger.info(
                        "Function succeeded after retry",
                        function=func.__name__,
                        attempt=attempt,
                        retry_name=self.name
                    )
                    metrics.increment(f"retry.success",
                                    tags={"retry_name": self.name, "attempt": str(attempt)})
                
                return result
                
            except exceptions as e:
                last_exception = e
                
                metrics.increment(f"retry.attempt",
                                tags={"retry_name": self.name, "attempt": str(attempt)})
                
                if attempt == self.config.max_attempts:
                    # Final attempt failed
                    logger.error(
                        "Function failed after all retry attempts",
                        function=func.__name__,
                        attempts=attempt,
                        retry_name=self.name,
                        error=str(e)
                    )
                    metrics.increment(f"retry.exhausted", 
                                    tags={"retry_name": self.name})
                    break
                
                # Calculate delay for next attempt
                delay = self._calculate_delay(attempt)
                
                logger.warning(
                    "Function failed, retrying",
                    function=func.__name__,
                    attempt=attempt,
                    retry_name=self.name,
                    next_delay=delay,
                    error=str(e)
                )
                
                # Call retry callback if provided
                if on_retry:
                    try:
                        if asyncio.iscoroutinefunction(on_retry):
                            await on_retry(attempt, e, delay)
                        else:
                            on_retry(attempt, e, delay)
                    except Exception as callback_error:
                        logger.warning(
                            "Retry callback failed",
                            callback_error=str(callback_error)
                        )
                
                await asyncio.sleep(delay)
            
            except Exception as e:
                # Non-retryable exception
                logger.error(
                    "Non-retryable exception in retry wrapper",
                    function=func.__name__,
                    error=str(e),
                    exception_type=type(e).__name__
                )
                raise
        
        # All attempts exhausted
        raise last_exception
    
    def _retry_sync(self, 
                   func: Callable,
                   exceptions: Union[type, tuple], 
                   on_retry: Optional[Callable],
                   *args, **kwargs):
        """Execute sync function with retry logic."""
        import time as sync_time
        
        last_exception = None
        
        for attempt in range(1, self.config.max_attempts + 1):
            try:
                result = func(*args, **kwargs)
                
                # Log successful retry if this wasn't the first attempt
                if attempt > 1:
                    logger.info(
                        "Function succeeded after retry",
                        function=func.__name__,
                        attempt=attempt,
                        retry_name=self.name
                    )
                    metrics.increment(f"retry.success",
                                    tags={"retry_name": self.name, "attempt": str(attempt)})
                
                return result
                
            except exceptions as e:
                last_exception = e
                
                metrics.increment(f"retry.attempt",
                                tags={"retry_name": self.name, "attempt": str(attempt)})
                
                if attempt == self.config.max_attempts:
                    # Final attempt failed
                    logger.error(
                        "Function failed after all retry attempts",
                        function=func.__name__,
                        attempts=attempt,
                        retry_name=self.name,
                        error=str(e)
                    )
                    metrics.increment(f"retry.exhausted",
                                    tags={"retry_name": self.name})
                    break
                
                # Calculate delay for next attempt
                delay = self._calculate_delay(attempt)
                
                logger.warning(
                    "Function failed, retrying",
                    function=func.__name__,
                    attempt=attempt,
                    retry_name=self.name,
                    next_delay=delay,
                    error=str(e)
                )
                
                # Call retry callback if provided
                if on_retry:
                    try:
                        on_retry(attempt, e, delay)
                    except Exception as callback_error:
                        logger.warning(
                            "Retry callback failed",
                            callback_error=str(callback_error)
                        )
                
                sync_time.sleep(delay)
            
            except Exception as e:
                # Non-retryable exception
                logger.error(
                    "Non-retryable exception in retry wrapper",
                    function=func.__name__,
                    error=str(e),
                    exception_type=type(e).__name__
                )
                raise
        
        # All attempts exhausted
        raise last_exception
    
    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay for retry attempt."""
        import random
        
        if self.config.backoff_strategy == "exponential":
            delay = self.config.base_delay * (self.config.exponential_base ** (attempt - 1))
        elif self.config.backoff_strategy == "linear":
            delay = self.config.base_delay * attempt
        elif self.config.backoff_strategy == "fixed":
            delay = self.config.base_delay
        else:
            delay = self.config.base_delay
        
        # Apply maximum delay limit
        delay = min(delay, self.config.max_delay)
        
        # Add jitter if enabled
        if self.config.jitter:
            jitter_amount = delay * 0.1  # 10% jitter
            delay += random.uniform(-jitter_amount, jitter_amount)
        
        return max(0, delay)  # Ensure non-negative delay

# Pre-configured instances for common use cases
nba_stats_circuit_breaker = CircuitBreaker(
    failure_threshold=5,
    recovery_timeout=60.0,
    expected_exception=Exception,
    name="nba_stats_api"
)

bref_circuit_breaker = CircuitBreaker(
    failure_threshold=3,
    recovery_timeout=30.0,
    expected_exception=Exception,
    name="basketball_reference"
)

gamebooks_circuit_breaker = CircuitBreaker(
    failure_threshold=3,
    recovery_timeout=45.0,
    expected_exception=Exception,
    name="nba_gamebooks"
)

# Pre-configured retry instances
conservative_retry = EnhancedRetry(
    RetryConfig(
        max_attempts=3,
        base_delay=1.0,
        max_delay=30.0,
        exponential_base=2.0,
        jitter=True
    ),
    name="conservative"
)

aggressive_retry = EnhancedRetry(
    RetryConfig(
        max_attempts=5,
        base_delay=0.5,
        max_delay=60.0,
        exponential_base=2.0,
        jitter=True
    ),
    name="aggressive"
)

database_retry = EnhancedRetry(
    RetryConfig(
        max_attempts=2,
        base_delay=0.1,
        max_delay=5.0,
        exponential_base=2.0,
        jitter=False
    ),
    name="database"
)