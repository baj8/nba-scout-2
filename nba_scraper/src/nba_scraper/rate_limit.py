"""Token bucket rate limiter for API requests."""

import asyncio
import time
from typing import Optional

from .config import get_settings
from .logging import get_logger

logger = get_logger(__name__)


class TokenBucket:
    """Token bucket rate limiter with async context manager support."""
    
    def __init__(self, rate: float, capacity: Optional[int] = None) -> None:
        """Initialize token bucket.
        
        Args:
            rate: Tokens per second refill rate
            capacity: Maximum token capacity (defaults to rate)
        """
        self.rate = rate
        self.capacity = capacity or int(rate)
        self.tokens = float(self.capacity)
        self.last_update = time.monotonic()
        self._lock = asyncio.Lock()
    
    async def acquire(self, tokens: int = 1) -> None:
        """Acquire tokens from the bucket, blocking if necessary."""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            
            # Add tokens based on elapsed time
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_update = now
            
            # Wait if insufficient tokens
            if self.tokens < tokens:
                wait_time = (tokens - self.tokens) / self.rate
                logger.debug("Rate limit reached, waiting", wait_time=wait_time)
                await asyncio.sleep(wait_time)
                self.tokens = 0
            else:
                self.tokens -= tokens
    
    async def __aenter__(self) -> "TokenBucket":
        """Acquire token on context entry."""
        await self.acquire()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context exit (no-op)."""
        pass


class RateLimiter:
    """Global rate limiter for all API requests."""
    
    def __init__(self) -> None:
        """Initialize rate limiter from settings."""
        settings = get_settings()
        # Convert requests per minute to requests per second
        rate_per_second = settings.requests_per_min / 60.0
        self._bucket = TokenBucket(rate_per_second)
        logger.info("Rate limiter initialized", 
                   requests_per_min=settings.requests_per_min,
                   tokens_per_second=rate_per_second)
    
    async def acquire(self, tokens: int = 1) -> None:
        """Acquire tokens for making requests."""
        await self._bucket.acquire(tokens)
    
    async def __aenter__(self) -> "RateLimiter":
        """Acquire token on context entry."""
        await self.acquire()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context exit (no-op)."""
        pass


# Global rate limiter instance
_rate_limiter: Optional[RateLimiter] = None


def get_rate_limiter() -> RateLimiter:
    """Get the global rate limiter instance."""
    global _rate_limiter
    if _rate_limiter is None:
        _rate_limiter = RateLimiter()
    return _rate_limiter