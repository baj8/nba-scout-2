"""Async HTTP client with rate limiting and retry logic."""

import asyncio
from typing import Any, Dict, Optional

import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from .config import get_settings
from .rate_limit import get_rate_limiter
from .nba_logging import get_logger

logger = get_logger(__name__)


# Global HTTP client instance
_client: Optional[httpx.AsyncClient] = None


def get_client() -> httpx.AsyncClient:
    """Get the global HTTP client instance."""
    global _client
    if _client is None:
        settings = get_settings()
        
        # Configure httpx client with timeouts and headers
        _client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=10.0),
            headers={
                'User-Agent': settings.user_agent,
                'Accept': 'application/json,text/html,application/xhtml+xml',
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'en-US,en;q=0.9',
                'Cache-Control': 'no-cache',
            },
            follow_redirects=True,
            limits=httpx.Limits(
                max_keepalive_connections=10,
                max_connections=20,
            ),
        )
        logger.info("HTTP client initialized")
    
    return _client


async def close_client() -> None:
    """Close the global HTTP client."""
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None
        logger.info("HTTP client closed")


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=60),
    retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.RequestError)),
    reraise=True,
)
async def get(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> httpx.Response:
    """Make an async GET request with rate limiting and retry logic.
    
    Args:
        url: URL to request
        params: Query parameters
        headers: Additional headers
        **kwargs: Additional httpx arguments
        
    Returns:
        HTTP response
        
    Raises:
        httpx.HTTPStatusError: For 4xx/5xx responses after retries
        httpx.RequestError: For network/connection errors after retries
    """
    # Apply rate limiting
    rate_limiter = get_rate_limiter()
    async with rate_limiter:
        client = get_client()
        
        # Merge headers
        request_headers = {}
        if headers:
            request_headers.update(headers)
        
        try:
            logger.debug("Making HTTP request", url=url, params=params)
            
            response = await client.get(
                url, 
                params=params, 
                headers=request_headers,
                **kwargs
            )
            
            # Log response details
            logger.debug("HTTP response received", 
                        url=url, 
                        status_code=response.status_code,
                        content_length=len(response.content))
            
            # Raise for 4xx/5xx status codes
            response.raise_for_status()
            
            return response
            
        except httpx.HTTPStatusError as e:
            logger.warning("HTTP error response", 
                          url=url, 
                          status_code=e.response.status_code,
                          response_text=e.response.text[:500])
            raise
        except httpx.RequestError as e:
            logger.warning("HTTP request error", url=url, error=str(e))
            raise


async def post(
    url: str,
    data: Optional[Any] = None,
    json: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> httpx.Response:
    """Make an async POST request with rate limiting and retry logic.
    
    Args:
        url: URL to request
        data: Form data or raw data
        json: JSON data
        headers: Additional headers
        **kwargs: Additional httpx arguments
        
    Returns:
        HTTP response
    """
    # Apply rate limiting
    rate_limiter = get_rate_limiter()
    async with rate_limiter:
        client = get_client()
        
        # Merge headers
        request_headers = {}
        if headers:
            request_headers.update(headers)
        
        try:
            logger.debug("Making HTTP POST request", url=url)
            
            response = await client.post(
                url,
                data=data,
                json=json,
                headers=request_headers,
                **kwargs
            )
            
            logger.debug("HTTP POST response received",
                        url=url,
                        status_code=response.status_code,
                        content_length=len(response.content))
            
            response.raise_for_status()
            return response
            
        except httpx.HTTPStatusError as e:
            logger.warning("HTTP POST error response",
                          url=url,
                          status_code=e.response.status_code,
                          response_text=e.response.text[:500])
            raise
        except httpx.RequestError as e:
            logger.warning("HTTP POST request error", url=url, error=str(e))
            raise


async def download_file(url: str, chunk_size: int = 8192) -> bytes:
    """Download a file in chunks to avoid memory issues.
    
    Args:
        url: URL to download
        chunk_size: Size of chunks to read
        
    Returns:
        File content as bytes
    """
    rate_limiter = get_rate_limiter()
    async with rate_limiter:
        client = get_client()
        
        logger.info("Downloading file", url=url)
        
        try:
            async with client.stream('GET', url) as response:
                response.raise_for_status()
                
                chunks = []
                total_size = 0
                
                async for chunk in response.aiter_bytes(chunk_size):
                    chunks.append(chunk)
                    total_size += len(chunk)
                
                content = b''.join(chunks)
                
                logger.info("Downloaded file", 
                           url=url, 
                           size=total_size,
                           chunks=len(chunks))
                
                return content
                
        except (httpx.HTTPStatusError, httpx.RequestError) as e:
            logger.error("Failed to download file", url=url, error=str(e))
            raise


class HTTPSession:
    """Context manager for HTTP sessions with automatic cleanup."""
    
    def __init__(self) -> None:
        """Initialize HTTP session."""
        self.client: Optional[httpx.AsyncClient] = None
    
    async def __aenter__(self) -> 'HTTPSession':
        """Enter async context."""
        self.client = get_client()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit async context."""
        # Don't close the global client, just clean up our reference
        self.client = None
    
    async def get(self, url: str, **kwargs) -> httpx.Response:
        """Make GET request using session client."""
        if not self.client:
            raise RuntimeError("HTTP session not initialized")
        
        rate_limiter = get_rate_limiter()
        async with rate_limiter:
            response = await self.client.get(url, **kwargs)
            response.raise_for_status()
            return response
    
    async def post(self, url: str, **kwargs) -> httpx.Response:
        """Make POST request using session client."""
        if not self.client:
            raise RuntimeError("HTTP session not initialized")
        
        rate_limiter = get_rate_limiter()
        async with rate_limiter:
            response = await self.client.post(url, **kwargs)
            response.raise_for_status()
            return response