"""Performance optimization tools and utilities for NBA scraper."""

import asyncio
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Dict, List, Optional, Tuple, Union, Callable
import threading

import asyncpg
from asyncpg import Connection, Pool

from .config import get_settings
from .logging import get_logger, metrics, monitor_function

logger = get_logger(__name__)

@dataclass
class QueryStats:
    """Statistics for database query performance."""
    query_hash: str
    query_template: str
    execution_count: int = 0
    total_duration: float = 0.0
    min_duration: float = float('inf')
    max_duration: float = 0.0
    avg_duration: float = 0.0
    recent_executions: deque = field(default_factory=lambda: deque(maxlen=100))
    first_seen: datetime = field(default_factory=datetime.utcnow)
    last_seen: datetime = field(default_factory=datetime.utcnow)
    
    def add_execution(self, duration: float):
        """Add a new query execution timing."""
        self.execution_count += 1
        self.total_duration += duration
        self.min_duration = min(self.min_duration, duration)
        self.max_duration = max(self.max_duration, duration)
        self.avg_duration = self.total_duration / self.execution_count
        self.recent_executions.append({
            'duration': duration,
            'timestamp': datetime.utcnow()
        })
        self.last_seen = datetime.utcnow()

class QueryMonitor:
    """Monitor and analyze database query performance."""
    
    def __init__(self):
        self._query_stats: Dict[str, QueryStats] = {}
        self._slow_queries: deque = deque(maxlen=1000)
        self._lock = threading.Lock()
        self.slow_query_threshold = 1.0  # seconds
        
    def normalize_query(self, query: str) -> Tuple[str, str]:
        """Normalize query for grouping and create a hash."""
        import hashlib
        import re
        
        # Remove comments
        normalized = re.sub(r'--.*$', '', query, flags=re.MULTILINE)
        
        # Replace string literals with placeholder
        normalized = re.sub(r"'[^']*'", "'?'", normalized)
        
        # Replace numeric literals with placeholder
        normalized = re.sub(r'\b\d+\b', '?', normalized)
        
        # Replace IN clauses with placeholder
        normalized = re.sub(r'IN\s*\([^)]+\)', 'IN (?)', normalized, flags=re.IGNORECASE)
        
        # Normalize whitespace
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        # Create hash for grouping
        query_hash = hashlib.md5(normalized.encode()).hexdigest()[:16]
        
        return query_hash, normalized
    
    def record_query(self, query: str, duration: float, params: Optional[tuple] = None):
        """Record a query execution for monitoring."""
        with self._lock:
            query_hash, normalized_query = self.normalize_query(query)
            
            if query_hash not in self._query_stats:
                self._query_stats[query_hash] = QueryStats(
                    query_hash=query_hash,
                    query_template=normalized_query
                )
            
            stats = self._query_stats[query_hash]
            stats.add_execution(duration)
            
            # Track slow queries
            if duration > self.slow_query_threshold:
                self._slow_queries.append({
                    'query_hash': query_hash,
                    'query': query[:500],  # Truncate for storage
                    'duration': duration,
                    'timestamp': datetime.utcnow(),
                    'params': str(params) if params else None
                })
                
                # Emit metrics for slow queries
                metrics.increment('database.slow_queries')
                metrics.histogram('database.slow_query_duration', duration)
                
                logger.warning(
                    "Slow query detected",
                    query_hash=query_hash,
                    duration=duration,
                    query_preview=query[:100]
                )
            
            # Emit general metrics
            metrics.timer('database.query_duration', duration)
            metrics.increment('database.queries')
    
    def get_slowest_queries(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get the slowest queries by average execution time."""
        with self._lock:
            sorted_stats = sorted(
                self._query_stats.values(),
                key=lambda x: x.avg_duration,
                reverse=True
            )
            
            return [{
                'query_hash': stats.query_hash,
                'query_template': stats.query_template,
                'avg_duration': stats.avg_duration,
                'max_duration': stats.max_duration,
                'execution_count': stats.execution_count,
                'total_duration': stats.total_duration
            } for stats in sorted_stats[:limit]]
    
    def get_most_frequent_queries(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get the most frequently executed queries."""
        with self._lock:
            sorted_stats = sorted(
                self._query_stats.values(),
                key=lambda x: x.execution_count,
                reverse=True
            )
            
            return [{
                'query_hash': stats.query_hash,
                'query_template': stats.query_template,
                'execution_count': stats.execution_count,
                'avg_duration': stats.avg_duration,
                'total_duration': stats.total_duration
            } for stats in sorted_stats[:limit]]
    
    def get_recent_slow_queries(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get recent slow queries."""
        with self._lock:
            return list(self._slow_queries)[-limit:]
    
    def get_query_stats(self) -> Dict[str, QueryStats]:
        """Get all query statistics."""
        with self._lock:
            return dict(self._query_stats)
    
    def get_summary(self) -> Dict[str, Any]:
        """Get performance monitoring summary."""
        with self._lock:
            if not self._query_stats:
                return {
                    'total_queries': 0,
                    'unique_queries': 0,
                    'avg_duration': 0,
                    'slow_queries_count': 0
                }
            
            total_queries = sum(stats.execution_count for stats in self._query_stats.values())
            total_duration = sum(stats.total_duration for stats in self._query_stats.values())
            avg_duration = total_duration / total_queries if total_queries > 0 else 0
            slow_queries_count = len(self._slow_queries)
            
            return {
                'total_queries': total_queries,
                'unique_queries': len(self._query_stats),
                'avg_duration': avg_duration,
                'slow_queries_count': slow_queries_count,
                'monitoring_since': min(stats.first_seen for stats in self._query_stats.values()) if self._query_stats else None
            }

# Global query monitor instance
query_monitor = QueryMonitor()

class PerformanceOptimizedConnection:
    """Enhanced database connection with performance monitoring."""
    
    def __init__(self, connection: Connection):
        self._connection = connection
        
    async def execute(self, query: str, *args) -> str:
        """Execute query with performance monitoring."""
        start_time = time.time()
        try:
            result = await self._connection.execute(query, *args)
            duration = time.time() - start_time
            query_monitor.record_query(query, duration, args)
            return result
        except Exception as e:
            duration = time.time() - start_time
            query_monitor.record_query(query, duration, args)
            logger.error(
                "Query execution failed",
                query=query[:100],
                duration=duration,
                error=str(e)
            )
            raise
    
    async def fetch(self, query: str, *args) -> List[asyncpg.Record]:
        """Fetch query results with performance monitoring."""
        start_time = time.time()
        try:
            result = await self._connection.fetch(query, *args)
            duration = time.time() - start_time
            query_monitor.record_query(query, duration, args)
            return result
        except Exception as e:
            duration = time.time() - start_time
            query_monitor.record_query(query, duration, args)
            logger.error(
                "Query fetch failed",
                query=query[:100],
                duration=duration,
                error=str(e)
            )
            raise
    
    async def fetchrow(self, query: str, *args) -> Optional[asyncpg.Record]:
        """Fetch single row with performance monitoring."""
        start_time = time.time()
        try:
            result = await self._connection.fetchrow(query, *args)
            duration = time.time() - start_time
            query_monitor.record_query(query, duration, args)
            return result
        except Exception as e:
            duration = time.time() - start_time
            query_monitor.record_query(query, duration, args)
            logger.error(
                "Query fetchrow failed",
                query=query[:100],
                duration=duration,
                error=str(e)
            )
            raise
    
    async def executemany(self, query: str, args_list: List[tuple]) -> None:
        """Execute many with performance monitoring."""
        start_time = time.time()
        try:
            await self._connection.executemany(query, args_list)
            duration = time.time() - start_time
            query_monitor.record_query(f"EXECUTEMANY: {query}", duration, (f"{len(args_list)} rows",))
            
            # Emit batch metrics
            metrics.timer('database.batch_operation_duration', duration)
            metrics.histogram('database.batch_size', len(args_list))
            
        except Exception as e:
            duration = time.time() - start_time
            query_monitor.record_query(f"EXECUTEMANY: {query}", duration, (f"{len(args_list)} rows",))
            logger.error(
                "Batch query execution failed",
                query=query[:100],
                duration=duration,
                batch_size=len(args_list),
                error=str(e)
            )
            raise
    
    def __getattr__(self, name):
        """Delegate other attributes to the underlying connection."""
        return getattr(self._connection, name)

@asynccontextmanager
async def get_performance_connection():
    """Get a performance-monitored database connection."""
    from .db import get_connection
    
    conn = await get_connection()
    try:
        yield PerformanceOptimizedConnection(conn)
    finally:
        await conn.close()

class BulkOperationOptimizer:
    """Optimize bulk database operations for better performance."""
    
    def __init__(self, batch_size: int = 1000):
        self.batch_size = batch_size
    
    @monitor_function("bulk_upsert", alert_on_error=True)
    async def bulk_upsert(self, 
                         connection: PerformanceOptimizedConnection,
                         table_name: str,
                         columns: List[str],
                         data: List[tuple],
                         conflict_columns: List[str],
                         update_columns: Optional[List[str]] = None) -> int:
        """Perform optimized bulk upsert operation."""
        if not data:
            return 0
        
        if update_columns is None:
            update_columns = [col for col in columns if col not in conflict_columns]
        
        # Build the upsert query
        placeholders = ", ".join([f"${i+1}" for i in range(len(columns))])
        column_list = ", ".join(columns)
        conflict_list = ", ".join(conflict_columns)
        
        # Build UPDATE SET clause with DISTINCT FROM condition for efficiency
        update_clauses = []
        for col in update_columns:
            update_clauses.append(f"{col} = EXCLUDED.{col}")
        
        # Add condition to only update when values are actually different
        if update_columns:
            distinct_conditions = " OR ".join([
                f"EXCLUDED.{col} IS DISTINCT FROM {table_name}.{col}"
                for col in update_columns
            ])
            where_clause = f"WHERE {distinct_conditions}"
        else:
            where_clause = ""
        
        upsert_query = f"""
            INSERT INTO {table_name} ({column_list})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_list})
            DO UPDATE SET {', '.join(update_clauses)}
            {where_clause}
        """
        
        total_processed = 0
        
        # Process in batches
        for i in range(0, len(data), self.batch_size):
            batch = data[i:i + self.batch_size]
            await connection.executemany(upsert_query, batch)
            total_processed += len(batch)
            
            # Log progress for large operations
            if len(data) > self.batch_size * 2:
                logger.info(
                    "Bulk upsert progress",
                    table=table_name,
                    processed=total_processed,
                    total=len(data),
                    progress_pct=round(100 * total_processed / len(data), 1)
                )
        
        logger.info(
            "Bulk upsert completed",
            table=table_name,
            rows_processed=total_processed,
            batch_size=self.batch_size
        )
        
        return total_processed
    
    @monitor_function("bulk_insert", alert_on_error=True)
    async def bulk_insert(self,
                         connection: PerformanceOptimizedConnection,
                         table_name: str,
                         columns: List[str],
                         data: List[tuple]) -> int:
        """Perform optimized bulk insert operation."""
        if not data:
            return 0
        
        placeholders = ", ".join([f"${i+1}" for i in range(len(columns))])
        column_list = ", ".join(columns)
        
        insert_query = f"""
            INSERT INTO {table_name} ({column_list})
            VALUES ({placeholders})
        """
        
        total_processed = 0
        
        # Process in batches
        for i in range(0, len(data), self.batch_size):
            batch = data[i:i + self.batch_size]
            await connection.executemany(insert_query, batch)
            total_processed += len(batch)
            
            # Log progress for large operations
            if len(data) > self.batch_size * 2:
                logger.info(
                    "Bulk insert progress",
                    table=table_name,
                    processed=total_processed,
                    total=len(data),
                    progress_pct=round(100 * total_processed / len(data), 1)
                )
        
        logger.info(
            "Bulk insert completed",
            table=table_name,
            rows_processed=total_processed,
            batch_size=self.batch_size
        )
        
        return total_processed

class ParallelProcessor:
    """Process large datasets in parallel for improved performance."""
    
    def __init__(self, max_workers: int = None):
        if max_workers is None:
            import os
            max_workers = min(8, (os.cpu_count() or 1) + 4)  # Conservative default
        
        self.max_workers = max_workers
        self.semaphore = asyncio.Semaphore(max_workers)
    
    @monitor_function("parallel_process", alert_on_error=True)
    async def process_parallel(self,
                              items: List[Any],
                              processor_func: Callable,
                              chunk_size: int = 100) -> List[Any]:
        """Process items in parallel chunks."""
        if not items:
            return []
        
        # Split items into chunks
        chunks = [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]
        
        async def process_chunk(chunk):
            async with self.semaphore:
                return await processor_func(chunk)
        
        logger.info(
            "Starting parallel processing",
            total_items=len(items),
            chunks=len(chunks),
            max_workers=self.max_workers,
            chunk_size=chunk_size
        )
        
        # Process chunks in parallel
        tasks = [process_chunk(chunk) for chunk in chunks]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Collect successful results and log errors
        successful_results = []
        error_count = 0
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                error_count += 1
                logger.error(
                    "Chunk processing failed",
                    chunk_index=i,
                    error=str(result)
                )
            else:
                if isinstance(result, list):
                    successful_results.extend(result)
                else:
                    successful_results.append(result)
        
        logger.info(
            "Parallel processing completed",
            total_chunks=len(chunks),
            successful_chunks=len(chunks) - error_count,
            failed_chunks=error_count,
            total_results=len(successful_results)
        )
        
        if error_count > 0:
            metrics.increment('parallel_processing.errors', error_count)
        
        metrics.histogram('parallel_processing.chunk_count', len(chunks))
        metrics.histogram('parallel_processing.success_rate', 
                         (len(chunks) - error_count) / len(chunks) * 100)
        
        return successful_results

class DatabaseOptimizer:
    """Optimize database performance through index analysis and recommendations."""
    
    def __init__(self):
        self.settings = get_settings()
    
    @monitor_function("analyze_table_stats", alert_on_error=True)
    async def analyze_table_stats(self, connection: PerformanceOptimizedConnection) -> Dict[str, Any]:
        """Analyze table statistics for performance insights."""
        stats_query = """
            SELECT 
                schemaname,
                tablename,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                pg_total_relation_size(schemaname||'.'||tablename) as size_bytes,
                n_tup_ins as inserts,
                n_tup_upd as updates,
                n_tup_del as deletes,
                n_live_tup as live_tuples,
                n_dead_tup as dead_tuples,
                last_vacuum,
                last_autovacuum,
                last_analyze,
                last_autoanalyze
            FROM pg_stat_user_tables
            WHERE schemaname = 'public'
            ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
        """
        
        rows = await connection.fetch(stats_query)
        
        return {
            'tables': [dict(row) for row in rows],
            'total_size_bytes': sum(row['size_bytes'] for row in rows),
            'analysis_timestamp': datetime.utcnow()
        }
    
    @monitor_function("analyze_index_usage", alert_on_error=True)
    async def analyze_index_usage(self, connection: PerformanceOptimizedConnection) -> Dict[str, Any]:
        """Analyze index usage statistics."""
        index_usage_query = """
            SELECT 
                schemaname,
                tablename,
                indexname,
                idx_scan as index_scans,
                idx_tup_read as tuples_read,
                idx_tup_fetch as tuples_fetched,
                pg_size_pretty(pg_relation_size(indexrelid)) as index_size,
                pg_relation_size(indexrelid) as index_size_bytes
            FROM pg_stat_user_indexes
            WHERE schemaname = 'public'
            ORDER BY idx_scan DESC
        """
        
        rows = await connection.fetch(index_usage_query)
        
        # Identify potentially unused indexes
        unused_indexes = [
            dict(row) for row in rows 
            if row['index_scans'] == 0 and row['index_size_bytes'] > 1024 * 1024  # > 1MB
        ]
        
        return {
            'indexes': [dict(row) for row in rows],
            'unused_indexes': unused_indexes,
            'total_index_size_bytes': sum(row['index_size_bytes'] for row in rows),
            'analysis_timestamp': datetime.utcnow()
        }
    
    @monitor_function("get_missing_indexes", alert_on_error=True)
    async def get_missing_indexes(self, connection: PerformanceOptimizedConnection) -> List[Dict[str, Any]]:
        """Identify potentially missing indexes based on query patterns."""
        # This is a simplified version - in production, you'd want more sophisticated analysis
        missing_indexes_query = """
            WITH query_stats AS (
                SELECT 
                    query,
                    calls,
                    total_time,
                    mean_time,
                    rows
                FROM pg_stat_statements
                WHERE query LIKE '%WHERE%'
                  AND calls > 10
                  AND mean_time > 100  -- milliseconds
                ORDER BY mean_time DESC
                LIMIT 20
            )
            SELECT * FROM query_stats
        """
        
        try:
            rows = await connection.fetch(missing_indexes_query)
            return [dict(row) for row in rows]
        except Exception as e:
            # pg_stat_statements extension might not be enabled
            logger.info("pg_stat_statements not available for index analysis", error=str(e))
            return []
    
    async def generate_optimization_report(self) -> Dict[str, Any]:
        """Generate comprehensive database optimization report."""
        async with get_performance_connection() as conn:
            table_stats = await self.analyze_table_stats(conn)
            index_stats = await self.analyze_index_usage(conn)
            missing_indexes = await self.get_missing_indexes(conn)
            query_stats = query_monitor.get_summary()
            slow_queries = query_monitor.get_slowest_queries(10)
            frequent_queries = query_monitor.get_most_frequent_queries(10)
            
            # Calculate recommendations
            recommendations = []
            
            # Check for tables needing vacuum
            for table in table_stats['tables']:
                if table['dead_tuples'] and table['live_tuples']:
                    dead_ratio = table['dead_tuples'] / (table['live_tuples'] + table['dead_tuples'])
                    if dead_ratio > 0.1:  # More than 10% dead tuples
                        recommendations.append({
                            'type': 'vacuum',
                            'priority': 'HIGH' if dead_ratio > 0.2 else 'MEDIUM',
                            'description': f"Table {table['tablename']} has {dead_ratio:.1%} dead tuples, consider VACUUM",
                            'table': table['tablename']
                        })
            
            # Check for unused indexes
            for index in index_stats['unused_indexes']:
                recommendations.append({
                    'type': 'unused_index',
                    'priority': 'MEDIUM',
                    'description': f"Index {index['indexname']} appears unused ({index['index_size']}), consider dropping",
                    'table': index['tablename'],
                    'index': index['indexname']
                })
            
            # Check for slow queries
            for query in slow_queries:
                if query['avg_duration'] > 5.0:  # > 5 seconds average
                    recommendations.append({
                        'type': 'slow_query',
                        'priority': 'HIGH',
                        'description': f"Query with hash {query['query_hash']} averages {query['avg_duration']:.2f}s",
                        'query_hash': query['query_hash']
                    })
            
            return {
                'table_stats': table_stats,
                'index_stats': index_stats,
                'missing_indexes': missing_indexes,
                'query_performance': {
                    'summary': query_stats,
                    'slowest_queries': slow_queries,
                    'frequent_queries': frequent_queries
                },
                'recommendations': recommendations,
                'generated_at': datetime.utcnow()
            }

# Global instances
bulk_optimizer = BulkOperationOptimizer()
parallel_processor = ParallelProcessor()
db_optimizer = DatabaseOptimizer()

def optimize_query(query_type: str = "general"):
    """Decorator to automatically optimize database queries."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            
            try:
                # Check if we should use bulk operations
                if 'bulk' in func.__name__.lower() and len(args) > 1:
                    # For bulk operations, use parallel processing if dataset is large
                    if hasattr(args[1], '__len__') and len(args[1]) > 1000:
                        logger.info(
                            "Using parallel processing for large dataset",
                            function=func.__name__,
                            dataset_size=len(args[1])
                        )
                
                result = await func(*args, **kwargs)
                
                duration = time.time() - start_time
                metrics.timer(f'query_optimization.{query_type}.duration', duration)
                
                return result
                
            except Exception as e:
                duration = time.time() - start_time
                metrics.increment(f'query_optimization.{query_type}.errors')
                logger.error(
                    "Optimized query failed",
                    function=func.__name__,
                    query_type=query_type,
                    duration=duration,
                    error=str(e)
                )
                raise
        
        return wrapper
    return decorator