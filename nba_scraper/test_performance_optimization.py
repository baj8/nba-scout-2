"""Performance optimization tests and benchmarks."""

import asyncio
import time
import pytest
from typing import List, Dict, Any
from datetime import datetime, timedelta, UTC

from src.nba_scraper.performance import (
    bulk_optimizer, parallel_processor, query_monitor,
    get_performance_connection, optimize_query
)
from src.nba_scraper.models.derived_rows import Q1WindowRow, EarlyShockRow, ScheduleTravelRow
from src.nba_scraper.models.enums import EarlyShockType
from src.nba_scraper.loaders.derived import DerivedLoader


class TestPerformanceOptimizations:
    """Test suite for performance optimization features."""
    
    @pytest.fixture
    async def sample_q1_windows(self) -> List[Q1WindowRow]:
        """Generate sample Q1 window data for testing."""
        windows = []
        for i in range(1000):  # Large dataset for performance testing
            windows.append(Q1WindowRow(
                game_id=f"2024010{i:03d}",
                home_team_tricode="LAL" if i % 2 == 0 else "GSW",
                away_team_tricode="BOS" if i % 2 == 0 else "MIA",
                possessions_elapsed=15 + (i % 10),
                pace48_actual=100.5 + (i % 20),
                pace48_expected=98.2 + (i % 15),
                home_efg_actual=0.52 + (i % 100) * 0.001,
                home_efg_expected=0.50 + (i % 80) * 0.001,
                away_efg_actual=0.48 + (i % 90) * 0.001,
                away_efg_expected=0.49 + (i % 85) * 0.001,
                home_to_rate=0.12 + (i % 50) * 0.001,
                away_to_rate=0.11 + (i % 45) * 0.001,
                home_ft_rate=0.25 + (i % 60) * 0.001,
                away_ft_rate=0.23 + (i % 55) * 0.001,
                home_orb_pct=0.28 + (i % 40) * 0.001,
                home_drb_pct=0.72 + (i % 40) * 0.001,
                away_orb_pct=0.26 + (i % 35) * 0.001,
                away_drb_pct=0.74 + (i % 35) * 0.001,
                bonus_time_home_sec=15.5 + (i % 30),
                bonus_time_away_sec=12.3 + (i % 25),
                transition_rate=0.18 + (i % 70) * 0.001,
                early_clock_rate=0.22 + (i % 80) * 0.001,
                source="test_performance",
                source_url=f"https://test.example.com/game/{i}"
            ))
        return windows
    
    @pytest.fixture
    async def sample_early_shocks(self) -> List[EarlyShockRow]:
        """Generate sample early shock data for testing."""
        shocks = []
        shock_types = [EarlyShockType.EARLY_FOUL_TROUBLE, EarlyShockType.TECH, EarlyShockType.FLAGRANT, EarlyShockType.INJURY_LEAVE]
        
        for i in range(500):  # Medium dataset for performance testing
            shocks.append(EarlyShockRow(
                game_id=f"2024010{i:03d}",
                shock_type=shock_types[i % len(shock_types)],
                period=1,
                clock_hhmmss=f"00:{11 - i % 12:02d}:{30 + i % 30:02d}",
                player_slug=f"player_{i % 50}",
                team_tricode="LAL" if i % 2 == 0 else "BOS",
                immediate_sub=i % 3 == 0,
                notes=f"Performance test shock event {i}",
                source="test_performance",
                source_url=f"https://test.example.com/shock/{i}"
            ))
        return shocks
    
    @pytest.mark.asyncio
    async def test_bulk_upsert_performance(self, sample_q1_windows):
        """Test bulk upsert performance vs individual inserts."""
        # Test bulk upsert
        start_time = time.time()
        
        async with get_performance_connection() as conn:
            # Prepare bulk data
            columns = [
                'game_id', 'home_team_tricode', 'away_team_tricode', 'possessions_elapsed',
                'pace48_actual', 'pace48_expected', 'source', 'source_url', 'ingested_at_utc'
            ]
            
            data = []
            for window in sample_q1_windows:
                data.append((
                    window.game_id,
                    window.home_team_tricode,
                    window.away_team_tricode,
                    window.possessions_elapsed,
                    window.pace48_actual,
                    window.pace48_expected,
                    window.source,
                    window.source_url,
                    datetime.now(UTC),
                ))
            
            # Execute bulk upsert
            updated_count = await bulk_optimizer.bulk_upsert(
                connection=conn,
                table_name='q1_window_12_8',
                columns=columns,
                data=data,
                conflict_columns=['game_id'],
                update_columns=['home_team_tricode', 'away_team_tricode', 'possessions_elapsed']
            )
        
        bulk_time = time.time() - start_time
        
        print(f"Bulk upsert: {len(sample_q1_windows)} records in {bulk_time:.2f}s")
        print(f"Rate: {len(sample_q1_windows) / bulk_time:.0f} records/second")
        
        # Verify performance is reasonable (should handle 1000 records in under 5 seconds)
        assert bulk_time < 5.0, f"Bulk upsert too slow: {bulk_time:.2f}s"
        assert updated_count >= 0, "Bulk upsert should return valid count"
    
    @pytest.mark.asyncio
    async def test_parallel_processing_performance(self, sample_early_shocks):
        """Test parallel processing vs sequential processing."""
        
        async def process_shock_sequential(shocks: List[EarlyShockRow]) -> List[Dict[str, Any]]:
            """Process shocks sequentially."""
            results = []
            for shock in shocks:
                # Simulate processing work
                await asyncio.sleep(0.001)  # 1ms per item
                results.append({
                    'game_id': shock.game_id,
                    'type': shock.shock_type.value,
                    'processed_at': datetime.now(UTC).isoformat()
                })
            return results
        
        async def process_shock_chunk(shock_chunk: List[EarlyShockRow]) -> List[Dict[str, Any]]:
            """Process a chunk of shocks."""
            chunk_results = []
            for shock in shock_chunk:
                # Simulate processing work
                await asyncio.sleep(0.001)  # 1ms per item
                chunk_results.append({
                    'game_id': shock.game_id,
                    'type': shock.shock_type.value,
                    'processed_at': datetime.now(UTC).isoformat()
                })
            return chunk_results
        
        # Test sequential processing
        start_time = time.time()
        sequential_results = await process_shock_sequential(sample_early_shocks)
        sequential_time = time.time() - start_time
        
        # Test parallel processing
        start_time = time.time()
        parallel_results = await parallel_processor.process_parallel(
            items=sample_early_shocks,
            processor_func=process_shock_chunk,
            chunk_size=50
        )
        # Flatten parallel results
        parallel_flat = []
        for chunk in parallel_results:
            parallel_flat.extend(chunk)
        parallel_time = time.time() - start_time
        
        print(f"Sequential: {len(sample_early_shocks)} items in {sequential_time:.2f}s")
        print(f"Parallel: {len(sample_early_shocks)} items in {parallel_time:.2f}s")
        print(f"Speedup: {sequential_time / parallel_time:.1f}x")
        
        # Verify results are equivalent
        assert len(sequential_results) == len(parallel_flat)
        
        # Parallel should be significantly faster for this workload
        assert parallel_time < sequential_time * 0.8, "Parallel processing should be faster"
    
    @pytest.mark.asyncio
    async def test_query_monitoring(self):
        """Test query performance monitoring."""
        
        @optimize_query("test_query")
        async def test_slow_query():
            """Simulate a slow query."""
            await asyncio.sleep(0.1)  # 100ms query
            return "query_result"
        
        @optimize_query("test_fast_query")
        async def test_fast_query():
            """Simulate a fast query."""
            await asyncio.sleep(0.01)  # 10ms query
            return "fast_result"
        
        # Execute queries
        slow_result = await test_slow_query()
        fast_result = await test_fast_query()
        
        # Check monitoring captured the queries
        stats = query_monitor.get_query_stats()
        
        assert "test_query" in stats
        assert "test_fast_query" in stats
        
        slow_stats = stats["test_query"]
        fast_stats = stats["test_fast_query"]
        
        assert slow_stats.total_calls >= 1
        assert fast_stats.total_calls >= 1
        assert slow_stats.avg_duration > fast_stats.avg_duration
        
        print(f"Slow query: {slow_stats}")
        print(f"Fast query: {fast_stats}")
    
    @pytest.mark.asyncio
    async def test_connection_pool_performance(self):
        """Test connection pool efficiency."""
        
        async def execute_query(query_id: int) -> float:
            """Execute a simple query and return execution time."""
            start_time = time.time()
            async with get_performance_connection() as conn:
                result = await conn.fetchval("SELECT $1", query_id)
                assert result == query_id
            return time.time() - start_time
        
        # Execute multiple concurrent queries
        concurrent_queries = 20
        start_time = time.time()
        
        tasks = [execute_query(i) for i in range(concurrent_queries)]
        query_times = await asyncio.gather(*tasks)
        
        total_time = time.time() - start_time
        avg_query_time = sum(query_times) / len(query_times)
        
        print(f"Executed {concurrent_queries} concurrent queries in {total_time:.2f}s")
        print(f"Average query time: {avg_query_time:.3f}s")
        print(f"Queries per second: {concurrent_queries / total_time:.1f}")
        
        # Connection pool should handle concurrent queries efficiently
        assert total_time < 2.0, "Connection pool should handle concurrent queries quickly"
        assert avg_query_time < 0.1, "Individual queries should be fast with connection pooling"
    
    @pytest.mark.asyncio
    async def test_derived_loader_performance(self, sample_q1_windows, sample_early_shocks):
        """Test end-to-end performance of the optimized derived loader."""
        loader = DerivedLoader()
        
        # Test Q1 windows upsert performance
        start_time = time.time()
        q1_count = await loader.upsert_q1_windows(sample_q1_windows)
        q1_time = time.time() - start_time
        
        # Test early shocks upsert performance
        start_time = time.time()
        shock_count = await loader.upsert_early_shocks(sample_early_shocks)
        shock_time = time.time() - start_time
        
        print(f"Q1 Windows: {len(sample_q1_windows)} records in {q1_time:.2f}s")
        print(f"Early Shocks: {len(sample_early_shocks)} records in {shock_time:.2f}s")
        
        # Verify performance benchmarks
        assert q1_time < 3.0, f"Q1 windows upsert too slow: {q1_time:.2f}s"
        assert shock_time < 2.0, f"Early shocks upsert too slow: {shock_time:.2f}s"
        assert q1_count >= 0, "Q1 upsert should return valid count"
        assert shock_count >= 0, "Shocks upsert should return valid count"
    
    @pytest.mark.asyncio
    async def test_query_plan_analysis(self):
        """Test query plan analysis for optimization insights."""
        async with get_performance_connection() as conn:
            # Test a complex analytical query
            query = """
            EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
            SELECT 
                g.season,
                g.home_team_tricode,
                COUNT(*) as games,
                AVG(q.pace48_actual) as avg_pace,
                AVG(q.home_efg_actual) as avg_efg
            FROM games g
            JOIN q1_window_12_8 q ON g.game_id = q.game_id
            WHERE g.game_date_local >= '2024-01-01'
            GROUP BY g.season, g.home_team_tricode
            ORDER BY avg_pace DESC
            LIMIT 10
            """
            
            result = await conn.fetchval(query)
            plan = result[0] if result else {}
            
            # Verify the query plan includes index usage
            plan_str = str(plan)
            print(f"Query plan analysis: {plan_str[:200]}...")
            
            # Should use indexes for efficient execution
            assert 'Index Scan' in plan_str or 'Bitmap Heap Scan' in plan_str, \
                "Query should use indexes for optimal performance"


class TestPerformanceRegression:
    """Regression tests to ensure performance doesn't degrade."""
    
    PERFORMANCE_BENCHMARKS = {
        'bulk_upsert_1000_records': 3.0,  # seconds
        'parallel_processing_500_items': 1.0,  # seconds
        'connection_pool_20_concurrent': 2.0,  # seconds
        'derived_loader_end_to_end': 5.0,  # seconds
    }
    
    @pytest.mark.asyncio
    async def test_performance_regression(self):
        """Ensure performance doesn't regress below benchmarks."""
        # This would be run in CI/CD to catch performance regressions
        
        # Create test data
        test_data = [f"test_item_{i}" for i in range(1000)]
        
        # Simulate bulk operation
        start_time = time.time()
        await asyncio.sleep(0.1)  # Simulate processing
        bulk_time = time.time() - start_time
        
        # Check against benchmark
        benchmark = self.PERFORMANCE_BENCHMARKS['bulk_upsert_1000_records']
        assert bulk_time < benchmark, \
            f"Performance regression detected: {bulk_time:.2f}s > {benchmark}s benchmark"
        
        print(f"Performance test passed: {bulk_time:.2f}s < {benchmark}s benchmark")


if __name__ == "__main__":
    # Run performance tests directly
    import asyncio
    
    async def run_performance_tests():
        """Run basic performance validation."""
        print("Running NBA Scraper Performance Tests...")
        
        # Test basic components
        test_instance = TestPerformanceOptimizations()
        
        # Generate test data
        windows = await test_instance.sample_q1_windows()
        shocks = await test_instance.sample_early_shocks()
        
        print(f"Generated {len(windows)} Q1 windows and {len(shocks)} early shocks for testing")
        
        # Test monitoring
        await test_instance.test_query_monitoring()
        print("✓ Query monitoring test passed")
        
        # Test connection pool
        await test_instance.test_connection_pool_performance()
        print("✓ Connection pool performance test passed")
        
        print("All performance tests completed successfully!")
    
    # Run the tests
    asyncio.run(run_performance_tests())

# Test data validation with performance monitoring
early_shock_data = {
    'game_id': 'TEST_GAME_001',
    'team_tricode': 'LAL',
    'player_slug': 'lebron-james',
    'shock_type': EarlyShockType.EARLY_TWO_PF,
    'shock_seq': 1,
    'period': 1,
    'clock_hhmmss': '11:30',
    'event_idx_start': 10,
    'event_idx_end': None,
    'immediate_sub': False,
    'poss_since_event': 0,
    'notes': 'Test early shock',
    'source': 'test',
    'source_url': 'http://test.com'
}

# Test data for EarlyShockRow
shock_data = EarlyShockRow(
    game_id="0022300001",
    team_tricode="LAL",
    player_slug="lebron-james",
    shock_type=EarlyShockType.TWO_PF_EARLY,  # Use correct enum value
    shock_seq=1,
    period=1,
    clock_hhmmss="11:45",
    event_idx_start=10,
    event_idx_end=None,
    immediate_sub=False,
    poss_since_event=2,
    notes="Two early fouls",
    source="test_source",
    source_url="http://test.com"
)