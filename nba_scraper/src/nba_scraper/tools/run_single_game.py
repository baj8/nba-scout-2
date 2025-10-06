#!/usr/bin/env python3
"""Single game ETL runner with raw payload persistence and comprehensive logging."""

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional

from ..io_clients import IoFacade
from ..pipelines.nba_stats_pipeline import NBAStatsPipeline
from ..nba_logging import get_logger

logger = get_logger(__name__)


def create_default_io_client(client_mode: str = "auto"):
    """Create a default IO client implementation.
    
    Args:
        client_mode: Client selection mode ('auto', 'nba_api', 'raw')
        
    Returns:
        Configured NBA client implementation
        
    Raises:
        RuntimeError: If requested client is not available
    """
    # Try to import available clients in preferred order
    tried_imports = []
    
    # First try the main NBA Stats client that we know exists
    try:
        from ..io_clients.nba_stats import NBAStatsClient
        return create_nba_stats_client_from_env()
    except ImportError as e:
        tried_imports.append(f"NBAStatsClient: {e}")
    
    # Try to find alternative clients if they exist
    if client_mode in ("auto", "nba_api"):
        try:
            # Try common nba_api patterns
            from nba_api.stats.endpoints import scoreboardv2
            # If nba_api is available, we could create a wrapper
            logger.warning("nba_api library found but no NbaApiClient wrapper - using NBAStatsClient")
        except ImportError:
            tried_imports.append("nba_api library not installed")
            if client_mode == "nba_api":
                raise RuntimeError("nba_api library not available. Install with: pip install nba_api")
    
    # If we get here, no client is available
    error_msg = (
        "No IO client available. Ensure NBA Stats client is properly configured. "
        f"Tried: {', '.join(tried_imports)}"
    )
    raise RuntimeError(error_msg)


def create_nba_stats_client_from_env():
    """Create NBAStatsClient with environment configuration."""
    from ..io_clients.nba_stats import NBAStatsClient
    
    # Create client with enhanced configuration
    client = NBAStatsClient()
    
    # Apply environment-based configuration
    raw_mode = os.getenv('NBA_API_RAW_MODE', 'false').lower() == 'true'
    rate_limit = int(os.getenv('NBA_API_RATE_LIMIT', '5'))
    timeout = int(os.getenv('NBA_API_TIMEOUT', '30'))
    proxy = os.getenv('NBA_API_PROXY')
    
    # Set browser-like headers if not already configured
    if not hasattr(client, '_env_configured'):
        client.headers.update({
            'Accept': 'application/json, text/plain, */*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'DNT': '1',
            'Host': 'stats.nba.com',
            'Origin': 'https://www.nba.com',
            'Referer': 'https://www.nba.com/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        client._env_configured = True
    
    logger.info("Created NBAStatsClient from environment", 
                raw_mode=raw_mode, rate_limit=rate_limit, timeout=timeout)
    
    return client


class SingleGameRunner:
    """Runs ETL for a single NBA game with full observability."""
    
    def __init__(self, raw_dir: str = "./raw", log_level: str = "INFO", client_mode: str = "auto"):
        self.raw_dir = Path(raw_dir)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.client_mode = client_mode
        
        # Setup logging
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    async def run_game(self, game_id: str, persist_raw: bool = True) -> Dict[str, Any]:
        """Run ETL for a single game with comprehensive metrics collection.
        
        Args:
            game_id: NBA game ID to process
            persist_raw: Whether to save raw API payloads
            
        Returns:
            Dictionary with execution results and metrics
        """
        start_time = time.time()
        result = {
            'game_id': game_id,
            'success': False,
            'start_time': datetime.now(timezone.utc).isoformat(),
            'raw_payloads_saved': 0,
            'records_processed': {},
            'dq_results': {},
            'errors': []
        }
        
        io_facade = None
        
        try:
            # Initialize IO client and facade
            try:
                io_impl = create_default_io_client(self.client_mode)
                io_facade = IoFacade(impl=io_impl)
                logger.info(f"Initialized IO facade with {type(io_impl).__name__}")
            except RuntimeError as e:
                logger.error(f"Failed to create IO client: {e}")
                result['errors'].append(str(e))
                return result
            
            # Initialize pipeline with IO facade
            pipeline = NBAStatsPipeline(io_facade)
            
            logger.info(f"Starting ETL for game {game_id}")
            
            # Run the pipeline
            pipeline_result = await pipeline.run_single_game(game_id)
            
            # Extract metrics from pipeline result
            result['success'] = pipeline_result.get('success', False)
            result['records_processed'] = pipeline_result.get('records', {})
            result['duration_seconds'] = time.time() - start_time
            
            # Save raw payloads if requested and pipeline succeeded
            if persist_raw and result['success']:
                await self._persist_raw_payloads(game_id, io_facade)
                result['raw_payloads_saved'] = await self._count_raw_files(game_id)
            
            # Run DQ checks
            result['dq_results'] = await self._run_dq_checks(game_id)
            
            # Log results
            self._log_results(result)
            
            # Write to ops log
            await self._write_ops_log(result)
            
            return result
            
        except Exception as e:
            logger.error(f"ETL failed for game {game_id}: {str(e)}")
            result['errors'].append(str(e))
            result['duration_seconds'] = time.time() - start_time
            
            # Only try to save raw payloads on error if we have a working facade
            if persist_raw and io_facade is not None:
                try:
                    await self._persist_raw_payloads(game_id, io_facade)
                    result['raw_payloads_saved'] = await self._count_raw_files(game_id)
                except Exception as persist_error:
                    logger.warning(f"Failed to persist raw payloads: {persist_error}")
            
            await self._write_ops_log(result)
            return result
    
    async def _persist_raw_payloads(self, game_id: str, io_facade: IoFacade) -> None:
        """Save raw API payloads for debugging and compliance."""
        game_dir = self.raw_dir / game_id
        game_dir.mkdir(parents=True, exist_ok=True)
        
        endpoints = [
            ('boxscore', io_facade.fetch_boxscore),
            ('pbp', io_facade.fetch_pbp),
            ('lineups', io_facade.fetch_lineups),
            ('shots', io_facade.fetch_shots)
        ]
        
        for endpoint_name, fetch_func in endpoints:
            try:
                logger.debug(f"Fetching {endpoint_name} for {game_id}")
                raw_data = await fetch_func(game_id)
                
                output_file = game_dir / f"{endpoint_name}.json"
                with open(output_file, 'w') as f:
                    json.dump(raw_data, f, indent=2, default=str)
                
                logger.debug(f"Saved {endpoint_name} raw data to {output_file}")
                
            except Exception as e:
                logger.warning(f"Failed to fetch/save {endpoint_name} for {game_id}: {e}")
                # Save error info
                error_file = game_dir / f"{endpoint_name}_error.json"
                with open(error_file, 'w') as f:
                    json.dump({
                        'error': str(e),
                        'timestamp': datetime.now(timezone.utc).isoformat(),
                        'game_id': game_id,
                        'endpoint': endpoint_name
                    }, f, indent=2)
    
    async def _count_raw_files(self, game_id: str) -> int:
        """Count successfully saved raw payload files."""
        game_dir = self.raw_dir / game_id
        if not game_dir.exists():
            return 0
        
        json_files = list(game_dir.glob("*.json"))
        # Don't count error files
        valid_files = [f for f in json_files if not f.name.endswith('_error.json')]
        return len(valid_files)
    
    async def _run_dq_checks(self, game_id: str) -> Dict[str, Any]:
        """Run data quality checks on the processed game."""
        try:
            from ..db import get_connection
            
            async with get_connection() as conn:
                # Check games table
                games_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM games WHERE game_id = $1", game_id
                )
                
                # Check PBP events
                pbp_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM pbp_events WHERE game_id = $1", game_id
                )
                
                # Check seconds_elapsed coverage
                elapsed_coverage = await conn.fetchval("""
                    SELECT COALESCE(
                        SUM(CASE WHEN seconds_elapsed IS NOT NULL THEN 1 ELSE 0 END)::float 
                        / NULLIF(COUNT(*), 0), 
                        0
                    )
                    FROM pbp_events WHERE game_id = $1
                """, game_id)
                
                # Check shots if present
                shots_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM shots WHERE game_id = $1", game_id
                )
                
                shot_coverage = 0.0
                if shots_count > 0:
                    shot_coverage = await conn.fetchval("""
                        SELECT 
                            SUM(CASE WHEN loc_x IS NOT NULL AND loc_y IS NOT NULL THEN 1 ELSE 0 END)::float 
                            / NULLIF(COUNT(*), 0)
                        FROM shots WHERE game_id = $1
                    """, game_id) or 0.0
                
                return {
                    'games_inserted': games_count,
                    'pbp_events_count': pbp_count,
                    'pbp_elapsed_coverage': round(elapsed_coverage * 100, 2),
                    'shots_count': shots_count,
                    'shot_coord_coverage': round(shot_coverage * 100, 2),
                    'passes_dq_gates': (
                        games_count == 1 and
                        pbp_count >= 400 and
                        elapsed_coverage >= 0.75 and
                        (shots_count == 0 or shot_coverage >= 0.80)
                    )
                }
        
        except Exception as e:
            logger.error(f"DQ checks failed for {game_id}: {e}")
            return {'error': str(e), 'passes_dq_gates': False}
    
    def _log_results(self, result: Dict[str, Any]) -> None:
        """Log execution results in structured format."""
        game_id = result['game_id']
        success = result['success']
        duration = result.get('duration_seconds', 0)
        
        if success:
            logger.info(
                f"✅ Game {game_id} processed successfully in {duration:.2f}s",
                extra={
                    'game_id': game_id,
                    'duration_seconds': duration,
                    'records_processed': result.get('records_processed', {}),
                    'dq_results': result.get('dq_results', {}),
                    'raw_payloads_saved': result.get('raw_payloads_saved', 0)
                }
            )
        else:
            logger.error(
                f"❌ Game {game_id} failed after {duration:.2f}s",
                extra={
                    'game_id': game_id,
                    'duration_seconds': duration,
                    'errors': result.get('errors', [])
                }
            )
    
    async def _write_ops_log(self, result: Dict[str, Any]) -> None:
        """Write result to operations log file."""
        ops_dir = Path("ops")
        ops_dir.mkdir(exist_ok=True)
        
        log_file = ops_dir / "single_game_runs.log"
        
        dq = result.get('dq_results', {})
        
        log_entry = (
            f"[{result.get('start_time', 'N/A')}] Game: {result['game_id']} | "
            f"Success: {result['success']} | "
            f"Duration: {result.get('duration_seconds', 0):.2f}s | "
            f"PBP: {dq.get('pbp_events_count', 0)} events "
            f"({dq.get('pbp_elapsed_coverage', 0):.1f}% elapsed) | "
            f"Shots: {dq.get('shots_count', 0)} "
            f"({dq.get('shot_coord_coverage', 0):.1f}% coords) | "
            f"DQ Pass: {dq.get('passes_dq_gates', False)} | "
            f"Raw Files: {result.get('raw_payloads_saved', 0)} | "
            f"Errors: {len(result.get('errors', []))}\n"
        )
        
        with open(log_file, 'a') as f:
            f.write(log_entry)


async def main():
    """CLI entry point for single game runner."""
    parser = argparse.ArgumentParser(
        description="Run ETL for a single NBA game",
        epilog="""
Environment Variables:
  NBA_API_RAW_MODE     Use raw HTTP mode (default: false)
  NBA_API_RATE_LIMIT   Rate limit per minute (default: 5)
  NBA_API_TIMEOUT      Request timeout in seconds (default: 30)
  NBA_API_PROXY        HTTP proxy URL (optional)

Examples:
  python3 -m nba_scraper.tools.run_single_game --game-id 0022300001 --persist-raw --raw-dir ./raw
  python3 -m nba_scraper.tools.run_single_game --game-id 0022300001 --client auto
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--game-id", required=True, help="NBA game ID (e.g., 0022300001)")
    parser.add_argument("--persist-raw", action="store_true", default=True, 
                       help="Save raw API payloads (default: True)")
    parser.add_argument("--raw-dir", default="./raw", 
                       help="Directory for raw payloads (default: ./raw)")
    parser.add_argument("--client", choices=["auto", "nba_api", "raw"], default="auto",
                       help="Client mode selection (default: auto)")
    parser.add_argument("--log-level", default="INFO", 
                       choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       help="Logging level (default: INFO)")
    
    args = parser.parse_args()
    
    # Run the single game ETL
    runner = SingleGameRunner(
        raw_dir=args.raw_dir, 
        log_level=args.log_level,
        client_mode=args.client
    )
    result = await runner.run_game(args.game_id, persist_raw=args.persist_raw)
    
    # Exit with appropriate code
    if result['success']:
        print(f"✅ Successfully processed game {args.game_id}")
        dq = result.get('dq_results', {})
        if dq.get('passes_dq_gates', False):
            print(f"✅ All DQ gates passed")
        else:
            print(f"⚠️  Some DQ gates failed - check logs")
        sys.exit(0)
    else:
        print(f"❌ Failed to process game {args.game_id}")
        for error in result.get('errors', []):
            print(f"   Error: {error}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())