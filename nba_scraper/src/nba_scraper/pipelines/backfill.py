#!/usr/bin/env python3
"""Batch backfill pipeline for NBA historical data with comprehensive orchestration."""

import argparse
import asyncio
import json
import logging
import random
import sys
import time
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional, Set
from collections import defaultdict

from ..io_clients import IoFacade
from ..pipelines.nba_stats_pipeline import NBAStatsPipeline
from ..nba_logging import get_logger
from ..utils.date_norm import derive_season_from_date

logger = get_logger(__name__)


class BackfillOrchestrator:
    """Orchestrates backfill operations for NBA data."""
    
    def __init__(
        self,
        rate_limit: float = 5.0,
        batch_size: int = 100,
        raw_root: str = "raw"
    ):
        """Initialize backfill orchestrator.
        
        Args:
            rate_limit: API rate limit (requests per second)
            batch_size: Number of games to process in each batch
            raw_root: Root directory for raw data storage
        """
        self.rate_limit = rate_limit
        self.batch_size = batch_size
        self.raw_root = raw_root
        
    async def backfill_seasons(
        self,
        seasons: List[str],
        dry_run: bool = False,
        retry_quarantined: bool = False
    ) -> Dict[str, Any]:
        """Backfill data for multiple seasons.
        
        Args:
            seasons: List of season strings (e.g., ['2021-22', '2022-23'])
            dry_run: If True, only log what would be done
            retry_quarantined: If True, retry previously failed items
            
        Returns:
            Summary of backfill results
        """
        logger.info("Starting season backfill", seasons=seasons, dry_run=dry_run)
        
        summary = {
            "seasons_processed": [],
            "total_dates": 0,
            "total_games": 0,
            "errors": []
        }
        
        for season in seasons:
            try:
                logger.info("Processing season", season=season)
                
                if dry_run:
                    logger.info("DRY RUN: Would backfill season", season=season)
                    summary["seasons_processed"].append({
                        "season": season,
                        "status": "dry_run",
                        "dates": 0,
                        "games": 0
                    })
                    continue
                
                # Get date range for season
                start_date, end_date = self._get_season_date_range(season)
                
                # Backfill the date range
                season_result = await self.backfill_date_range(
                    start_date, end_date, dry_run, retry_quarantined
                )
                
                season_summary = {
                    "season": season,
                    "status": "completed",
                    "dates": season_result.get("dates_processed", 0),
                    "games": season_result.get("total_games", 0)
                }
                
                summary["seasons_processed"].append(season_summary)
                summary["total_dates"] += season_summary["dates"]
                summary["total_games"] += season_summary["games"]
                
                if season_result.get("errors"):
                    summary["errors"].extend(season_result["errors"])
                
                logger.info("Season completed", season=season, **season_summary)
                
            except Exception as e:
                error_msg = f"Season {season} failed: {e}"
                logger.error("Season backfill failed", season=season, error=str(e))
                summary["errors"].append(error_msg)
                
                summary["seasons_processed"].append({
                    "season": season,
                    "status": "failed",
                    "dates": 0,
                    "games": 0
                })
        
        logger.info("Backfill complete", summary=summary)
        return summary
    
    async def backfill_date_range(
        self,
        start_date: str,
        end_date: str,
        dry_run: bool = False,
        retry_quarantined: bool = False
    ) -> Dict[str, Any]:
        """Backfill data for a date range.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            dry_run: If True, only log what would be done
            retry_quarantined: If True, retry previously failed items
            
        Returns:
            Summary of backfill results
        """
        logger.info("Starting date range backfill", 
                   start_date=start_date, end_date=end_date, dry_run=dry_run)
        
        summary = {
            "start_date": start_date,
            "end_date": end_date,
            "dates_processed": 0,
            "total_games": 0,
            "errors": []
        }
        
        # Generate date list
        dates = self._generate_date_range(start_date, end_date)
        
        for date_str in dates:
            try:
                logger.info("Processing date", date=date_str)
                
                if dry_run:
                    logger.info("DRY RUN: Would process date", date=date_str)
                    summary["dates_processed"] += 1
                    continue
                
                # Process the date (raw harvest + silver load)
                date_result = await self._process_date(date_str, retry_quarantined)
                
                summary["dates_processed"] += 1
                summary["total_games"] += date_result.get("games", 0)
                
                if date_result.get("errors"):
                    summary["errors"].extend(date_result["errors"])
                
                logger.info("Date completed", date=date_str, 
                           games=date_result.get("games", 0))
                
            except Exception as e:
                error_msg = f"Date {date_str} failed: {e}"
                logger.error("Date processing failed", date=date_str, error=str(e))
                summary["errors"].append(error_msg)
        
        logger.info("Date range backfill complete", summary=summary)
        return summary
    
    async def _process_date(self, date_str: str, retry_quarantined: bool) -> Dict[str, Any]:
        """Process a single date (raw harvest + silver load).
        
        Args:
            date_str: Date in YYYY-MM-DD format
            retry_quarantined: Whether to retry failed items
            
        Returns:
            Processing summary
        """
        summary = {"date": date_str, "games": 0, "errors": []}
        
        # Step 1: Raw harvest (Bronze layer)
        try:
            raw_result = await self._run_raw_harvest(date_str)
            summary["games"] = raw_result.get("games_found", 0)
            
            if raw_result.get("errors"):
                summary["errors"].extend([f"Raw: {e}" for e in raw_result["errors"]])
                
        except Exception as e:
            error_msg = f"Raw harvest failed for {date_str}: {e}"
            summary["errors"].append(error_msg)
            return summary
        
        # Step 2: Silver load (if raw harvest succeeded)
        if summary["games"] > 0:
            try:
                silver_result = await self._run_silver_load(date_str)
                
                if silver_result.get("errors"):
                    summary["errors"].extend([f"Silver: {e}" for e in silver_result["errors"]])
                    
            except Exception as e:
                error_msg = f"Silver load failed for {date_str}: {e}"
                summary["errors"].append(error_msg)
        
        return summary
    
    async def _run_raw_harvest(self, date_str: str) -> Dict[str, Any]:
        """Run raw harvest for a date."""
        # Import at runtime to avoid heavy module loading
        from ..tools.raw_harvest_date import load_date as harvest_date
        
        return await harvest_date(
            date_str=date_str,
            root=self.raw_root,
            rate_limit=self.rate_limit,
            retries=3
        )
    
    async def _run_silver_load(self, date_str: str) -> Dict[str, Any]:
        """Run silver load for a date."""
        # Import at runtime to avoid heavy module loading
        from ..tools.silver_load_date import load_date as silver_load_date
        
        return await silver_load_date(
            date_str=date_str,
            raw_root=self.raw_root
        )
    
    def _get_season_date_range(self, season: str) -> tuple[str, str]:
        """Get start and end dates for an NBA season.
        
        Args:
            season: Season string like '2023-24'
            
        Returns:
            Tuple of (start_date, end_date) in YYYY-MM-DD format
        """
        # Parse season (e.g., '2023-24' -> start year 2023, end year 2024)
        start_year, end_year = season.split('-')
        start_year = int(start_year)
        end_year = int('20' + end_year)
        
        # NBA season typically runs October to April
        start_date = f"{start_year}-10-01"
        end_date = f"{end_year}-04-30"
        
        return start_date, end_date
    
    def _generate_date_range(self, start_date: str, end_date: str) -> List[str]:
        """Generate list of dates between start and end dates.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            List of date strings
        """
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        dates = []
        current = start
        
        while current <= end:
            dates.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)
        
        return dates


async def main():
    """CLI entry point for batch backfill."""
    parser = argparse.ArgumentParser(description="NBA Historical Data Backfill Pipeline")
    
    # Date range options (mutually exclusive with season)
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument("--start", help="Start date (YYYY-MM-DD)")
    date_group.add_argument("--season", help="NBA season (e.g., 2023-24)")
    
    parser.add_argument("--end", help="End date (YYYY-MM-DD, required with --start)")
    parser.add_argument("--dry-run", action="store_true", 
                       help="Run extraction/validation without DB inserts")
    parser.add_argument("--rate-limit", type=float, default=5.0,
                       help="Requests per second limit (default: 5.0)")
    parser.add_argument("--batch-size", type=int, default=100,
                       help="Games per batch (default: 100)")
    parser.add_argument("--raw-dir", default="./raw",
                       help="Directory for raw payloads (default: ./raw)")
    parser.add_argument("--ops-dir", default="./ops",
                       help="Directory for ops logs (default: ./ops)")
    parser.add_argument("--retry-quarantined", action="store_true",
                       help="Retry all quarantined games")
    parser.add_argument("--log-level", default="INFO",
                       choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       help="Logging level (default: INFO)")
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.retry_quarantined:
        # Special mode: retry quarantined games
        pass
    elif args.season:
        # Season mode
        pass
    elif args.start and args.end:
        # Date range mode
        pass
    else:
        parser.error("Must specify either --season, --start/--end, or --retry-quarantined")
    
    # Create orchestrator
    orchestrator = BackfillOrchestrator(
        rate_limit=args.rate_limit,
        batch_size=args.batch_size,
        raw_root=args.raw_dir
    )
    
    try:
        if args.retry_quarantined:
            result = await orchestrator.backfill_date_range(args.start, args.end, retry_quarantined=True)
            print(f"✅ Quarantine retry: {result['total_games']} games processed")
        elif args.season:
            result = await orchestrator.backfill_seasons([args.season], dry_run=args.dry_run)
            print(f"✅ Season {args.season}: {result['total_games']} games processed")
        else:
            result = await orchestrator.backfill_date_range(args.start, args.end, dry_run=args.dry_run)
            print(f"✅ Range {args.start} to {args.end}: {result['total_games']} games processed")
        
        # Report errors
        if result.get('errors'):
            print(f"⚠️  Errors encountered: {result['errors']}")
        
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Backfill failed: {e}")
        print(f"❌ Backfill failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())