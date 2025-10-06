#!/usr/bin/env python3
"""Command-line tool for harvesting NBA raw data for an entire season.

Usage:
    python -m nba_scraper.tools.raw_harvest_season --season 2024-25
    python -m nba_scraper.tools.raw_harvest_season --season 2023-24 --root ./raw --rate-limit 3
"""

import asyncio
import sys
import argparse
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import List, Tuple

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from nba_scraper.raw_io.backfill import harvest_date
from nba_scraper.raw_io.report import summarize_date, summarize_season, format_summary_for_display
from nba_scraper.raw_io.persist import ensure_dir
from nba_scraper.nba_logging import get_logger

logger = get_logger(__name__)


def parse_season_dates(season: str) -> List[str]:
    """Parse season string and return list of dates covering the regular season.
    
    Args:
        season: Season string like "2024-25", "2023-24", etc.
        
    Returns:
        List of date strings in YYYY-MM-DD format covering regular season
        
    Raises:
        ValueError: If season format is invalid
    """
    try:
        # Parse season format "YYYY-YY"
        if '-' not in season or len(season) != 7:
            raise ValueError(f"Invalid season format: {season}. Expected format: YYYY-YY")
        
        start_year_str, end_year_suffix = season.split('-')
        start_year = int(start_year_str)
        end_year = int(f"20{end_year_suffix}")
        
        # Validate year sequence
        if end_year != start_year + 1:
            raise ValueError(f"Invalid season years: {start_year} -> {end_year}")
        
        # NBA regular season typically runs October through April
        # Start from October 1st of start year through April 30th of end year
        start_date = date(start_year, 10, 1)
        end_date = date(end_year, 4, 30)
        
        # Generate all dates in range
        dates = []
        current_date = start_date
        
        while current_date <= end_date:
            dates.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)
        
        logger.info("Generated season date range", 
                   season=season,
                   start_date=start_date.isoformat(),
                   end_date=end_date.isoformat(),
                   total_dates=len(dates))
        
        return dates
        
    except ValueError as e:
        raise ValueError(f"Failed to parse season {season}: {e}")


def validate_season(season: str) -> str:
    """Validate season string format.
    
    Args:
        season: Season string to validate
        
    Returns:
        Validated season string
        
    Raises:
        ValueError: If season format is invalid or out of supported range
    """
    try:
        # Parse and validate format
        parse_season_dates(season)
        
        # Check if season is in supported range (2021-22 through 2024-25)
        start_year = int(season.split('-')[0])
        if start_year < 2021 or start_year > 2024:
            raise ValueError(f"Season {season} is outside supported range (2021-22 to 2024-25)")
        
        return season
        
    except ValueError as e:
        raise ValueError(f"Invalid season: {e}")


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Harvest NBA raw data for an entire season",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m nba_scraper.tools.raw_harvest_season --season 2024-25
  python -m nba_scraper.tools.raw_harvest_season --season 2023-24 --root ./data/raw --rate-limit 3
  python -m nba_scraper.tools.raw_harvest_season --season 2022-23 --retries 3 --skip-existing
        """
    )
    
    parser.add_argument(
        '--season',
        required=True,
        type=str,
        choices=['2024-25', '2023-24', '2022-23', '2021-22'],
        help='Season to harvest (2021-22, 2022-23, 2023-24, or 2024-25)'
    )
    
    parser.add_argument(
        '--root',
        default='./raw',
        type=str,
        help='Root directory for raw data storage (default: ./raw)'
    )
    
    parser.add_argument(
        '--rate-limit',
        default=5,
        type=int,
        help='Requests per second limit (default: 5)'
    )
    
    parser.add_argument(
        '--retries',
        default=5,
        type=int,
        help='Maximum retry attempts per endpoint (default: 5)'
    )
    
    parser.add_argument(
        '--skip-existing',
        action='store_true',
        help='Skip dates that already have manifest.json files'
    )
    
    parser.add_argument(
        '--max-dates',
        type=int,
        help='Maximum number of dates to process (for testing)'
    )
    
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Suppress detailed output, show only summaries'
    )
    
    parser.add_argument(
        '--reverse',
        action='store_true',
        help='Process dates in reverse chronological order (newest first)'
    )
    
    return parser.parse_args()


async def main():
    """Main entry point for season harvest CLI."""
    season_start_time = datetime.now()
    
    try:
        args = parse_args()
        
        # Validate season
        try:
            season = validate_season(args.season)
        except ValueError as e:
            print(f"❌ Error: {e}", file=sys.stderr)
            sys.exit(1)
        
        # Ensure root and ops directories exist
        root_path = Path(args.root)
        ops_path = Path('./ops')
        
        try:
            ensure_dir(root_path)
            ensure_dir(ops_path)
        except Exception as e:
            print(f"❌ Failed to create directories: {e}", file=sys.stderr)
            sys.exit(1)
        
        # Setup season logging
        season_log_path = ops_path / f"raw_season_{season.replace('-', '_')}.log"
        
        if not args.quiet:
            print(f"🏀 NBA Raw Season Harvest - {season}")
            print("=" * 60)
            print(f"📂 Root directory: {root_path.absolute()}")
            print(f"⚡ Rate limit: {args.rate_limit} req/sec")
            print(f"🔄 Max retries: {args.retries}")
            print(f"📋 Season log: {season_log_path}")
            print(f"🎯 Season: {season}")
            if args.skip_existing:
                print("⏭️  Skipping existing dates")
            if args.reverse:
                print("🔄 Processing in reverse chronological order")
            print()
        
        # Generate date list for season
        try:
            all_dates = parse_season_dates(season)
            
            if args.reverse:
                all_dates.reverse()
                
            if args.max_dates:
                all_dates = all_dates[:args.max_dates]
                
        except ValueError as e:
            print(f"❌ Failed to generate season dates: {e}", file=sys.stderr)
            sys.exit(1)
        
        # Track season-wide statistics
        season_stats = {
            'season': season,
            'dates_attempted': 0,
            'dates_completed': 0,
            'dates_skipped': 0,
            'total_games': 0,
            'total_endpoints': 0,
            'total_failures': 0,
            'total_bytes': 0,
            'start_time': season_start_time.isoformat(),
            'errors': []
        }
        
        # Process each date in the season
        for i, date_str in enumerate(all_dates, 1):
            date_dir = root_path / date_str
            manifest_path = date_dir / "manifest.json"
            
            # Skip if manifest exists and skip-existing is enabled
            if args.skip_existing and manifest_path.exists():
                if not args.quiet:
                    print(f"⏭️  [{i:3d}/{len(all_dates)}] {date_str} - SKIPPED (manifest exists)")
                season_stats['dates_skipped'] += 1
                continue
            
            season_stats['dates_attempted'] += 1
            
            try:
                if not args.quiet:
                    print(f"🚀 [{i:3d}/{len(all_dates)}] Harvesting {date_str}...")
                
                # Harvest the date
                date_summary = await harvest_date(
                    date_str=date_str,
                    root=args.root,
                    rate_limit=args.rate_limit,
                    max_retries=args.retries
                )
                
                # Update season statistics
                season_stats['dates_completed'] += 1
                season_stats['total_games'] += date_summary.get('games_processed', 0)
                season_stats['total_endpoints'] += date_summary.get('endpoints_succeeded', 0)
                season_stats['total_failures'] += date_summary.get('endpoints_failed', 0)
                season_stats['total_bytes'] += date_summary.get('total_bytes', 0)
                
                # Log to season file
                with open(season_log_path, 'a', encoding='utf-8') as log_file:
                    log_entry = (
                        f"{datetime.now().isoformat()} {date_str} "
                        f"games={date_summary.get('games_processed', 0)} "
                        f"ok={date_summary.get('endpoints_succeeded', 0)} "
                        f"fail={date_summary.get('endpoints_failed', 0)} "
                        f"bytes={date_summary.get('total_bytes', 0)}\n"
                    )
                    log_file.write(log_entry)
                
                # Show progress
                if not args.quiet:
                    games = date_summary.get('games_processed', 0)
                    endpoints_ok = date_summary.get('endpoints_succeeded', 0)
                    endpoints_fail = date_summary.get('endpoints_failed', 0)
                    bytes_mb = date_summary.get('total_bytes', 0) / (1024 * 1024)
                    
                    status = "✅" if endpoints_fail == 0 else "⚠️"
                    print(f"   {status} {games} games, {endpoints_ok} OK, {endpoints_fail} failed, {bytes_mb:.1f}MB")
                else:
                    # Quiet mode - just a progress dot
                    print(".", end="", flush=True)
                
                # Small delay between dates to be respectful
                await asyncio.sleep(0.5)
                
            except Exception as e:
                error_msg = f"Failed to harvest {date_str}: {str(e)}"
                logger.error(error_msg)
                season_stats['errors'].append(error_msg)
                
                if not args.quiet:
                    print(f"   ❌ Error: {str(e)}")
                
                # Log error to season file
                with open(season_log_path, 'a', encoding='utf-8') as log_file:
                    log_file.write(f"{datetime.now().isoformat()} {date_str} ERROR: {str(e)}\n")
        
        if args.quiet:
            print()  # New line after progress dots
        
        # Generate final season summary
        season_end_time = datetime.now()
        season_duration = season_end_time - season_start_time
        
        season_stats['end_time'] = season_end_time.isoformat()
        season_stats['duration_seconds'] = season_duration.total_seconds()
        
        # Generate comprehensive season summary from manifests
        try:
            detailed_summary = summarize_season(root_path, season)
            formatted_summary = format_summary_for_display(detailed_summary)
            
            print("\n" + "=" * 60)
            print(formatted_summary)
            print("=" * 60)
            
        except Exception as e:
            logger.warning("Failed to generate detailed season summary", error=str(e))
            # Fallback to basic summary
            print(f"\n📊 Season {season} Summary:")
            print(f"   Dates attempted: {season_stats['dates_attempted']}")
            print(f"   Dates completed: {season_stats['dates_completed']}")
            print(f"   Dates skipped: {season_stats['dates_skipped']}")
            print(f"   Total games: {season_stats['total_games']}")
            print(f"   Total endpoints OK: {season_stats['total_endpoints']}")
            print(f"   Total failures: {season_stats['total_failures']}")
            print(f"   Total data: {season_stats['total_bytes'] / (1024**3):.2f} GB")
            print(f"   Duration: {season_duration}")
        
        # Show log location
        print(f"\n📋 Season log: {season_log_path}")
        
        if season_stats['errors']:
            print(f"⚠️  {len(season_stats['errors'])} date(s) had errors")
        
        # Determine exit code
        if season_stats['total_failures'] > 0 or season_stats['errors']:
            print(f"\n⚠️  Some endpoints failed - check logs and quarantine file")
            sys.exit(1)  # Non-zero exit for CI alerting
        else:
            print(f"\n✅ Season harvest completed successfully!")
            sys.exit(0)
            
    except KeyboardInterrupt:
        print(f"\n⏹️  Interrupted by user", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"❌ Unexpected error: {e}", file=sys.stderr)
        logger.error("Unexpected error in season harvest CLI", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())