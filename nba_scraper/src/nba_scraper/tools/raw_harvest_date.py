#!/usr/bin/env python3
"""Raw harvest CLI - fetches NBA data to local files without database dependencies."""

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Dict, Any

# Only import from raw_io to avoid heavy dependencies
from nba_scraper.raw_io.client import RawNbaClient


async def harvest_date(date: str, root: str = "raw", rate_limit: float = 5.0, retries: int = 5) -> Dict[str, Any]:
    """Harvest all NBA data for a specific date to local files.
    
    Args:
        date: Date in YYYY-MM-DD format
        root: Root directory for raw data storage
        rate_limit: Requests per second limit
        retries: Maximum retry attempts
        
    Returns:
        Summary of harvested data
    """
    root_path = Path(root)
    date_dir = root_path / date
    date_dir.mkdir(parents=True, exist_ok=True)
    
    # Initialize client
    client = RawNbaClient(rate_limit=int(rate_limit), max_retries=retries)
    
    summary = {
        "date": date,
        "root": str(root_path.absolute()),
        "files_created": [],
        "games_found": 0,
        "errors": []
    }
    
    try:
        # 1. Fetch scoreboard for the date
        print(f"📅 Fetching scoreboard for {date}...")
        scoreboard_data = await client.fetch_scoreboard(date)
        
        # Save scoreboard
        scoreboard_file = date_dir / "scoreboard.json"
        with open(scoreboard_file, 'w') as f:
            json.dump(scoreboard_data, f, indent=2, default=str)
        summary["files_created"].append(str(scoreboard_file))
        
        # Extract game IDs from scoreboard
        game_ids = []
        if 'resultSets' in scoreboard_data:
            for result_set in scoreboard_data['resultSets']:
                if result_set.get('name') == 'GameHeader':
                    headers = result_set.get('headers', [])
                    if 'GAME_ID' in headers:
                        game_id_idx = headers.index('GAME_ID')
                        for row in result_set.get('rowSet', []):
                            if len(row) > game_id_idx:
                                game_ids.append(str(row[game_id_idx]))
        
        summary["games_found"] = len(game_ids)
        print(f"🏀 Found {len(game_ids)} games for {date}")
        
        # 2. Fetch detailed data for each game
        for i, game_id in enumerate(game_ids, 1):
            print(f"📊 Fetching game {i}/{len(game_ids)}: {game_id}")
            game_dir = date_dir / game_id
            game_dir.mkdir(exist_ok=True)
            
            # Fetch all game endpoints
            endpoints = [
                ("boxscoresummary", client.fetch_boxscoresummary),
                ("boxscoretraditional", client.fetch_boxscoretraditional),
                ("playbyplay", client.fetch_playbyplay),
            ]
            
            for endpoint_name, fetch_func in endpoints:
                try:
                    data = await fetch_func(game_id)
                    file_path = game_dir / f"{endpoint_name}.json"
                    with open(file_path, 'w') as f:
                        json.dump(data, f, indent=2, default=str)
                    summary["files_created"].append(str(file_path))
                except Exception as e:
                    error_msg = f"Failed to fetch {endpoint_name} for {game_id}: {e}"
                    print(f"⚠️  {error_msg}")
                    summary["errors"].append(error_msg)
            
            # Try to fetch shot chart (may fail for some games)
            try:
                shot_data = await client.fetch_shotchart(game_id)
                file_path = game_dir / "shotchart.json"
                with open(file_path, 'w') as f:
                    json.dump(shot_data, f, indent=2, default=str)
                summary["files_created"].append(str(file_path))
            except Exception as e:
                error_msg = f"Failed to fetch shot chart for {game_id}: {e}"
                print(f"⚠️  {error_msg}")
                summary["errors"].append(error_msg)
        
        # 3. Create manifest file
        manifest_file = date_dir / "manifest.json"
        with open(manifest_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        summary["files_created"].append(str(manifest_file))
        
        print(f"✅ Harvest complete: {len(summary['files_created'])} files created")
        
    except Exception as e:
        error_msg = f"Harvest failed: {e}"
        print(f"❌ {error_msg}")
        summary["errors"].append(error_msg)
        raise
    
    finally:
        await client.close()
    
    return summary


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Harvest NBA data for a specific date")
    parser.add_argument("--date", required=True, help="Date in YYYY-MM-DD format")
    parser.add_argument("--root", default="raw", help="Root directory for raw data storage")
    parser.add_argument("--rate-limit", type=float, default=5.0, help="Requests per second limit")
    parser.add_argument("--retries", type=int, default=5, help="Maximum retry attempts")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    try:
        summary = asyncio.run(harvest_date(
            args.date, 
            root=args.root, 
            rate_limit=args.rate_limit,
            retries=args.retries
        ))
        
        date_dir = Path(args.root) / args.date
        print(f"\n📁 Raw data saved to: {date_dir.absolute()}")
        print(f"📝 Manifest written: {date_dir / 'manifest.json'}")
        
        if summary["errors"]:
            print(f"\n⚠️  {len(summary['errors'])} errors occurred:")
            for error in summary["errors"][:5]:  # Show first 5 errors
                print(f"   - {error}")
            if len(summary["errors"]) > 5:
                print(f"   ... and {len(summary['errors']) - 5} more errors")
        
        print(f"\n📊 Summary:")
        print(json.dumps({
            "date": summary["date"],
            "games_found": summary["games_found"],
            "files_created": len(summary["files_created"]),
            "errors": len(summary["errors"])
        }, indent=2))
        
        # Exit with error code if there were failures
        sys.exit(1 if summary["errors"] else 0)
        
    except KeyboardInterrupt:
        print("\n⚠️  Harvest interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Harvest failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()