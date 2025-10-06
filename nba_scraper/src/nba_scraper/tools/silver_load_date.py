#!/usr/bin/env python3
"""Silver load CLI - reads raw NBA data files and loads to database via facade."""

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional

# Import Silver layer components at runtime only
def load_silver_components():
    """Import Silver components only when needed."""
    from nba_scraper.silver.raw_reader import RawReader
    from nba_scraper.silver.transform_games import transform_game
    from nba_scraper.silver.transform_pbp import transform_pbp
    from nba_scraper.silver.transform_shots import transform_shots
    from nba_scraper.silver.transform_officials import transform_officials
    from nba_scraper.silver.transform_starters import transform_starters
    from nba_scraper.loaders.facade import (
        upsert_game, upsert_pbp, upsert_shots, upsert_officials, upsert_starting_lineups
    )
    from nba_scraper.db import get_connection
    
    return {
        'RawReader': RawReader,
        'transform_game': transform_game,
        'transform_pbp': transform_pbp,
        'transform_shots': transform_shots,
        'transform_officials': transform_officials,
        'transform_starters': transform_starters,
        'upsert_game': upsert_game,
        'upsert_pbp': upsert_pbp,
        'upsert_shots': upsert_shots,
        'upsert_officials': upsert_officials,
        'upsert_starting_lineups': upsert_starting_lineups,
        'get_connection': get_connection
    }


async def load_date(date_str: str, raw_root: str) -> Dict[str, Any]:
    """Load processed NBA data from raw files to database.
    
    Args:
        date_str: Date in YYYY-MM-DD format
        raw_root: Root directory containing raw data
        
    Returns:
        Summary dict with counts and errors
    """
    # Load components at runtime
    components = load_silver_components()
    RawReader = components['RawReader']
    get_connection = components['get_connection']
    
    # Initialize raw reader
    reader = RawReader(raw_root)
    
    # Summary tracking
    summary = {
        "date": date_str,
        "games": 0,
        "inserted": {
            "games": 0,
            "pbp": 0,
            "shots": 0,
            "officials": 0,
            "starters": 0
        },
        "errors": []
    }
    
    print(f"📅 Loading Silver data for {date_str} from {raw_root}")
    
    # Get game directories for the date
    game_dirs = list(reader.iter_game_directories(date_str))
    
    if not game_dirs:
        error_msg = f"No game directories found for date {date_str} in {raw_root}"
        print(f"❌ {error_msg}")
        summary["errors"].append(error_msg)
        return summary
    
    print(f"🏀 Found {len(game_dirs)} games to process")
    summary["games"] = len(game_dirs)
    
    try:
        # Get database connection
        conn = await get_connection()
        
        # Process each game
        for i, game_dir in enumerate(game_dirs, 1):
            game_id = reader.get_game_id(game_dir)
            print(f"📊 Processing game {i}/{len(game_dirs)}: {game_id}")
            
            try:
                await process_game(conn, reader, game_dir, game_id, components, summary)
            except Exception as e:
                error_msg = f"Failed to process game {game_id}: {e}"
                print(f"❌ {error_msg}")
                summary["errors"].append(error_msg)
        
        print(f"✅ Silver load complete")
        
    except Exception as e:
        error_msg = f"Database connection failed: {e}"
        print(f"❌ {error_msg}")
        summary["errors"].append(error_msg)
        raise
    
    finally:
        if 'conn' in locals():
            await conn.close()
    
    return summary


async def process_game(conn, reader: 'RawReader', game_dir: Path, game_id: str, 
                      components: Dict[str, Any], summary: Dict[str, Any]) -> None:
    """Process a single game's data through the Silver transformers.
    
    Args:
        conn: Database connection
        reader: RawReader instance
        game_dir: Path to game directory
        game_id: Game ID
        components: Dictionary of loaded Silver components
        summary: Summary dict to update
    """
    # Extract components
    transform_game = components['transform_game']
    transform_pbp = components['transform_pbp']
    transform_shots = components['transform_shots']
    transform_officials = components['transform_officials']
    transform_starters = components['transform_starters']
    upsert_game = components['upsert_game']
    upsert_pbp = components['upsert_pbp']
    upsert_shots = components['upsert_shots']
    upsert_officials = components['upsert_officials']
    upsert_starting_lineups = components['upsert_starting_lineups']
    
    # 1. Transform and load game data
    boxscore_summary = reader.get_boxscore_summary(game_dir)
    if boxscore_summary:
        try:
            game_data = transform_game(boxscore_summary)
            if game_data:
                await upsert_game(conn, game_data)
                summary["inserted"]["games"] += 1
        except Exception as e:
            summary["errors"].append(f"Game transform/load failed for {game_id}: {e}")
    
    # 2. Transform and load play-by-play data
    pbp_data = reader.get_playbyplay(game_dir)
    if pbp_data:
        try:
            pbp_events = transform_pbp(pbp_data, game_id=game_id)
            if pbp_events:
                count = await upsert_pbp(conn, pbp_events)
                summary["inserted"]["pbp"] += count
        except Exception as e:
            summary["errors"].append(f"PBP transform/load failed for {game_id}: {e}")
    
    # 3. Transform and load shot data (optional, may not exist)
    shot_data = reader.get_shotchart(game_dir)
    if shot_data:
        try:
            shots = transform_shots(shot_data, game_id=game_id)
            if shots:
                count = await upsert_shots(conn, shots)
                summary["inserted"]["shots"] += count
        except Exception as e:
            summary["errors"].append(f"Shots transform/load failed for {game_id}: {e}")
    
    # 4. Transform and load officials data
    if boxscore_summary:
        try:
            officials = transform_officials(boxscore_summary, game_id=game_id)
            if officials:
                count = await upsert_officials(conn, officials)
                summary["inserted"]["officials"] += count
        except Exception as e:
            summary["errors"].append(f"Officials transform/load failed for {game_id}: {e}")
    
    # 5. Transform and load starting lineups data
    if boxscore_summary:
        try:
            starters = transform_starters(boxscore_summary, game_id=game_id)
            if starters:
                count = await upsert_starting_lineups(conn, starters)
                summary["inserted"]["starters"] += count
        except Exception as e:
            summary["errors"].append(f"Starters transform/load failed for {game_id}: {e}")


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Load NBA raw data to database via Silver transformers")
    parser.add_argument("--date", required=True, help="Date in YYYY-MM-DD format")
    parser.add_argument("--raw-root", default="raw", help="Root directory containing raw data")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    try:
        summary = asyncio.run(load_date(args.date, args.raw_root))
        
        print(f"\n📊 Summary:")
        print(json.dumps(summary, indent=2))
        
        if summary["errors"]:
            print(f"\n⚠️  {len(summary['errors'])} errors occurred:")
            for error in summary["errors"][:5]:  # Show first 5 errors
                print(f"   - {error}")
            if len(summary["errors"]) > 5:
                print(f"   ... and {len(summary['errors']) - 5} more errors")
        
        # Exit with error code if there were failures
        sys.exit(1 if summary["errors"] else 0)
        
    except KeyboardInterrupt:
        print("\n⚠️  Silver load interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Silver load failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()