#!/usr/bin/env python3
"""CLI debugging tool for team tricode resolution and transformer bridge testing."""

import argparse
import sys
from datetime import datetime, date, timezone
from typing import Optional

from nba_scraper.utils.team_lookup import (
    get_team_index, resolve_tricode_to_id, get_canonical_tricode, validate_tricode_pair
)
from nba_scraper.utils.dates import (
    derive_game_date_with_provenance, derive_season_from_game_id, 
    derive_season_from_date
)
from nba_scraper.transformers.games_bridge import to_db_game
from nba_scraper.models.game_rows import GameRow
from nba_scraper.models.enums import GameStatus


def debug_team_lookup(tricode: str) -> None:
    """Debug team tricode resolution."""
    print(f"\n🔍 Debugging tricode: '{tricode}'")
    print("=" * 50)
    
    try:
        # Get team index
        team_index = get_team_index()
        print(f"📊 Total teams in index: {len(team_index)}")
        
        # Try to resolve
        team_id = resolve_tricode_to_id(tricode)
        canonical = get_canonical_tricode(tricode)
        
        print(f"✅ Resolution successful:")
        print(f"   Canonical tricode: {canonical}")
        print(f"   Team ID: {team_id}")
        
    except ValueError as e:
        print(f"❌ Resolution failed: {str(e)}")
        
        # Show similar tricodes for debugging
        print(f"\n💡 Available tricodes (showing first 10):")
        available = sorted(team_index.keys()) if 'team_index' in locals() else []
        for tc in available[:10]:
            print(f"   {tc}")
        
        if len(available) > 10:
            print(f"   ... and {len(available) - 10} more")


def debug_game_transformation(
    game_id: str,
    home_tricode: str,
    away_tricode: str,
    season: Optional[str] = None,
    game_date: Optional[str] = None,
    status: str = "FINAL"
) -> None:
    """Debug full game transformation pipeline."""
    print(f"\n🎮 Debugging game transformation: {game_id}")
    print("=" * 60)
    
    try:
        # Create test GameRow
        game_row_data = {
            "game_id": game_id,
            "season": season or "",
            "home_team_tricode": home_tricode,
            "away_team_tricode": away_tricode,
            "status": GameStatus(status),
            "source": "debug_cli",
            "source_url": "https://debug.local"
        }
        
        # Add date info
        if game_date:
            game_row_data["game_date_local"] = date.fromisoformat(game_date)
        else:
            game_row_data["game_date_local"] = date.today()
            
        game_row = GameRow(**game_row_data)
        
        print(f"📝 Created GameRow:")
        print(f"   Game ID: {game_row.game_id}")
        print(f"   Season: {game_row.season}")
        print(f"   Home: {game_row.home_team_tricode}")
        print(f"   Away: {game_row.away_team_tricode}")
        print(f"   Date: {game_row.game_date_local}")
        print(f"   Status: {game_row.status}")
        
        # Transform to Game
        db_game = to_db_game(game_row)
        
        print(f"\n✅ Transformation successful:")
        print(f"   Game ID: {db_game.game_id}")
        print(f"   Season: {db_game.season}")
        print(f"   Game Date: {db_game.game_date}")
        print(f"   Home Team ID: {db_game.home_team_id}")
        print(f"   Away Team ID: {db_game.away_team_id}")
        print(f"   Status: {db_game.status}")
        
    except Exception as e:
        print(f"❌ Transformation failed: {str(e)}")
        print(f"   Error type: {type(e).__name__}")


def debug_season_derivation(game_id: str, game_date: Optional[str] = None) -> None:
    """Debug season derivation logic."""
    print(f"\n📅 Debugging season derivation")
    print("=" * 40)
    
    # From game ID
    season_from_id = derive_season_from_game_id(game_id)
    print(f"From game ID '{game_id}': {season_from_id}")
    
    # From date
    if game_date:
        season_from_date = derive_season_from_date(game_date)
        print(f"From date '{game_date}': {season_from_date}")
    else:
        print("No date provided for date-based derivation")


def debug_date_handling(
    local_date: Optional[str] = None,
    utc_datetime: Optional[str] = None,
    timezone_str: Optional[str] = None
) -> None:
    """Debug date handling logic."""
    print(f"\n🕐 Debugging date handling")
    print("=" * 35)
    
    try:
        # Parse UTC datetime if provided
        utc_dt = None
        if utc_datetime:
            utc_dt = datetime.fromisoformat(utc_datetime.replace('Z', '+00:00'))
        
        result_date, provenance = derive_game_date_with_provenance(
            game_date_local=local_date,
            game_date_utc=utc_dt,
            arena_timezone=timezone_str,
            game_id="debug"
        )
        
        print(f"✅ Date derivation successful:")
        print(f"   Final date: {result_date}")
        print(f"   Provenance: {provenance}")
        
    except Exception as e:
        print(f"❌ Date derivation failed: {str(e)}")


def list_all_teams() -> None:
    """List all available teams."""
    print(f"\n📋 All available teams")
    print("=" * 30)
    
    try:
        team_index = get_team_index()
        
        print(f"Total teams: {len(team_index)}")
        print("\nTricode → Team ID:")
        
        for tricode in sorted(team_index.keys()):
            team_id = team_index[tricode]
            print(f"   {tricode:<4} → {team_id}")
            
    except Exception as e:
        print(f"❌ Failed to load teams: {str(e)}")


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Debug team tricode resolution and game transformation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test tricode resolution
  python debug_games_bridge.py --tricode LAL
  
  # Test game transformation
  python debug_games_bridge.py --game-transform \\
    --game-id 0022400001 --home LAL --away GSW
  
  # Debug season derivation
  python debug_games_bridge.py --season-debug \\
    --game-id 0022400001 --date 2024-01-15
  
  # List all teams
  python debug_games_bridge.py --list-teams
        """
    )
    
    # Team lookup debugging
    parser.add_argument("--tricode", help="Test tricode resolution")
    
    # Game transformation debugging
    parser.add_argument("--game-transform", action="store_true", 
                       help="Test full game transformation")
    parser.add_argument("--game-id", help="Game ID for transformation test")
    parser.add_argument("--home", help="Home team tricode")
    parser.add_argument("--away", help="Away team tricode")
    parser.add_argument("--season", help="Season (optional)")
    parser.add_argument("--date", help="Game date YYYY-MM-DD (optional)")
    parser.add_argument("--status", default="FINAL", help="Game status")
    
    # Season derivation debugging
    parser.add_argument("--season-debug", action="store_true",
                       help="Test season derivation")
    
    # Date handling debugging
    parser.add_argument("--date-debug", action="store_true",
                       help="Test date handling")
    parser.add_argument("--local-date", help="Local date YYYY-MM-DD")
    parser.add_argument("--utc-datetime", help="UTC datetime ISO format")
    parser.add_argument("--timezone", help="Arena timezone")
    
    # Utility commands
    parser.add_argument("--list-teams", action="store_true",
                       help="List all available teams")
    
    args = parser.parse_args()
    
    # Execute requested debugging
    if args.tricode:
        debug_team_lookup(args.tricode)
    
    elif args.game_transform:
        if not all([args.game_id, args.home, args.away]):
            print("❌ Game transformation requires --game-id, --home, and --away")
            sys.exit(1)
        debug_game_transformation(
            args.game_id, args.home, args.away,
            args.season, args.date, args.status
        )
    
    elif args.season_debug:
        if not args.game_id:
            print("❌ Season debugging requires --game-id")
            sys.exit(1)
        debug_season_derivation(args.game_id, args.date)
    
    elif args.date_debug:
        debug_date_handling(args.local_date, args.utc_datetime, args.timezone)
    
    elif args.list_teams:
        list_all_teams()
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()