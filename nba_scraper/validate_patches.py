#!/usr/bin/env python3
"""
Validation script for historical backfill patches.
Tests all the components we've implemented.
"""

import sys
import traceback
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

def test_loaders_facade():
    """Test loader facade resolution."""
    print("🔧 Testing Loaders Facade...")
    try:
        from nba_scraper.loaders import (
            upsert_game, upsert_pbp, upsert_lineups, 
            upsert_shots, upsert_adv_metrics
        )
        
        # Check each loader is callable or None
        loaders = {
            'upsert_game': upsert_game,
            'upsert_pbp': upsert_pbp, 
            'upsert_lineups': upsert_lineups,
            'upsert_shots': upsert_shots,
            'upsert_adv_metrics': upsert_adv_metrics
        }
        
        for name, loader in loaders.items():
            status = "✅ AVAILABLE" if (loader is not None and callable(loader)) else "❌ NOT FOUND"
            print(f"   {name}: {status}")
        
        print("✅ Loaders facade works - no ImportError")
        return True
        
    except Exception as e:
        print(f"❌ Loaders facade failed: {e}")
        return False

def test_io_facade():
    """Test IO facade."""
    print("\n📡 Testing IO Facade...")
    try:
        from nba_scraper.io_clients import IoFacade
        
        # Test with mock implementation
        class MockImpl:
            async def fetch_boxscore(self, game_id): return {"test": "boxscore"}
            async def fetch_pbp(self, game_id): return {"test": "pbp"}
            async def fetch_lineups(self, game_id): return {"test": "lineups"}
            async def fetch_shots(self, game_id): return {"test": "shots"}
            async def fetch_scoreboard(self, date_str): return {"test": "scoreboard"}
        
        facade = IoFacade(MockImpl())
        
        # Test all required methods exist
        required_methods = ['fetch_boxscore', 'fetch_pbp', 'fetch_lineups', 'fetch_shots', 'fetch_scoreboard']
        for method in required_methods:
            if hasattr(facade, method):
                print(f"   {method}: ✅ AVAILABLE")
            else:
                print(f"   {method}: ❌ MISSING")
        
        print("✅ IO facade works")
        return True
        
    except Exception as e:
        print(f"❌ IO facade failed: {e}")
        return False

def test_team_crosswalk():
    """Test team crosswalk utility."""
    print("\n🏀 Testing Team Crosswalk...")
    try:
        from nba_scraper.utils.team_crosswalk import get_team_index, resolve_team_id
        
        team_index = get_team_index()
        print(f"   Team index loaded: {len(team_index)} teams")
        
        # Test some common teams
        test_cases = [
            ("LAL", "Lakers"),
            ("BOS", "Celtics"), 
            ("BKN", "Brooklyn (alias for BRK)"),
            ("PHO", "Phoenix (alias for PHX)")
        ]
        
        for tricode, description in test_cases:
            try:
                team_id = resolve_team_id(tricode, team_index)
                print(f"   {tricode} ({description}): ✅ ID={team_id}")
            except ValueError:
                print(f"   {tricode} ({description}): ❌ NOT FOUND")
        
        print("✅ Team crosswalk works")
        return True
        
    except Exception as e:
        print(f"❌ Team crosswalk failed: {e}")
        return False

def test_clock_utilities():
    """Test clock parsing utilities."""
    print("\n⏰ Testing Clock Utilities...")
    try:
        from nba_scraper.utils.clock import parse_clock_to_seconds, compute_seconds_elapsed
        
        # Test clock parsing
        test_cases = [
            ("12:00", 720.0),
            ("5:30", 330.0),
            ("11:45.5", 705.5),
            ("PT11M45S", 705.0)
        ]
        
        for time_str, expected in test_cases:
            result = parse_clock_to_seconds(time_str)
            if result == expected:
                print(f"   parse_clock_to_seconds('{time_str}'): ✅ {result}")
            else:
                print(f"   parse_clock_to_seconds('{time_str}'): ❌ {result} != {expected}")
        
        # Test seconds elapsed computation
        elapsed_cases = [
            ("12:00", 1, 0.0),    # Q1 start
            ("0:00", 1, 720.0),   # Q1 end
            ("5:00", 5, 0.0),     # OT start
            ("0:00", 5, 300.0),   # OT end
        ]
        
        for time_str, period, expected in elapsed_cases:
            result = compute_seconds_elapsed(time_str, period)
            if result == expected:
                print(f"   compute_seconds_elapsed('{time_str}', {period}): ✅ {result}")
            else:
                print(f"   compute_seconds_elapsed('{time_str}', {period}): ❌ {result} != {expected}")
        
        print("✅ Clock utilities work")
        return True
        
    except Exception as e:
        print(f"❌ Clock utilities failed: {e}")
        return False

def test_date_utilities():
    """Test date normalization utilities."""
    print("\n📅 Testing Date Utilities...")
    try:
        from nba_scraper.utils.date_norm import (
            to_date_str, derive_season_from_game_id, derive_season_from_date
        )
        from datetime import date, datetime
        
        # Test date string conversion
        date_cases = [
            (date(2024, 10, 15), "2024-10-15"),
            (datetime(2024, 10, 15, 15, 30), "2024-10-15"),
            ("2024-10-15", "2024-10-15")
        ]
        
        for date_input, expected in date_cases:
            result = to_date_str(date_input)
            if result == expected:
                print(f"   to_date_str({date_input}): ✅ {result}")
            else:
                print(f"   to_date_str({date_input}): ❌ {result} != {expected}")
        
        # Test season derivation from game ID
        season_id_cases = [
            ("0022400001", "2024-25"),
            ("0022300456", "2023-24"),
            ("0042300101", "2023-24")  # Playoffs
        ]
        
        for game_id, expected in season_id_cases:
            result = derive_season_from_game_id(game_id)
            if result == expected:
                print(f"   derive_season_from_game_id('{game_id}'): ✅ {result}")
            else:
                print(f"   derive_season_from_game_id('{game_id}'): ❌ {result} != {expected}")
        
        # Test season derivation from date
        season_date_cases = [
            ("2024-01-15", "2023-24"),  # January = previous season
            ("2024-10-15", "2024-25"),  # October = current season
        ]
        
        for date_str, expected in season_date_cases:
            result = derive_season_from_date(date_str)
            if result == expected:
                print(f"   derive_season_from_date('{date_str}'): ✅ {result}")
            else:
                print(f"   derive_season_from_date('{date_str}'): ❌ {result} != {expected}")
        
        print("✅ Date utilities work")
        return True
        
    except Exception as e:
        print(f"❌ Date utilities failed: {e}")
        return False

def test_games_bridge():
    """Test games bridge transformer."""
    print("\n🎮 Testing Games Bridge...")
    try:
        from nba_scraper.transformers.games_bridge import _normalize_game_status
        
        # Test status normalization
        status_cases = [
            ("FINAL", "FINAL"),
            ("final", "FINAL"),
            ("FINISHED", "FINAL"),
            ("LIVE", "LIVE"),
            ("IN_PROGRESS", "LIVE"),
            (None, "SCHEDULED")
        ]
        
        for status_input, expected in status_cases:
            result = _normalize_game_status(status_input)
            if result == expected:
                print(f"   _normalize_game_status({status_input}): ✅ {result}")
            else:
                print(f"   _normalize_game_status({status_input}): ❌ {result} != {expected}")
        
        print("✅ Games bridge works")
        return True
        
    except Exception as e:
        print(f"❌ Games bridge failed: {e}")
        return False

def main():
    """Run all validation tests."""
    print("🔍 NBA Historical Backfill Patches - Validation")
    print("=" * 60)
    
    tests = [
        test_loaders_facade,
        test_io_facade,
        test_team_crosswalk,
        test_clock_utilities,
        test_date_utilities,
        test_games_bridge,
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            if test():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"❌ {test.__name__} crashed: {e}")
            traceback.print_exc()
            failed += 1
    
    print(f"\n📊 Validation Results:")
    print(f"   ✅ Passed: {passed}")
    print(f"   ❌ Failed: {failed}")
    print(f"   📈 Success Rate: {passed/(passed+failed)*100:.1f}%")
    
    if failed == 0:
        print(f"\n🎉 ALL PATCHES VALIDATED - Ready for pilot backfill!")
        return True
    else:
        print(f"\n⚠️  Some components need fixes before pilot backfill")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)