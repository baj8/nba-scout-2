#!/usr/bin/env python3
"""
Comprehensive Analytics Pipeline Demo
=====================================

This script demonstrates the advanced analytics capabilities of our NBA scraper,
including team and player analytics with sophisticated metrics.
"""

import asyncio
import sys
from datetime import date, datetime
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.nba_scraper.pipelines.analytics_pipeline import AnalyticsPipeline
from src.nba_scraper.db import get_connection
from src.nba_scraper.nba_logging import get_logger

logger = get_logger(__name__)


async def demo_team_analytics():
    """Demonstrate team analytics capabilities."""
    print("\n🏀 NBA TEAM ANALYTICS DEMO")
    print("=" * 50)
    
    pipeline = AnalyticsPipeline()
    
    # Run team analytics for current season
    print("\n📊 Computing team analytics for 2024-25 season...")
    result = await pipeline.compute_team_analytics(season="2024-25")
    
    if result.success:
        print(f"✅ Success! Analyzed {result.games_analyzed} games")
        print(f"📈 Computed metrics: {', '.join(result.metrics_computed)}")
        print(f"⏱️  Duration: {result.duration_seconds:.2f} seconds")
    else:
        print(f"❌ Failed: {result.error}")
    
    # Show sample team stats
    await show_sample_team_stats()
    
    return result


async def demo_player_analytics():
    """Demonstrate player analytics capabilities."""
    print("\n🏀 NBA PLAYER ANALYTICS DEMO")
    print("=" * 50)
    
    pipeline = AnalyticsPipeline()
    
    # Run player analytics for current season
    print("\n📊 Computing player analytics for 2024-25 season...")
    result = await pipeline.compute_player_analytics(season="2024-25")
    
    if result.success:
        print(f"✅ Success! Analyzed {result.games_analyzed} games")
        print(f"📈 Computed metrics: {', '.join(result.metrics_computed)}")
        print(f"⏱️  Duration: {result.duration_seconds:.2f} seconds")
    else:
        print(f"❌ Failed: {result.error}")
    
    # Show sample player stats
    await show_sample_player_stats()
    
    return result


async def show_sample_team_stats():
    """Display sample team analytics results."""
    print("\n📈 SAMPLE TEAM ANALYTICS RESULTS")
    print("-" * 40)
    
    conn = await get_connection()
    
    # Get top performing teams by net rating
    query = """
    SELECT 
        t.full_name as team_name,
        COUNT(*) as games_played,
        AVG(tgs.points) as avg_points,
        AVG(tgs.offensive_rating) as avg_off_rating,
        AVG(tgs.defensive_rating) as avg_def_rating,
        AVG(tgs.net_rating) as avg_net_rating,
        AVG(tgs.pace) as avg_pace,
        AVG(tgs.effective_fg_pct) as avg_efg_pct,
        AVG(tgs.true_shooting_pct) as avg_ts_pct,
        AVG(tgs.assist_turnover_ratio) as avg_ast_to_ratio
    FROM team_game_stats tgs
    JOIN teams t ON tgs.team_id = t.team_id
    JOIN games g ON tgs.game_id = g.game_id
    WHERE g.season = '2024-25' 
    AND tgs.net_rating IS NOT NULL
    GROUP BY t.team_id, t.full_name
    HAVING COUNT(*) >= 3  -- At least 3 games
    ORDER BY AVG(tgs.net_rating) DESC
    LIMIT 10
    """
    
    try:
        rows = await conn.fetch(query)
        
        if rows:
            print("\n🏆 TOP 10 TEAMS BY NET RATING:")
            print(f"{'Team':<25} {'GP':>3} {'PPG':>6} {'ORtg':>6} {'DRtg':>6} {'NetRtg':>7} {'Pace':>6} {'eFG%':>6} {'TS%':>6} {'AST/TO':>7}")
            print("-" * 95)
            
            for row in rows:
                print(f"{row['team_name']:<25} "
                      f"{row['games_played']:>3} "
                      f"{row['avg_points']:>6.1f} "
                      f"{row['avg_off_rating']:>6.1f} "
                      f"{row['avg_def_rating']:>6.1f} "
                      f"{row['avg_net_rating']:>7.1f} "
                      f"{row['avg_pace']:>6.1f} "
                      f"{row['avg_efg_pct']:>6.3f} "
                      f"{row['avg_ts_pct']:>6.3f} "
                      f"{row['avg_ast_to_ratio']:>7.2f}")
        else:
            print("📊 No team analytics data found. Run data collection first!")
            
    except Exception as e:
        print(f"❌ Error fetching team stats: {e}")


async def show_sample_player_stats():
    """Display sample player analytics results."""
    print("\n📈 SAMPLE PLAYER ANALYTICS RESULTS")
    print("-" * 40)
    
    conn = await get_connection()
    
    # Check if we have player analytics tables
    check_query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
    AND table_name LIKE '%player%'
    """
    
    try:
        tables = await conn.fetch(check_query)
        table_names = [row['table_name'] for row in tables]
        
        print(f"📋 Available player analytics tables: {', '.join(table_names) if table_names else 'None found'}")
        
        if 'player_game_stats' in table_names:
            # Show top scorers
            scorer_query = """
            SELECT 
                p.full_name,
                COUNT(*) as games_played,
                AVG(pgs.points) as avg_points,
                AVG(pgs.usage_rate) as avg_usage_rate,
                AVG(pgs.efficiency_rating) as avg_efficiency
            FROM player_game_stats pgs
            JOIN players p ON pgs.player_id = p.player_id
            JOIN games g ON pgs.game_id = g.game_id
            WHERE g.season = '2024-25'
            GROUP BY p.player_id, p.full_name
            HAVING COUNT(*) >= 3
            ORDER BY AVG(pgs.points) DESC
            LIMIT 10
            """
            
            rows = await conn.fetch(scorer_query)
            
            if rows:
                print("\n🏀 TOP 10 SCORERS:")
                print(f"{'Player':<25} {'GP':>3} {'PPG':>6} {'Usage%':>7} {'Efficiency':>10}")
                print("-" * 60)
                
                for row in rows:
                    print(f"{row['full_name']:<25} "
                          f"{row['games_played']:>3} "
                          f"{row['avg_points']:>6.1f} "
                          f"{row['avg_usage_rate']:>7.1f} "
                          f"{row['avg_efficiency']:>10.2f}")
        else:
            print("📊 Player analytics tables not found. Player stats need to be implemented!")
            
    except Exception as e:
        print(f"❌ Error fetching player stats: {e}")


async def show_analytics_capabilities():
    """Display the full range of analytics capabilities."""
    print("\n🔍 ANALYTICS PIPELINE CAPABILITIES")
    print("=" * 50)
    
    capabilities = {
        "🏀 Team Analytics": [
            "Basic stats per game (points, FG%, rebounds, assists, turnovers)",
            "Advanced metrics (eFG%, true shooting %, assist/turnover ratio)", 
            "Pace and efficiency ratings (offensive rating, defensive rating, net rating)",
            "League-relative performance (z-scores for pace and efficiency)",
            "Possession-based calculations",
            "Opponent-adjusted defensive ratings"
        ],
        
        "👤 Player Analytics": [
            "Usage rates and involvement metrics",
            "Efficiency ratings and per-possession stats",
            "Plus/minus calculations and impact metrics",
            "Advanced shooting metrics",
            "Contextual performance analysis",
            "Team contribution analytics"
        ],
        
        "📊 Advanced Features": [
            "Batch processing with configurable sizes",
            "Date range and team/player filtering",
            "Comprehensive error handling and logging",
            "Performance optimization with async processing",
            "Real-time computation status tracking",
            "Flexible analytics pipeline architecture"
        ]
    }
    
    for category, features in capabilities.items():
        print(f"\n{category}:")
        for feature in features:
            print(f"  ✓ {feature}")


async def check_data_availability():
    """Check what data is available for analytics."""
    print("\n📋 DATA AVAILABILITY CHECK")
    print("=" * 50)
    
    conn = await get_connection()
    
    queries = {
        "Games": "SELECT COUNT(*) as count, MIN(game_date) as earliest, MAX(game_date) as latest FROM games WHERE status = 'FINAL'",
        "Teams": "SELECT COUNT(*) as count FROM teams",
        "Play-by-Play Events": "SELECT COUNT(*) as count FROM pbp_events",
        "Team Game Stats": "SELECT COUNT(*) as count FROM team_game_stats",
    }
    
    for category, query in queries.items():
        try:
            result = await conn.fetchrow(query)
            if category == "Games" and result['count'] > 0:
                print(f"📊 {category}: {result['count']:,} records (from {result['earliest']} to {result['latest']})")
            else:
                print(f"📊 {category}: {result['count']:,} records")
        except Exception as e:
            print(f"❌ {category}: Error - {e}")


async def main():
    """Run the comprehensive analytics demo."""
    print("🏀 NBA SCOUT 2 - ADVANCED ANALYTICS DEMO")
    print("=" * 60)
    print("Welcome to the NBA Scout 2 Advanced Analytics Pipeline!")
    print("This demo showcases our sophisticated basketball analytics capabilities.")
    
    try:
        # Check data availability
        await check_data_availability()
        
        # Show capabilities
        await show_analytics_capabilities()
        
        # Run team analytics demo
        team_result = await demo_team_analytics()
        
        # Run player analytics demo  
        player_result = await demo_player_analytics()
        
        # Summary
        print(f"\n🎯 DEMO SUMMARY")
        print("=" * 30)
        print(f"Team Analytics: {'✅ Success' if team_result.success else '❌ Failed'}")
        print(f"Player Analytics: {'✅ Success' if player_result.success else '❌ Failed'}")
        
        if team_result.success or player_result.success:
            print("\n🚀 The NBA Scout 2 analytics pipeline is ready for advanced basketball analysis!")
        else:
            print("\n📊 Analytics pipeline is configured but needs game data to run.")
            print("💡 Tip: Run the data collection pipeline first to populate games and play-by-play data.")
        
    except Exception as e:
        logger.error("Demo failed", error=str(e))
        print(f"\n❌ Demo failed: {e}")
        return False
    
    return True


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)