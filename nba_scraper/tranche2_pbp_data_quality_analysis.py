#!/usr/bin/env python3
"""
Tranche 2: Play-by-Play Data Quality Analysis
NBA Scout 2 - Enhanced Analytics Implementation

This script analyzes existing PBP data quality and identifies opportunities
for enhanced analytics including shot chart analysis, lineup tracking, and
advanced play-by-play metrics.
"""

import sys
import asyncio
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
import json

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

@dataclass
class PbpQualityMetrics:
    """Data quality metrics for PBP analysis."""
    total_games: int = 0
    total_events: int = 0
    games_with_pbp: int = 0
    games_missing_pbp: int = 0
    events_with_coordinates: int = 0
    events_missing_coordinates: int = 0
    shot_events: int = 0
    shots_with_distance: int = 0
    shots_missing_distance: int = 0
    unique_event_types: List[str] = None
    period_coverage: Dict[int, int] = None
    coordinate_quality_score: float = 0.0
    
    def __post_init__(self):
        if self.unique_event_types is None:
            self.unique_event_types = []
        if self.period_coverage is None:
            self.period_coverage = {}

@dataclass
class GameQualityReport:
    """Quality report for individual games."""
    game_id: str
    home_team: str
    away_team: str
    game_date: date
    total_events: int
    shot_events: int
    coordinate_coverage: float
    missing_data_flags: List[str]
    quality_score: float

async def analyze_pbp_data_quality():
    """Comprehensive analysis of PBP data quality for Tranche 2 planning."""
    print("🎯 TRANCHE 2: Play-by-Play Data Quality Analysis")
    print("=" * 60)
    print(f"📅 Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🔍 Objective: Assess PBP data for enhanced analytics opportunities")
    print()
    
    try:
        from nba_scraper.db import get_connection
        conn = await get_connection()
        
        # 1. Overall PBP Coverage Analysis
        print("1️⃣ Overall PBP Coverage Analysis")
        print("-" * 40)
        
        metrics = await analyze_overall_coverage(conn)
        print_coverage_metrics(metrics)
        
        # 2. Shot Chart Data Quality Assessment
        print("\n2️⃣ Shot Chart Data Quality Assessment")
        print("-" * 40)
        
        shot_quality = await analyze_shot_chart_quality(conn)
        print_shot_quality_metrics(shot_quality)
        
        # 3. Coordinate Data Completeness
        print("\n3️⃣ Coordinate Data Completeness")
        print("-" * 40)
        
        coord_quality = await analyze_coordinate_completeness(conn)
        print_coordinate_metrics(coord_quality)
        
        # 4. Event Type Distribution Analysis
        print("\n4️⃣ Event Type Distribution Analysis")
        print("-" * 40)
        
        event_dist = await analyze_event_distribution(conn)
        print_event_distribution(event_dist)
        
        # 5. Lineup Tracking Opportunities
        print("\n5️⃣ Lineup Tracking Opportunities")
        print("-" * 40)
        
        lineup_opportunities = await analyze_lineup_tracking_potential(conn)
        print_lineup_opportunities(lineup_opportunities)
        
        # 6. Data Gaps and Missing Information
        print("\n6️⃣ Data Gaps and Missing Information")
        print("-" * 40)
        
        data_gaps = await identify_data_gaps(conn)
        print_data_gaps(data_gaps)
        
        # 7. Game-by-Game Quality Report
        print("\n7️⃣ Sample Game Quality Reports")
        print("-" * 40)
        
        game_reports = await generate_sample_game_reports(conn)
        print_game_reports(game_reports)
        
        # 8. Tranche 2 Implementation Recommendations
        print("\n8️⃣ Tranche 2 Implementation Recommendations")
        print("-" * 50)
        
        recommendations = generate_tranche2_recommendations(
            metrics, shot_quality, coord_quality, event_dist, lineup_opportunities, data_gaps
        )
        print_recommendations(recommendations)
        
        # 9. Priority Matrix for Enhanced Analytics
        print("\n9️⃣ Priority Matrix for Enhanced Analytics")
        print("-" * 45)
        
        priority_matrix = create_priority_matrix(
            metrics, shot_quality, coord_quality, event_dist
        )
        print_priority_matrix(priority_matrix)
        
        print(f"\n✅ PBP Data Quality Analysis Complete!")
        print(f"📊 Ready to implement Tranche 2 enhanced analytics")
        
        return True
        
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def analyze_overall_coverage(conn) -> PbpQualityMetrics:
    """Analyze overall PBP data coverage."""
    
    # Total games and PBP coverage
    coverage_query = """
    WITH game_pbp_stats AS (
        SELECT 
            g.game_id,
            g.home_team_tricode,
            g.away_team_tricode,
            g.game_date_local,
            COUNT(p.event_idx) as event_count,
            COUNT(CASE WHEN p.shot_made IS NOT NULL THEN 1 END) as shot_events,
            COUNT(CASE WHEN p.shot_x IS NOT NULL AND p.shot_y IS NOT NULL THEN 1 END) as coord_events
        FROM games g
        LEFT JOIN pbp_events p ON g.game_id = p.game_id
        WHERE g.season = '2024-25' AND g.status = 'FINAL'
        GROUP BY g.game_id, g.home_team_tricode, g.away_team_tricode, g.game_date_local
    )
    SELECT 
        COUNT(*) as total_games,
        COUNT(CASE WHEN event_count > 0 THEN 1 END) as games_with_pbp,
        COUNT(CASE WHEN event_count = 0 THEN 1 END) as games_missing_pbp,
        COALESCE(SUM(event_count), 0) as total_events,
        COALESCE(SUM(shot_events), 0) as total_shot_events,
        COALESCE(SUM(coord_events), 0) as events_with_coordinates
    FROM game_pbp_stats
    """
    
    result = await conn.fetchrow(coverage_query)
    
    # Event type distribution
    event_types_query = """
    SELECT event_type, COUNT(*) as count
    FROM pbp_events p
    JOIN games g ON p.game_id = g.game_id
    WHERE g.season = '2024-25'
    GROUP BY event_type
    ORDER BY count DESC
    """
    
    event_types = await conn.fetch(event_types_query)
    
    # Period coverage
    period_query = """
    SELECT p.period, COUNT(*) as count
    FROM pbp_events p
    JOIN games g ON p.game_id = g.game_id
    WHERE g.season = '2024-25'
    GROUP BY p.period
    ORDER BY p.period
    """
    
    periods = await conn.fetch(period_query)
    
    return PbpQualityMetrics(
        total_games=result['total_games'],
        games_with_pbp=result['games_with_pbp'],
        games_missing_pbp=result['games_missing_pbp'],
        total_events=result['total_events'],
        shot_events=result['total_shot_events'],
        events_with_coordinates=result['events_with_coordinates'],
        events_missing_coordinates=result['total_shot_events'] - result['events_with_coordinates'],
        unique_event_types=[row['event_type'] for row in event_types],
        period_coverage={row['period']: row['count'] for row in periods},
        coordinate_quality_score=result['events_with_coordinates'] / max(result['total_shot_events'], 1) * 100
    )

async def analyze_shot_chart_quality(conn) -> Dict[str, Any]:
    """Analyze shot chart data quality and completeness."""
    
    shot_quality_query = """
    WITH shot_analysis AS (
        SELECT 
            p.event_type,
            COUNT(*) as total_shots,
            COUNT(p.shot_distance_ft) as shots_with_distance,
            COUNT(p.shot_x) as shots_with_x,
            COUNT(p.shot_y) as shots_with_y,
            COUNT(CASE WHEN p.shot_x IS NOT NULL AND p.shot_y IS NOT NULL THEN 1 END) as shots_with_coords,
            COUNT(p.shot_zone) as shots_with_zone,
            COUNT(p.shot_type) as shots_with_type,
            AVG(p.shot_distance_ft) as avg_distance,
            MIN(p.shot_distance_ft) as min_distance,
            MAX(p.shot_distance_ft) as max_distance
        FROM pbp_events p
        JOIN games g ON p.game_id = g.game_id
        WHERE g.season = '2024-25' 
        AND p.event_type IN ('SHOT_MADE', 'SHOT_MISSED')
        GROUP BY p.event_type
    )
    SELECT * FROM shot_analysis
    ORDER BY total_shots DESC
    """
    
    shot_results = await conn.fetch(shot_quality_query)
    
    # Shot zone distribution
    zone_query = """
    SELECT 
        shot_zone,
        COUNT(*) as count,
        AVG(CASE WHEN shot_made THEN 1.0 ELSE 0.0 END) as fg_pct
    FROM pbp_events p
    JOIN games g ON p.game_id = g.game_id
    WHERE g.season = '2024-25' 
    AND p.event_type IN ('SHOT_MADE', 'SHOT_MISSED')
    AND p.shot_zone IS NOT NULL
    GROUP BY shot_zone
    ORDER BY count DESC
    """
    
    zone_results = await conn.fetch(zone_query)
    
    return {
        'shot_analysis': [dict(row) for row in shot_results],
        'zone_distribution': [dict(row) for row in zone_results]
    }

async def analyze_coordinate_completeness(conn) -> Dict[str, Any]:
    """Analyze coordinate data completeness and quality."""
    
    coord_query = """
    WITH coordinate_analysis AS (
        SELECT 
            g.game_date_local,
            COUNT(CASE WHEN p.event_type IN ('SHOT_MADE', 'SHOT_MISSED') THEN 1 END) as total_shots,
            COUNT(CASE WHEN p.shot_x IS NOT NULL AND p.event_type IN ('SHOT_MADE', 'SHOT_MISSED') THEN 1 END) as shots_with_x,
            COUNT(CASE WHEN p.shot_y IS NOT NULL AND p.event_type IN ('SHOT_MADE', 'SHOT_MISSED') THEN 1 END) as shots_with_y,
            COUNT(CASE WHEN p.shot_x IS NOT NULL AND p.shot_y IS NOT NULL AND p.event_type IN ('SHOT_MADE', 'SHOT_MISSED') THEN 1 END) as shots_with_both_coords
        FROM games g
        LEFT JOIN pbp_events p ON g.game_id = p.game_id
        WHERE g.season = '2024-25' AND g.status = 'FINAL'
        GROUP BY g.game_date_local
        HAVING COUNT(CASE WHEN p.event_type IN ('SHOT_MADE', 'SHOT_MISSED') THEN 1 END) > 0
    )
    SELECT 
        COUNT(*) as games_analyzed,
        AVG(CASE WHEN total_shots > 0 THEN shots_with_both_coords::float / total_shots * 100 ELSE 0 END) as avg_coord_coverage,
        MIN(CASE WHEN total_shots > 0 THEN shots_with_both_coords::float / total_shots * 100 ELSE 0 END) as min_coord_coverage,
        MAX(CASE WHEN total_shots > 0 THEN shots_with_both_coords::float / total_shots * 100 ELSE 0 END) as max_coord_coverage,
        SUM(total_shots) as total_shots_analyzed,
        SUM(shots_with_both_coords) as total_shots_with_coords
    FROM coordinate_analysis
    """
    
    coord_result = await conn.fetchrow(coord_query)
    
    # Recent games coordinate quality
    recent_query = """
    SELECT 
        g.game_id,
        g.home_team_tricode || ' vs ' || g.away_team_tricode as matchup,
        g.game_date_local,
        COUNT(CASE WHEN p.event_type IN ('SHOT_MADE', 'SHOT_MISSED') THEN 1 END) as shots,
        COUNT(CASE WHEN p.shot_x IS NOT NULL AND p.shot_y IS NOT NULL AND p.event_type IN ('SHOT_MADE', 'SHOT_MISSED') THEN 1 END) as shots_with_coords,
        CASE 
            WHEN COUNT(CASE WHEN p.event_type IN ('SHOT_MADE', 'SHOT_MISSED') THEN 1 END) > 0 
            THEN COUNT(CASE WHEN p.shot_x IS NOT NULL AND p.shot_y IS NOT NULL AND p.event_type IN ('SHOT_MADE', 'SHOT_MISSED') THEN 1 END)::float / COUNT(CASE WHEN p.event_type IN ('SHOT_MADE', 'SHOT_MISSED') THEN 1 END) * 100 
            ELSE 0 
        END as coord_pct
    FROM games g
    LEFT JOIN pbp_events p ON g.game_id = p.game_id
    WHERE g.season = '2024-25' AND g.status = 'FINAL'
    GROUP BY g.game_id, g.home_team_tricode, g.away_team_tricode, g.game_date_local
    ORDER BY g.game_date_local DESC
    LIMIT 10
    """
    
    recent_games = await conn.fetch(recent_query)
    
    return {
        'overall_stats': dict(coord_result),
        'recent_games': [dict(row) for row in recent_games]
    }

async def analyze_event_distribution(conn) -> Dict[str, Any]:
    """Analyze distribution of different event types."""
    
    event_dist_query = """
    WITH event_analysis AS (
        SELECT 
            p.event_type,
            COUNT(*) as total_count,
            COUNT(DISTINCT p.game_id) as games_with_event,
            AVG(COUNT(*)) OVER () as avg_per_type,
            COUNT(p.team_tricode) as events_with_team,
            COUNT(p.player1_name_slug) as events_with_player
        FROM pbp_events p
        JOIN games g ON p.game_id = g.game_id
        WHERE g.season = '2024-25'
        GROUP BY p.event_type
    )
    SELECT 
        event_type,
        total_count,
        games_with_event,
        ROUND((total_count::numeric / games_with_event), 2) as avg_per_game,
        ROUND((total_count::numeric / SUM(total_count) OVER () * 100), 2) as pct_of_total,
        events_with_team,
        events_with_player,
        ROUND((events_with_team::numeric / total_count * 100), 2) as team_completeness_pct,
        ROUND((events_with_player::numeric / total_count * 100), 2) as player_completeness_pct
    FROM event_analysis
    ORDER BY total_count DESC
    """
    
    event_results = await conn.fetch(event_dist_query)
    
    return {
        'distribution': [dict(row) for row in event_results]
    }

async def analyze_lineup_tracking_potential(conn) -> Dict[str, Any]:
    """Analyze potential for lineup tracking and substitution analysis."""
    
    substitution_query = """
    WITH sub_analysis AS (
        SELECT 
            COUNT(*) as total_substitutions,
            COUNT(DISTINCT p.game_id) as games_with_subs,
            COUNT(p.player1_name_slug) as subs_with_player_out,
            COUNT(p.player2_name_slug) as subs_with_player_in,
            AVG(COUNT(*)) OVER () as avg_subs_per_game
        FROM pbp_events p
        JOIN games g ON p.game_id = g.game_id
        WHERE g.season = '2024-25' 
        AND p.event_type = 'SUBSTITUTION'
        GROUP BY p.game_id
    )
    SELECT 
        COUNT(*) as games_analyzed,
        AVG(total_substitutions) as avg_subs_per_game,
        MIN(total_substitutions) as min_subs_per_game,
        MAX(total_substitutions) as max_subs_per_game,
        AVG(CASE WHEN subs_with_player_out > 0 THEN subs_with_player_out::float / total_substitutions * 100 ELSE 0 END) as avg_player_out_completeness,
        AVG(CASE WHEN subs_with_player_in > 0 THEN subs_with_player_in::float / total_substitutions * 100 ELSE 0 END) as avg_player_in_completeness
    FROM sub_analysis
    """
    
    sub_result = await conn.fetchrow(substitution_query)
    
    # Starting lineup coverage
    lineup_query = """
    WITH lineup_coverage AS (
        SELECT 
            g.game_id,
            g.home_team_tricode,
            g.away_team_tricode,
            COUNT(CASE WHEN s.team_tricode = g.home_team_tricode THEN 1 END) as home_starters,
            COUNT(CASE WHEN s.team_tricode = g.away_team_tricode THEN 1 END) as away_starters
        FROM games g
        LEFT JOIN starting_lineups s ON g.game_id = s.game_id
        WHERE g.season = '2024-25' AND g.status = 'FINAL'
        GROUP BY g.game_id, g.home_team_tricode, g.away_team_tricode
    )
    SELECT 
        COUNT(*) as total_games,
        COUNT(CASE WHEN home_starters = 5 AND away_starters = 5 THEN 1 END) as games_with_complete_lineups,
        AVG(home_starters + away_starters) as avg_starters_per_game
    FROM lineup_coverage
    """
    
    lineup_result = await conn.fetchrow(lineup_query)
    
    return {
        'substitution_analysis': dict(sub_result),
        'lineup_coverage': dict(lineup_result)
    }

async def identify_data_gaps(conn) -> Dict[str, Any]:
    """Identify key data gaps and missing information."""
    
    gaps_query = """
    WITH data_gaps AS (
        SELECT 
            g.game_id,
            g.home_team_tricode || ' vs ' || g.away_team_tricode as matchup,
            g.game_date_local,
            COUNT(p.event_idx) as total_events,
            COUNT(CASE WHEN p.event_type IN ('SHOT_MADE', 'SHOT_MISSED') THEN 1 END) as shot_events,
            COUNT(CASE WHEN p.shot_distance_ft IS NULL AND p.event_type IN ('SHOT_MADE', 'SHOT_MISSED') THEN 1 END) as shots_missing_distance,
            COUNT(CASE WHEN p.shot_x IS NULL AND p.event_type IN ('SHOT_MADE', 'SHOT_MISSED') THEN 1 END) as shots_missing_x,
            COUNT(CASE WHEN p.shot_y IS NULL AND p.event_type IN ('SHOT_MADE', 'SHOT_MISSED') THEN 1 END) as shots_missing_y,
            COUNT(CASE WHEN p.team_tricode IS NULL THEN 1 END) as events_missing_team,
            COUNT(CASE WHEN p.player1_name_slug IS NULL AND p.event_type NOT IN ('PERIOD_BEGIN', 'PERIOD_END', 'GAME_END') THEN 1 END) as events_missing_player
        FROM games g
        LEFT JOIN pbp_events p ON g.game_id = p.game_id
        WHERE g.season = '2024-25' AND g.status = 'FINAL'
        GROUP BY g.game_id, g.home_team_tricode, g.away_team_tricode, g.game_date_local
        HAVING COUNT(p.event_idx) > 0
    )
    SELECT 
        COUNT(*) as games_with_data,
        AVG(shots_missing_distance::float / NULLIF(shot_events, 0) * 100) as avg_pct_shots_missing_distance,
        AVG(shots_missing_x::float / NULLIF(shot_events, 0) * 100) as avg_pct_shots_missing_x,
        AVG(shots_missing_y::float / NULLIF(shot_events, 0) * 100) as avg_pct_shots_missing_y,
        AVG(events_missing_team::float / NULLIF(total_events, 0) * 100) as avg_pct_events_missing_team,
        AVG(events_missing_player::float / NULLIF(total_events, 0) * 100) as avg_pct_events_missing_player,
        SUM(shots_missing_distance) as total_shots_missing_distance,
        SUM(shots_missing_x) as total_shots_missing_x,
        SUM(events_missing_team) as total_events_missing_team,
        SUM(events_missing_player) as total_events_missing_player
    FROM data_gaps
    """
    
    gaps_result = await conn.fetchrow(gaps_query)
    
    return {
        'gap_analysis': dict(gaps_result)
    }

async def generate_sample_game_reports(conn) -> List[GameQualityReport]:
    """Generate quality reports for sample games."""
    
    sample_games_query = """
    WITH game_quality AS (
        SELECT 
            g.game_id,
            g.home_team_tricode,
            g.away_team_tricode,
            g.game_date_local,
            COUNT(p.event_idx) as total_events,
            COUNT(CASE WHEN p.event_type IN ('SHOT_MADE', 'SHOT_MISSED') THEN 1 END) as shot_events,
            CASE 
                WHEN COUNT(CASE WHEN p.event_type IN ('SHOT_MADE', 'SHOT_MISSED') THEN 1 END) > 0
                THEN COUNT(CASE WHEN p.shot_x IS NOT NULL AND p.shot_y IS NOT NULL AND p.event_type IN ('SHOT_MADE', 'SHOT_MISSED') THEN 1 END)::float / COUNT(CASE WHEN p.event_type IN ('SHOT_MADE', 'SHOT_MISSED') THEN 1 END) * 100
                ELSE 0
            END as coordinate_coverage,
            ARRAY_AGG(DISTINCT 
                CASE 
                    WHEN p.shot_distance_ft IS NULL AND p.event_type IN ('SHOT_MADE', 'SHOT_MISSED') THEN 'Missing shot distance'
                    WHEN p.shot_x IS NULL AND p.event_type IN ('SHOT_MADE', 'SHOT_MISSED') THEN 'Missing shot coordinates'
                    WHEN p.team_tricode IS NULL THEN 'Missing team attribution'
                    WHEN p.player1_name_slug IS NULL AND p.event_type NOT IN ('PERIOD_BEGIN', 'PERIOD_END') THEN 'Missing player attribution'
                END
            ) FILTER (WHERE 
                (p.shot_distance_ft IS NULL AND p.event_type IN ('SHOT_MADE', 'SHOT_MISSED')) OR
                (p.shot_x IS NULL AND p.event_type IN ('SHOT_MADE', 'SHOT_MISSED')) OR
                (p.team_tricode IS NULL) OR
                (p.player1_name_slug IS NULL AND p.event_type NOT IN ('PERIOD_BEGIN', 'PERIOD_END'))
            ) as missing_data_flags
        FROM games g
        LEFT JOIN pbp_events p ON g.game_id = p.game_id
        WHERE g.season = '2024-25' AND g.status = 'FINAL'
        GROUP BY g.game_id, g.home_team_tricode, g.away_team_tricode, g.game_date_local
        HAVING COUNT(p.event_idx) > 0
        ORDER BY g.game_date_local DESC
        LIMIT 5
    )
    SELECT * FROM game_quality
    """
    
    games = await conn.fetch(sample_games_query)
    
    reports = []
    for game in games:
        quality_score = calculate_game_quality_score(
            game['total_events'],
            game['shot_events'], 
            game['coordinate_coverage'],
            game['missing_data_flags']
        )
        
        reports.append(GameQualityReport(
            game_id=game['game_id'],
            home_team=game['home_team_tricode'],
            away_team=game['away_team_tricode'],
            game_date=game['game_date_local'],
            total_events=game['total_events'],
            shot_events=game['shot_events'],
            coordinate_coverage=game['coordinate_coverage'],
            missing_data_flags=game['missing_data_flags'] or [],
            quality_score=quality_score
        ))
    
    return reports

def calculate_game_quality_score(total_events: int, shot_events: int, coord_coverage: float, missing_flags: List[str]) -> float:
    """Calculate a quality score for a game (0-100)."""
    base_score = 100.0
    
    # Penalize for missing data
    if missing_flags:
        base_score -= len(missing_flags) * 10
    
    # Reward good coordinate coverage
    base_score = base_score * (coord_coverage / 100.0)
    
    # Penalize for too few events (likely incomplete data)
    if total_events < 200:  # Typical NBA game has 400+ events
        base_score *= 0.8
    
    if shot_events < 50:  # Typical NBA game has 80+ shot attempts
        base_score *= 0.9
    
    return max(0.0, min(100.0, base_score))

def print_coverage_metrics(metrics: PbpQualityMetrics):
    """Print overall coverage metrics."""
    print(f"📊 Total Games Analyzed: {metrics.total_games}")
    print(f"✅ Games with PBP Data: {metrics.games_with_pbp} ({metrics.games_with_pbp/max(metrics.total_games,1)*100:.1f}%)")
    print(f"❌ Games Missing PBP: {metrics.games_missing_pbp}")
    print(f"🎯 Total PBP Events: {metrics.total_events:,}")
    print(f"🏀 Shot Events: {metrics.shot_events:,}")
    print(f"📍 Events with Coordinates: {metrics.events_with_coordinates:,} ({metrics.coordinate_quality_score:.1f}%)")
    print(f"🔢 Unique Event Types: {len(metrics.unique_event_types)}")

def print_shot_quality_metrics(shot_quality: Dict[str, Any]):
    """Print shot chart quality metrics."""
    for shot_type in shot_quality['shot_analysis']:
        print(f"🎯 {shot_type['event_type']}:")
        print(f"   Total: {shot_type['total_shots']:,}")
        print(f"   With Distance: {shot_type['shots_with_distance']:,} ({shot_type['shots_with_distance']/max(shot_type['total_shots'],1)*100:.1f}%)")
        print(f"   With Coordinates: {shot_type['shots_with_coords']:,} ({shot_type['shots_with_coords']/max(shot_type['total_shots'],1)*100:.1f}%)")
        if shot_type['avg_distance']:
            print(f"   Avg Distance: {shot_type['avg_distance']:.1f} ft")
    
    if shot_quality['zone_distribution']:
        print(f"\n📍 Shot Zone Distribution:")
        for zone in shot_quality['zone_distribution'][:5]:  # Top 5 zones
            print(f"   {zone['shot_zone'] or 'Unknown'}: {zone['count']:,} shots ({zone['fg_pct']*100:.1f}% FG)")

def print_coordinate_metrics(coord_quality: Dict[str, Any]):
    """Print coordinate completeness metrics."""
    stats = coord_quality['overall_stats']
    print(f"📊 Games Analyzed: {stats['games_analyzed']}")
    print(f"📍 Average Coordinate Coverage: {stats['avg_coord_coverage']:.1f}%")
    print(f"📍 Coverage Range: {stats['min_coord_coverage']:.1f}% - {stats['max_coord_coverage']:.1f}%")
    print(f"🎯 Total Shots with Coordinates: {stats['total_shots_with_coords']:,} / {stats['total_shots_analyzed']:,}")

def print_event_distribution(event_dist: Dict[str, Any]):
    """Print event type distribution."""
    print("📊 Top Event Types:")
    for event in event_dist['distribution'][:10]:  # Top 10 event types
        print(f"   {event['event_type']}: {event['total_count']:,} ({event['pct_of_total']:.1f}%) - {event['avg_per_game']:.1f}/game")

def print_lineup_opportunities(lineup_opportunities: Dict[str, Any]):
    """Print lineup tracking opportunities."""
    sub_analysis = lineup_opportunities['substitution_analysis']
    lineup_coverage = lineup_opportunities['lineup_coverage']
    
    print(f"🔄 Substitution Analysis:")
    print(f"   Games Analyzed: {sub_analysis['games_analyzed']}")
    print(f"   Avg Substitutions/Game: {sub_analysis['avg_subs_per_game']:.1f}")
    print(f"   Player Out Completeness: {sub_analysis['avg_player_out_completeness']:.1f}%")
    print(f"   Player In Completeness: {sub_analysis['avg_player_in_completeness']:.1f}%")
    
    print(f"\n👥 Starting Lineup Coverage:")
    print(f"   Total Games: {lineup_coverage['total_games']}")
    print(f"   Complete Lineups (5v5): {lineup_coverage['games_with_complete_lineups']} ({lineup_coverage['games_with_complete_lineups']/max(lineup_coverage['total_games'],1)*100:.1f}%)")

def print_data_gaps(data_gaps: Dict[str, Any]):
    """Print data gap analysis."""
    gaps = data_gaps['gap_analysis']
    print(f"📊 Data Gap Analysis:")
    print(f"   Shots Missing Distance: {gaps['avg_pct_shots_missing_distance']:.1f}% avg")
    print(f"   Shots Missing X Coordinate: {gaps['avg_pct_shots_missing_x']:.1f}% avg")
    print(f"   Shots Missing Y Coordinate: {gaps['avg_pct_shots_missing_y']:.1f}% avg")
    print(f"   Events Missing Team: {gaps['avg_pct_events_missing_team']:.1f}% avg")
    print(f"   Events Missing Player: {gaps['avg_pct_events_missing_player']:.1f}% avg")

def print_game_reports(game_reports: List[GameQualityReport]):
    """Print sample game quality reports."""
    for report in game_reports:
        print(f"🎮 {report.home_team} vs {report.away_team} ({report.game_date})")
        print(f"   Quality Score: {report.quality_score:.1f}/100")
        print(f"   Events: {report.total_events}, Shots: {report.shot_events}")
        print(f"   Coordinate Coverage: {report.coordinate_coverage:.1f}%")
        if report.missing_data_flags:
            print(f"   Issues: {', '.join(report.missing_data_flags)}")
        print()

def generate_tranche2_recommendations(metrics, shot_quality, coord_quality, event_dist, lineup_opportunities, data_gaps) -> List[str]:
    """Generate Tranche 2 implementation recommendations."""
    recommendations = []
    
    # Shot chart recommendations
    if metrics.coordinate_quality_score < 80:
        recommendations.append("🎯 HIGH PRIORITY: Enhance shot chart coordinate collection - only {:.1f}% coverage".format(metrics.coordinate_quality_score))
    
    # Lineup tracking recommendations
    lineup_coverage = lineup_opportunities['lineup_coverage']
    complete_lineup_pct = lineup_coverage['games_with_complete_lineups'] / max(lineup_coverage['total_games'], 1) * 100
    if complete_lineup_pct < 90:
        recommendations.append("👥 MEDIUM PRIORITY: Improve starting lineup data collection - {:.1f}% complete".format(complete_lineup_pct))
    
    # Event enrichment recommendations
    if len(metrics.unique_event_types) > 15:
        recommendations.append("🔍 LOW PRIORITY: Standardize event type taxonomy - {} unique types detected".format(len(metrics.unique_event_types)))
    
    # Data quality recommendations
    gaps = data_gaps['gap_analysis']
    if gaps['avg_pct_shots_missing_distance'] > 20:
        recommendations.append("📏 HIGH PRIORITY: Improve shot distance calculation - {:.1f}% missing".format(gaps['avg_pct_shots_missing_distance']))
    
    # Advanced analytics opportunities
    if metrics.shot_events > 10000:
        recommendations.append("📊 OPPORTUNITY: Large shot dataset ({:,} shots) ready for advanced analytics".format(metrics.shot_events))
    
    return recommendations

def print_recommendations(recommendations: List[str]):
    """Print implementation recommendations."""
    for i, rec in enumerate(recommendations, 1):
        print(f"{i}. {rec}")

def create_priority_matrix(metrics, shot_quality, coord_quality, event_dist) -> Dict[str, List[str]]:
    """Create priority matrix for Tranche 2 features."""
    
    high_priority = []
    medium_priority = []
    low_priority = []
    
    # Shot analytics priority
    if metrics.coordinate_quality_score >= 70:
        high_priority.append("Shot Zone Classification & Expected FG% Models")
    else:
        medium_priority.append("Shot Zone Classification (after coordinate improvement)")
    
    # Lineup analytics priority
    if coord_quality['overall_stats']['avg_coord_coverage'] >= 60:
        high_priority.append("Lineup Effectiveness Metrics")
    else:
        medium_priority.append("Lineup Effectiveness Metrics")
    
    # Play-by-play enhancements
    if metrics.total_events > 50000:
        high_priority.append("Advanced PBP Event Classification")
        medium_priority.append("Transition Detection & Early Clock Analysis")
    else:
        low_priority.append("Advanced PBP Event Classification")
    
    # Always add these based on data readiness
    medium_priority.extend([
        "Shot Chart Coordinate Validation",
        "Plus/Minus by Lineup Calculations"
    ])
    
    low_priority.extend([
        "Substitution Pattern Analysis",
        "Player Tracking Integration"
    ])
    
    return {
        "High Priority (Implement First)": high_priority,
        "Medium Priority (Next Phase)": medium_priority,
        "Low Priority (Future Enhancement)": low_priority
    }

def print_priority_matrix(priority_matrix: Dict[str, List[str]]):
    """Print the priority matrix."""
    for category, items in priority_matrix.items():
        print(f"\n{category}:")
        for item in items:
            print(f"  • {item}")

async def main():
    """Run the PBP data quality analysis."""
    success = await analyze_pbp_data_quality()
    return success

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)