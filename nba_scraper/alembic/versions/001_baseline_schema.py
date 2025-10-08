"""Baseline schema migration

Revision ID: 001_baseline_schema
Revises:
Create Date: 2025-10-07

This migration captures the current production schema as documented in schema.sql.
All future migrations will build upon this baseline.

Schema includes:
- Core game tracking (games, crosswalks, officials)
- Player tracking (lineups, injuries)
- Play-by-play events with shot details
- Q1 window analytics (12:00-8:00)
- Early shock events detection
- Schedule and travel analysis
- Game outcomes
- Pipeline state management
- NBA API advanced metrics (Tranche 1)
- Team and player game statistics
"""
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "001_baseline_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create all baseline tables matching schema.sql"""

    # Enable extensions (PostgreSQL only)
    # Skip for SQLite which doesn't support extensions
    if op.get_context().dialect.name == 'postgresql':
        op.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')

    # Games table - core game information
    op.create_table(
        "games",
        sa.Column("game_id", sa.Text(), nullable=False),
        sa.Column("bref_game_id", sa.Text(), nullable=True),
        sa.Column("season", sa.Text(), nullable=False),
        sa.Column("game_date_utc", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("game_date_local", sa.Date(), nullable=False),
        sa.Column("arena_tz", sa.Text(), nullable=False),
        sa.Column("home_team_tricode", sa.Text(), nullable=False),
        sa.Column("away_team_tricode", sa.Text(), nullable=False),
        sa.Column("home_team_id", sa.Text(), nullable=True),
        sa.Column("away_team_id", sa.Text(), nullable=True),
        sa.Column("odds_join_key", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default="SCHEDULED"),
        sa.Column("period", sa.Integer(), nullable=True, server_default="0"),
        sa.Column("time_remaining", sa.Text(), nullable=True),
        sa.Column("arena_name", sa.Text(), nullable=True),
        sa.Column("attendance", sa.Integer(), nullable=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("ingested_at_utc", sa.TIMESTAMP(timezone=True), server_default=sa.text("NOW()")),
        sa.PrimaryKeyConstraint("game_id"),
    )

    # Game ID crosswalk
    op.create_table(
        "game_id_crosswalk",
        sa.Column("game_id", sa.Text(), nullable=False),
        sa.Column("bref_game_id", sa.Text(), nullable=False),
        sa.Column("nba_stats_game_id", sa.Text(), nullable=True),
        sa.Column("espn_game_id", sa.Text(), nullable=True),
        sa.Column("yahoo_game_id", sa.Text(), nullable=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("ingested_at_utc", sa.TIMESTAMP(timezone=True), server_default=sa.text("NOW()")),
        sa.PrimaryKeyConstraint("game_id", "bref_game_id"),
    )

    # Referee assignments
    op.create_table(
        "ref_assignments",
        sa.Column("game_id", sa.Text(), nullable=False),
        sa.Column("referee_name_slug", sa.Text(), nullable=False),
        sa.Column("referee_display_name", sa.Text(), nullable=False),
        sa.Column("role", sa.Text(), nullable=False),
        sa.Column("crew_position", sa.Integer(), nullable=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("ingested_at_utc", sa.TIMESTAMP(timezone=True), server_default=sa.text("NOW()")),
        sa.PrimaryKeyConstraint("game_id", "referee_name_slug"),
        sa.CheckConstraint(
            "role IN ('CREW_CHIEF', 'REFEREE', 'UMPIRE', 'OFFICIAL')", name="ref_role_check"
        ),
    )

    # Referee alternates
    op.create_table(
        "ref_alternates",
        sa.Column("game_id", sa.Text(), nullable=False),
        sa.Column("referee_name_slug", sa.Text(), nullable=False),
        sa.Column("referee_display_name", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("ingested_at_utc", sa.TIMESTAMP(timezone=True), server_default=sa.text("NOW()")),
        sa.PrimaryKeyConstraint("game_id", "referee_name_slug"),
    )

    # Starting lineups
    op.create_table(
        "starting_lineups",
        sa.Column("game_id", sa.Text(), nullable=False),
        sa.Column("team_tricode", sa.Text(), nullable=False),
        sa.Column("player_name_slug", sa.Text(), nullable=False),
        sa.Column("player_display_name", sa.Text(), nullable=False),
        sa.Column("player_id", sa.Text(), nullable=True),
        sa.Column("position", sa.Text(), nullable=True),
        sa.Column("jersey_number", sa.Integer(), nullable=True),
        sa.Column("final_pre_tip", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("ingested_at_utc", sa.TIMESTAMP(timezone=True), server_default=sa.text("NOW()")),
        sa.PrimaryKeyConstraint("game_id", "team_tricode", "player_name_slug"),
    )

    # Injury status
    op.create_table(
        "injury_status",
        sa.Column("game_id", sa.Text(), nullable=False),
        sa.Column("team_tricode", sa.Text(), nullable=False),
        sa.Column("player_name_slug", sa.Text(), nullable=False),
        sa.Column("player_display_name", sa.Text(), nullable=False),
        sa.Column("player_id", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("snapshot_utc", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("ingested_at_utc", sa.TIMESTAMP(timezone=True), server_default=sa.text("NOW()")),
        sa.PrimaryKeyConstraint("game_id", "team_tricode", "player_name_slug", "snapshot_utc"),
        sa.CheckConstraint(
            "status IN ('OUT', 'QUESTIONABLE', 'PROBABLE', 'ACTIVE', 'DNP', 'INACTIVE')",
            name="injury_status_check",
        ),
    )

    # Play-by-play events
    op.create_table(
        "pbp_events",
        sa.Column("game_id", sa.Text(), nullable=False),
        sa.Column("period", sa.Integer(), nullable=False),
        sa.Column("event_idx", sa.Integer(), nullable=False),
        sa.Column("event_id", sa.Text(), nullable=True),
        sa.Column("time_remaining", sa.Text(), nullable=True),
        sa.Column("seconds_elapsed", sa.Numeric(), nullable=True),
        sa.Column("score_home", sa.Integer(), nullable=True),
        sa.Column("score_away", sa.Integer(), nullable=True),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("event_subtype", sa.Text(), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("team_tricode", sa.Text(), nullable=True),
        sa.Column("player1_name_slug", sa.Text(), nullable=True),
        sa.Column("player1_display_name", sa.Text(), nullable=True),
        sa.Column("player1_id", sa.Text(), nullable=True),
        sa.Column("player2_name_slug", sa.Text(), nullable=True),
        sa.Column("player2_display_name", sa.Text(), nullable=True),
        sa.Column("player2_id", sa.Text(), nullable=True),
        sa.Column("player3_name_slug", sa.Text(), nullable=True),
        sa.Column("player3_display_name", sa.Text(), nullable=True),
        sa.Column("player3_id", sa.Text(), nullable=True),
        sa.Column("shot_made", sa.Boolean(), nullable=True),
        sa.Column("shot_value", sa.Integer(), nullable=True),
        sa.Column("shot_type", sa.Text(), nullable=True),
        sa.Column("shot_zone", sa.Text(), nullable=True),
        sa.Column("shot_distance_ft", sa.Numeric(), nullable=True),
        sa.Column("shot_x", sa.Numeric(), nullable=True),
        sa.Column("shot_y", sa.Numeric(), nullable=True),
        sa.Column("shot_clock_seconds", sa.Numeric(), nullable=True),
        sa.Column("possession_team", sa.Text(), nullable=True),
        sa.Column("is_transition", sa.Boolean(), server_default="false"),
        sa.Column("is_early_clock", sa.Boolean(), server_default="false"),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("ingested_at_utc", sa.TIMESTAMP(timezone=True), server_default=sa.text("NOW()")),
        sa.PrimaryKeyConstraint("game_id", "period", "event_idx"),
    )

    # Q1 window analysis
    op.create_table(
        "q1_window_12_8",
        sa.Column("game_id", sa.Text(), nullable=False),
        sa.Column("home_team_tricode", sa.Text(), nullable=False),
        sa.Column("away_team_tricode", sa.Text(), nullable=False),
        sa.Column("possessions_elapsed", sa.Integer(), nullable=False),
        sa.Column("pace48_actual", sa.Numeric(), nullable=True),
        sa.Column("pace48_expected", sa.Numeric(), nullable=True),
        sa.Column("home_efg_actual", sa.Numeric(), nullable=True),
        sa.Column("home_efg_expected", sa.Numeric(), nullable=True),
        sa.Column("away_efg_actual", sa.Numeric(), nullable=True),
        sa.Column("away_efg_expected", sa.Numeric(), nullable=True),
        sa.Column("home_to_rate", sa.Numeric(), nullable=True),
        sa.Column("away_to_rate", sa.Numeric(), nullable=True),
        sa.Column("home_ft_rate", sa.Numeric(), nullable=True),
        sa.Column("away_ft_rate", sa.Numeric(), nullable=True),
        sa.Column("home_orb_pct", sa.Numeric(), nullable=True),
        sa.Column("home_drb_pct", sa.Numeric(), nullable=True),
        sa.Column("away_orb_pct", sa.Numeric(), nullable=True),
        sa.Column("away_drb_pct", sa.Numeric(), nullable=True),
        sa.Column("bonus_time_home_sec", sa.Numeric(), server_default="0"),
        sa.Column("bonus_time_away_sec", sa.Numeric(), server_default="0"),
        sa.Column("transition_rate", sa.Numeric(), nullable=True),
        sa.Column("early_clock_rate", sa.Numeric(), nullable=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("ingested_at_utc", sa.TIMESTAMP(timezone=True), server_default=sa.text("NOW()")),
        sa.PrimaryKeyConstraint("game_id"),
    )

    # Early shocks
    op.create_table(
        "early_shocks",
        sa.Column("game_id", sa.Text(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("period", sa.Integer(), nullable=False),
        sa.Column("time_remaining", sa.Text(), nullable=False),
        sa.Column("seconds_elapsed", sa.Numeric(), nullable=False),
        sa.Column("player_name_slug", sa.Text(), nullable=True),
        sa.Column("player_display_name", sa.Text(), nullable=True),
        sa.Column("team_tricode", sa.Text(), nullable=True),
        sa.Column("severity", sa.Text(), nullable=True),
        sa.Column("immediate_sub", sa.Boolean(), server_default="false"),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("ingested_at_utc", sa.TIMESTAMP(timezone=True), server_default=sa.text("NOW()")),
        sa.PrimaryKeyConstraint(
            "game_id", "event_type", "period", "seconds_elapsed", "player_name_slug"
        ),
        sa.CheckConstraint(
            "event_type IN ('EARLY_FOUL_TROUBLE', 'TECHNICAL', 'FLAGRANT', 'INJURY_EXIT')",
            name="shock_event_type_check",
        ),
    )

    # Schedule and travel
    op.create_table(
        "schedule_travel",
        sa.Column("game_id", sa.Text(), nullable=False),
        sa.Column("team_tricode", sa.Text(), nullable=False),
        sa.Column("is_back_to_back", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("is_3_in_4", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("is_5_in_7", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("days_rest", sa.Integer(), nullable=False),
        sa.Column("timezone_shift_hours", sa.Numeric(), server_default="0"),
        sa.Column("circadian_index", sa.Numeric(), nullable=True),
        sa.Column("altitude_change_m", sa.Numeric(), server_default="0"),
        sa.Column("travel_distance_km", sa.Numeric(), server_default="0"),
        sa.Column("prev_game_date", sa.Date(), nullable=True),
        sa.Column("prev_arena_tz", sa.Text(), nullable=True),
        sa.Column("prev_lat", sa.Numeric(), nullable=True),
        sa.Column("prev_lon", sa.Numeric(), nullable=True),
        sa.Column("prev_altitude_m", sa.Numeric(), nullable=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("ingested_at_utc", sa.TIMESTAMP(timezone=True), server_default=sa.text("NOW()")),
        sa.PrimaryKeyConstraint("game_id"),
    )

    # Outcomes
    op.create_table(
        "outcomes",
        sa.Column("game_id", sa.Text(), nullable=False),
        sa.Column("home_team_tricode", sa.Text(), nullable=False),
        sa.Column("away_team_tricode", sa.Text(), nullable=False),
        sa.Column("q1_home_points", sa.Integer(), nullable=True),
        sa.Column("q1_away_points", sa.Integer(), nullable=True),
        sa.Column("final_home_points", sa.Integer(), nullable=False),
        sa.Column("final_away_points", sa.Integer(), nullable=False),
        sa.Column("total_points", sa.Integer(), nullable=False),
        sa.Column("home_win", sa.Boolean(), nullable=False),
        sa.Column("margin", sa.Integer(), nullable=False),
        sa.Column("overtime_periods", sa.Integer(), server_default="0"),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("ingested_at_utc", sa.TIMESTAMP(timezone=True), server_default=sa.text("NOW()")),
        sa.PrimaryKeyConstraint("game_id"),
    )

    # Pipeline state
    op.create_table(
        "pipeline_state",
        sa.Column("pipeline_name", sa.Text(), nullable=False),
        sa.Column("game_id", sa.Text(), nullable=True),
        sa.Column("date_key", sa.Date(), nullable=True),
        sa.Column("step_name", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("started_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("completed_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column(
            "metadata",
            postgresql.JSONB(astext_type=sa.Text()) if op.get_context().dialect.name == 'postgresql' else sa.JSON(),
            nullable=True
        ),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("NOW()")),
        sa.PrimaryKeyConstraint("pipeline_name", "game_id", "date_key", "step_name"),
        sa.CheckConstraint(
            "status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED')", name="pipeline_status_check"
        ),
    )

    # Advanced player stats
    op.create_table(
        "advanced_player_stats",
        sa.Column("game_id", sa.Text(), nullable=False),
        sa.Column("player_id", sa.Text(), nullable=False),
        sa.Column("player_name", sa.Text(), nullable=False),
        sa.Column("team_id", sa.Text(), nullable=True),
        sa.Column("team_abbreviation", sa.Text(), nullable=False),
        sa.Column("offensive_rating", sa.Numeric(), nullable=True),
        sa.Column("defensive_rating", sa.Numeric(), nullable=True),
        sa.Column("net_rating", sa.Numeric(), nullable=True),
        sa.Column("assist_percentage", sa.Numeric(), nullable=True),
        sa.Column("assist_to_turnover", sa.Numeric(), nullable=True),
        sa.Column("assist_ratio", sa.Numeric(), nullable=True),
        sa.Column("offensive_rebound_pct", sa.Numeric(), nullable=True),
        sa.Column("defensive_rebound_pct", sa.Numeric(), nullable=True),
        sa.Column("rebound_pct", sa.Numeric(), nullable=True),
        sa.Column("turnover_ratio", sa.Numeric(), nullable=True),
        sa.Column("effective_fg_pct", sa.Numeric(), nullable=True),
        sa.Column("true_shooting_pct", sa.Numeric(), nullable=True),
        sa.Column("usage_pct", sa.Numeric(), nullable=True),
        sa.Column("pace", sa.Numeric(), nullable=True),
        sa.Column("pie", sa.Numeric(), nullable=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("ingested_at_utc", sa.TIMESTAMP(timezone=True), server_default=sa.text("NOW()")),
        sa.PrimaryKeyConstraint("game_id", "player_id"),
        sa.ForeignKeyConstraint(["game_id"], ["games.game_id"], ondelete="CASCADE"),
    )

    # Misc player stats
    op.create_table(
        "misc_player_stats",
        sa.Column("game_id", sa.Text(), nullable=False),
        sa.Column("player_id", sa.Text(), nullable=False),
        sa.Column("player_name", sa.Text(), nullable=False),
        sa.Column("team_id", sa.Text(), nullable=True),
        sa.Column("team_abbreviation", sa.Text(), nullable=False),
        sa.Column("plus_minus", sa.Numeric(), nullable=True),
        sa.Column("nba_fantasy_pts", sa.Numeric(), nullable=True),
        sa.Column("dd2", sa.Integer(), nullable=True),
        sa.Column("td3", sa.Integer(), nullable=True),
        sa.Column("fg_pct_rank", sa.Integer(), nullable=True),
        sa.Column("ft_pct_rank", sa.Integer(), nullable=True),
        sa.Column("fg3_pct_rank", sa.Integer(), nullable=True),
        sa.Column("pts_rank", sa.Integer(), nullable=True),
        sa.Column("reb_rank", sa.Integer(), nullable=True),
        sa.Column("ast_rank", sa.Integer(), nullable=True),
        sa.Column("wnba_fantasy_pts", sa.Numeric(), nullable=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("ingested_at_utc", sa.TIMESTAMP(timezone=True), server_default=sa.text("NOW()")),
        sa.PrimaryKeyConstraint("game_id", "player_id"),
        sa.ForeignKeyConstraint(["game_id"], ["games.game_id"], ondelete="CASCADE"),
    )

    # Usage player stats
    op.create_table(
        "usage_player_stats",
        sa.Column("game_id", sa.Text(), nullable=False),
        sa.Column("player_id", sa.Text(), nullable=False),
        sa.Column("player_name", sa.Text(), nullable=False),
        sa.Column("team_id", sa.Text(), nullable=True),
        sa.Column("team_abbreviation", sa.Text(), nullable=False),
        sa.Column("usage_pct", sa.Numeric(), nullable=True),
        sa.Column("pct_fgm", sa.Numeric(), nullable=True),
        sa.Column("pct_fga", sa.Numeric(), nullable=True),
        sa.Column("pct_fg3m", sa.Numeric(), nullable=True),
        sa.Column("pct_fg3a", sa.Numeric(), nullable=True),
        sa.Column("pct_ftm", sa.Numeric(), nullable=True),
        sa.Column("pct_fta", sa.Numeric(), nullable=True),
        sa.Column("pct_oreb", sa.Numeric(), nullable=True),
        sa.Column("pct_dreb", sa.Numeric(), nullable=True),
        sa.Column("pct_reb", sa.Numeric(), nullable=True),
        sa.Column("pct_ast", sa.Numeric(), nullable=True),
        sa.Column("pct_tov", sa.Numeric(), nullable=True),
        sa.Column("pct_stl", sa.Numeric(), nullable=True),
        sa.Column("pct_blk", sa.Numeric(), nullable=True),
        sa.Column("pct_blka", sa.Numeric(), nullable=True),
        sa.Column("pct_pf", sa.Numeric(), nullable=True),
        sa.Column("pct_pfd", sa.Numeric(), nullable=True),
        sa.Column("pct_pts", sa.Numeric(), nullable=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("ingested_at_utc", sa.TIMESTAMP(timezone=True), server_default=sa.text("NOW()")),
        sa.PrimaryKeyConstraint("game_id", "player_id"),
        sa.ForeignKeyConstraint(["game_id"], ["games.game_id"], ondelete="CASCADE"),
    )

    # Advanced team stats
    op.create_table(
        "advanced_team_stats",
        sa.Column("game_id", sa.Text(), nullable=False),
        sa.Column("team_id", sa.Text(), nullable=False),
        sa.Column("team_abbreviation", sa.Text(), nullable=False),
        sa.Column("team_name", sa.Text(), nullable=True),
        sa.Column("offensive_rating", sa.Numeric(), nullable=True),
        sa.Column("defensive_rating", sa.Numeric(), nullable=True),
        sa.Column("net_rating", sa.Numeric(), nullable=True),
        sa.Column("assist_percentage", sa.Numeric(), nullable=True),
        sa.Column("assist_to_turnover", sa.Numeric(), nullable=True),
        sa.Column("assist_ratio", sa.Numeric(), nullable=True),
        sa.Column("offensive_rebound_pct", sa.Numeric(), nullable=True),
        sa.Column("defensive_rebound_pct", sa.Numeric(), nullable=True),
        sa.Column("rebound_pct", sa.Numeric(), nullable=True),
        sa.Column("turnover_ratio", sa.Numeric(), nullable=True),
        sa.Column("effective_fg_pct", sa.Numeric(), nullable=True),
        sa.Column("true_shooting_pct", sa.Numeric(), nullable=True),
        sa.Column("pace", sa.Numeric(), nullable=True),
        sa.Column("pie", sa.Numeric(), nullable=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("ingested_at_utc", sa.TIMESTAMP(timezone=True), server_default=sa.text("NOW()")),
        sa.PrimaryKeyConstraint("game_id", "team_id"),
        sa.ForeignKeyConstraint(["game_id"], ["games.game_id"], ondelete="CASCADE"),
    )

    # Team game stats
    op.create_table(
        "team_game_stats",
        sa.Column("game_id", sa.Text(), nullable=False),
        sa.Column("team_id", sa.Text(), nullable=True),
        sa.Column("team_tricode", sa.Text(), nullable=False),
        sa.Column("points", sa.Integer(), server_default="0"),
        sa.Column("fgm", sa.Integer(), server_default="0"),
        sa.Column("fga", sa.Integer(), server_default="0"),
        sa.Column("fg_pct", sa.Numeric(), nullable=True),
        sa.Column("fg3m", sa.Integer(), server_default="0"),
        sa.Column("fg3a", sa.Integer(), server_default="0"),
        sa.Column("fg3_pct", sa.Numeric(), nullable=True),
        sa.Column("ftm", sa.Integer(), server_default="0"),
        sa.Column("fta", sa.Integer(), server_default="0"),
        sa.Column("ft_pct", sa.Numeric(), nullable=True),
        sa.Column("oreb", sa.Integer(), server_default="0"),
        sa.Column("dreb", sa.Integer(), server_default="0"),
        sa.Column("reb", sa.Integer(), server_default="0"),
        sa.Column("ast", sa.Integer(), server_default="0"),
        sa.Column("stl", sa.Integer(), server_default="0"),
        sa.Column("blk", sa.Integer(), server_default="0"),
        sa.Column("tov", sa.Integer(), server_default="0"),
        sa.Column("pf", sa.Integer(), server_default="0"),
        sa.Column("possessions_estimated", sa.Numeric(), nullable=True),
        sa.Column("pace", sa.Numeric(), nullable=True),
        sa.Column("offensive_rating", sa.Numeric(), nullable=True),
        sa.Column("defensive_rating", sa.Numeric(), nullable=True),
        sa.Column("net_rating", sa.Numeric(), nullable=True),
        sa.Column("effective_fg_pct", sa.Numeric(), nullable=True),
        sa.Column("true_shooting_pct", sa.Numeric(), nullable=True),
        sa.Column("efg_pct", sa.Numeric(), nullable=True),
        sa.Column("tov_rate", sa.Numeric(), nullable=True),
        sa.Column("orb_pct", sa.Numeric(), nullable=True),
        sa.Column("ft_rate", sa.Numeric(), nullable=True),
        sa.Column("pace_z_score", sa.Numeric(), nullable=True),
        sa.Column("offensive_efficiency_z", sa.Numeric(), nullable=True),
        sa.Column("defensive_efficiency_z", sa.Numeric(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("NOW()")),
        sa.PrimaryKeyConstraint("game_id", "team_tricode"),
        sa.ForeignKeyConstraint(["game_id"], ["games.game_id"], ondelete="CASCADE"),
    )

    # Player game stats
    op.create_table(
        "player_game_stats",
        sa.Column("game_id", sa.Text(), nullable=False),
        sa.Column("player_id", sa.Text(), nullable=False),
        sa.Column("player_name", sa.Text(), nullable=False),
        sa.Column("team_id", sa.Text(), nullable=True),
        sa.Column("team_abbreviation", sa.Text(), nullable=False),
        sa.Column("minutes_played", sa.Text(), nullable=True),
        sa.Column("points", sa.Integer(), server_default="0"),
        sa.Column("field_goals_made", sa.Integer(), server_default="0"),
        sa.Column("field_goals_attempted", sa.Integer(), server_default="0"),
        sa.Column("field_goal_percentage", sa.Numeric(), nullable=True),
        sa.Column("three_pointers_made", sa.Integer(), server_default="0"),
        sa.Column("three_pointers_attempted", sa.Integer(), server_default="0"),
        sa.Column("three_point_percentage", sa.Numeric(), nullable=True),
        sa.Column("free_throws_made", sa.Integer(), server_default="0"),
        sa.Column("free_throws_attempted", sa.Integer(), server_default="0"),
        sa.Column("free_throw_percentage", sa.Numeric(), nullable=True),
        sa.Column("offensive_rebounds", sa.Integer(), server_default="0"),
        sa.Column("defensive_rebounds", sa.Integer(), server_default="0"),
        sa.Column("total_rebounds", sa.Integer(), server_default="0"),
        sa.Column("assists", sa.Integer(), server_default="0"),
        sa.Column("steals", sa.Integer(), server_default="0"),
        sa.Column("blocks", sa.Integer(), server_default="0"),
        sa.Column("turnovers", sa.Integer(), server_default="0"),
        sa.Column("personal_fouls", sa.Integer(), server_default="0"),
        sa.Column("plus_minus", sa.Integer(), nullable=True),
        sa.Column("starter", sa.Boolean(), server_default="false"),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("ingested_at_utc", sa.TIMESTAMP(timezone=True), server_default=sa.text("NOW()")),
        sa.PrimaryKeyConstraint("game_id", "player_id"),
        sa.ForeignKeyConstraint(["game_id"], ["games.game_id"], ondelete="CASCADE"),
    )

    # Create all indexes
    _create_indexes()


def _create_indexes() -> None:
    """Create all performance indexes"""

    # Games indexes
    op.create_index("idx_games_date_local", "games", ["game_date_local"])
    op.create_index("idx_games_date_utc", "games", ["game_date_utc"])
    op.create_index("idx_games_season", "games", ["season"])
    op.create_index("idx_games_teams", "games", ["home_team_tricode", "away_team_tricode"])
    op.create_index("idx_games_status", "games", ["status"])
    op.create_index(
        "idx_games_season_date_teams",
        "games",
        ["season", "game_date_local", "home_team_tricode", "away_team_tricode"],
    )
    op.create_index("idx_games_date_status", "games", ["game_date_local", "status"])

    # Crosswalk indexes
    op.create_index("idx_crosswalk_bref_id", "game_id_crosswalk", ["bref_game_id"])
    op.create_index("idx_crosswalk_nba_stats_id", "game_id_crosswalk", ["nba_stats_game_id"])
    op.create_index(
        "idx_crosswalk_game_bref_composite", "game_id_crosswalk", ["game_id", "bref_game_id"]
    )

    # Referee indexes
    op.create_index("idx_ref_assignments_referee", "ref_assignments", ["referee_name_slug"])
    op.create_index("idx_ref_assignments_role", "ref_assignments", ["role"])
    op.create_index(
        "idx_ref_assignments_referee_role", "ref_assignments", ["referee_name_slug", "role"]
    )
    op.create_index("idx_ref_alternates_referee", "ref_alternates", ["referee_name_slug"])

    # Lineup indexes
    op.create_index("idx_lineups_team", "starting_lineups", ["team_tricode"])
    op.create_index("idx_lineups_player", "starting_lineups", ["player_name_slug"])
    op.create_index(
        "idx_lineups_team_player", "starting_lineups", ["team_tricode", "player_name_slug"]
    )
    op.create_index("idx_lineups_game_team", "starting_lineups", ["game_id", "team_tricode"])

    # Injury indexes
    op.create_index("idx_injury_status_team", "injury_status", ["team_tricode"])
    op.create_index("idx_injury_status_player", "injury_status", ["player_name_slug"])
    op.create_index("idx_injury_status_snapshot", "injury_status", ["snapshot_utc"])
    op.create_index("idx_injury_status_team_status", "injury_status", ["team_tricode", "status"])
    op.create_index("idx_injury_status_game_team", "injury_status", ["game_id", "team_tricode"])

    # PBP indexes
    op.create_index("idx_pbp_period_time", "pbp_events", ["game_id", "period", "seconds_elapsed"])
    op.create_index("idx_pbp_event_type", "pbp_events", ["event_type"])
    op.create_index("idx_pbp_team", "pbp_events", ["team_tricode"])
    op.create_index("idx_pbp_players", "pbp_events", ["player1_name_slug", "player2_name_slug"])


def downgrade() -> None:
    """Drop all tables and indexes"""

    # Drop tables in reverse order (respecting foreign keys)
    op.drop_table("player_game_stats")
    op.drop_table("team_game_stats")
    op.drop_table("advanced_team_stats")
    op.drop_table("usage_player_stats")
    op.drop_table("misc_player_stats")
    op.drop_table("advanced_player_stats")
    op.drop_table("pipeline_state")
    op.drop_table("outcomes")
    op.drop_table("schedule_travel")
    op.drop_table("early_shocks")
    op.drop_table("q1_window_12_8")
    op.drop_table("pbp_events")
    op.drop_table("injury_status")
    op.drop_table("starting_lineups")
    op.drop_table("ref_alternates")
    op.drop_table("ref_assignments")
    op.drop_table("game_id_crosswalk")
    op.drop_table("games")

    # Drop extension
    op.execute('DROP EXTENSION IF EXISTS "uuid-ossp"')
