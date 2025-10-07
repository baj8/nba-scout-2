"""Game row Pydantic model with validators."""

import logging
import re
from datetime import UTC, date
from datetime import datetime as dt
from typing import Any, ClassVar, Dict, Optional
from zoneinfo import ZoneInfo

import yaml
from dateutil import tz
from pydantic import BaseModel, Field, ValidationInfo, field_validator, model_validator

from ..config import get_project_root
from .enums import GameStatus

logger = logging.getLogger(__name__)


class GameRow(BaseModel):
    """Game row model with validation and normalization."""

    game_id: str = Field(..., description="Unique game identifier")
    bref_game_id: Optional[str] = Field(None, description="Basketball Reference game ID")
    season: str = Field(..., description="Season (e.g., '2023-24')")
    game_date_utc: dt = Field(..., description="Game date/time in UTC")
    game_date_local: date = Field(..., description="Game date in local arena timezone")
    arena_tz: str = Field(..., description="Arena timezone (IANA)")
    home_team_tricode: str = Field(..., description="Home team tricode")
    away_team_tricode: str = Field(..., description="Away team tricode")
    home_team_id: Optional[str] = Field(None, description="Home team ID")
    away_team_id: Optional[str] = Field(None, description="Away team ID")
    odds_join_key: Optional[str] = Field(None, description="Key for joining odds data")
    status: GameStatus = Field(default=GameStatus.SCHEDULED, description="Game status")
    period: int = Field(default=0, description="Current period")
    time_remaining: Optional[str] = Field(None, description="Time remaining in period")
    arena_name: Optional[str] = Field(None, description="Arena name")
    attendance: Optional[int] = Field(None, description="Game attendance")
    source: str = Field(..., description="Data source")
    source_url: str = Field(..., description="Source URL")

    # Team aliases cache - using ClassVar to avoid Pydantic treating it as a field
    _team_aliases: ClassVar[Optional[Dict[str, Any]]] = None

    @model_validator(mode="before")
    @classmethod
    def preserve_original_local_date(cls, data: Any) -> Any:
        """Store the original game_date_local before any processing."""
        if isinstance(data, dict) and "game_date_local" in data:
            # Store the original local date in a special field
            data["_original_game_date_local"] = data["game_date_local"]
        return data

    @model_validator(mode="before")
    @classmethod
    def preprocess_data(cls, data: Any) -> Any:
        """Apply comprehensive preprocessing before field validation to prevent int/str comparison errors."""
        if isinstance(data, dict):
            # Import here to avoid circular imports
            from .utils import preprocess_nba_stats_data

            # Apply NBA Stats preprocessing to handle mixed data types
            processed_data = preprocess_nba_stats_data(data)

            return processed_data
        return data

    @classmethod
    def _load_team_aliases(cls) -> Dict[str, Any]:
        """Load team aliases from YAML file."""
        if cls._team_aliases is None:
            aliases_path = get_project_root() / "team_aliases.yaml"
            if aliases_path.exists():
                with open(aliases_path, "r") as f:
                    cls._team_aliases = yaml.safe_load(f)
            else:
                cls._team_aliases = {"teams": {}}
        return cls._team_aliases

    @classmethod
    def _normalize_tricode(cls, tricode: str) -> str:
        """Normalize team tricode using aliases."""
        if not tricode:
            return tricode

        tricode_upper = tricode.upper()
        aliases = cls._load_team_aliases()

        # Check if it's already canonical
        if tricode_upper in aliases.get("teams", {}):
            return tricode_upper

        # Search through all team data for matches
        for canonical, team_data in aliases.get("teams", {}).items():
            # Check NBA Stats variations
            if tricode_upper in [t.upper() for t in team_data.get("nba_stats", [])]:
                return canonical
            # Check Basketball Reference variations
            if tricode_upper in [t.upper() for t in team_data.get("bref", [])]:
                return canonical
            # Check aliases
            if tricode_upper in [t.upper() for t in team_data.get("aliases", [])]:
                return canonical

        # If not found, return as-is but log warning
        return tricode_upper

    @classmethod
    def _normalize_status(cls, status: str) -> str:
        """Normalize game status from various sources to our enum values."""
        if not status:
            return "SCHEDULED"

        status_upper = status.upper().strip()

        # Handle common variations
        status_map = {
            "FINAL": "FINAL",
            "FINISHED": "FINAL",
            "COMPLETED": "FINAL",
            "LIVE": "LIVE",  # Keep LIVE as LIVE, don't convert to IN_PROGRESS
            "IN_PROGRESS": "LIVE",  # Convert IN_PROGRESS to LIVE for consistency
            "IN PROGRESS": "LIVE",
            "SCHEDULED": "SCHEDULED",
            "UPCOMING": "SCHEDULED",
            "POSTPONED": "POSTPONED",
            "CANCELLED": "CANCELLED",
            "CANCELED": "CANCELLED",
            "SUSPENDED": "SUSPENDED",
            "RESCHEDULED": "RESCHEDULED",
        }

        return status_map.get(status_upper, "SCHEDULED")

    @field_validator("home_team_tricode", "away_team_tricode", mode="before")
    @classmethod
    def validate_tricode(cls, v) -> str:
        """Validate and normalize team tricode, handling both strings and integer team IDs."""
        # CRITICAL: Handle the case where NBA Stats API sends integer team IDs instead of tricodes
        if isinstance(v, int):
            # Convert integer team ID to tricode
            return cls._team_id_to_tricode(str(v))
        elif v is None:
            return "UNK"
        else:
            # Convert to string and normalize
            return cls._normalize_tricode(str(v))

    @field_validator("arena_tz")
    @classmethod
    def validate_timezone(cls, v: str) -> str:
        """Validate IANA timezone string."""
        try:
            tz.gettz(v)
            return v
        except Exception:
            raise ValueError(f"Invalid timezone: {v}")

    @field_validator("season")
    @classmethod
    def validate_season(cls, v: str) -> str:
        """Validate season format (e.g., '2023-24')."""
        if not re.match(r"^\d{4}-\d{2}$", v):
            raise ValueError(f"Invalid season format: {v}. Expected format: '2023-24'")
        return v

    @field_validator("status", mode="before")
    @classmethod
    def validate_status(cls, v) -> GameStatus:
        """Validate and normalize game status."""
        if isinstance(v, GameStatus):
            return v

        # CRITICAL: Convert any input to string first to prevent int/str comparison errors
        if v is None:
            status_str = "SCHEDULED"
        else:
            # Ensure we always work with a string, regardless of input type
            status_str = str(v).strip()

        normalized_status = cls._normalize_status(status_str)

        try:
            return GameStatus(normalized_status)
        except ValueError:
            # If the normalized status is not a valid enum value, return default
            return GameStatus.SCHEDULED

    @model_validator(mode="after")
    def validate_game_data(self, info: ValidationInfo) -> "GameRow":
        """Validate game data consistency."""
        # Generate odds join key if not provided
        if not self.odds_join_key and self.game_date_local:
            # Format: YYYYMMDD (arena local date)
            self.odds_join_key = self.game_date_local.strftime("%Y%m%d")

        # Detect whether local date was explicitly provided at construction
        try:
            fields_set = info.fields_set  # Pydantic v2
        except Exception:
            # Fallback to model_fields_set for Pydantic v2 compatibility
            fields_set = getattr(self, "model_fields_set", set())
        local_was_provided = "game_date_local" in (fields_set or set())

        # If we can derive a date from UTC + arena_tz, do it (for comparison or fill-in)
        derived_local_date = None
        if self.game_date_utc and self.arena_tz:
            try:
                tz_obj = ZoneInfo(self.arena_tz)  # zoneinfo supports aliases like "US/Pacific"
            except Exception:
                logger.warning("game_row.tz_unrecognized", extra={"arena_tz": self.arena_tz})
                tz_obj = None
            if tz_obj is not None:
                derived_local_date = self.game_date_utc.astimezone(tz_obj).date()

        if local_was_provided and self.game_date_local is not None:
            # Preserve explicitly provided local date; only warn/error on mismatch.
            if derived_local_date is not None:
                delta = (self.game_date_local - derived_local_date).days
                if abs(delta) > 1:
                    logger.error(
                        "game_row.local_vs_derived_date_mismatch_hard",
                        extra={
                            "provided_local": self.game_date_local.isoformat(),
                            "derived_local": derived_local_date.isoformat(),
                            "arena_tz": self.arena_tz,
                        },
                    )
                    raise ValueError(
                        f"game_date_local {self.game_date_local} differs from derived {derived_local_date} by >1 day"
                    )
                if delta != 0:
                    logger.warning(
                        "game_row.local_vs_derived_date_mismatch_minor",
                        extra={
                            "provided_local": self.game_date_local.isoformat(),
                            "derived_local": derived_local_date.isoformat(),
                            "arena_tz": self.arena_tz,
                        },
                    )
            # do NOT overwrite self.game_date_local
            return self

        # If not provided, and we have a derived value, fill it
        if self.game_date_local is None and derived_local_date is not None:
            object.__setattr__(self, "game_date_local", derived_local_date)
            return self

        # Else leave as-is (could be None; upstream bridge will handle resolution)
        return self

    @classmethod
    def from_nba_stats(cls, data: Dict[str, Any], source_url: str) -> "GameRow":
        """Create GameRow from NBA Stats API data."""
        # Preprocess data to handle mixed data types before validation
        from .utils import preprocess_nba_stats_data

        data = preprocess_nba_stats_data(data)

        # Parse NBA Stats scoreboard format
        game_date_est = data.get("GAME_DATE_EST", "")
        matchup = data.get("MATCHUP", "")

        # Convert season format from '2023' to '2023-24'
        raw_season = data.get("SEASON", "")
        if raw_season and len(raw_season) == 4 and raw_season.isdigit():
            # Convert from '2023' to '2023-24'
            season_year = int(raw_season)
            next_year = (season_year + 1) % 100  # Get last 2 digits of next year
            season = f"{raw_season}-{next_year:02d}"
        else:
            # Use as-is if already in correct format or empty
            season = raw_season

        # Extract team tricodes - try multiple field names used by NBA Stats API
        home_team = ""
        away_team = ""

        # Method 1: Try direct team abbreviation fields first (most reliable)
        home_team = (
            data.get("HOME_TEAM_ABBREVIATION")
            or data.get("HOME_TEAM_TRICODE")
            or data.get("homeTeam", {}).get("teamTricode", "")
            if isinstance(data.get("homeTeam"), dict)
            else ""
        )

        away_team = (
            data.get("VISITOR_TEAM_ABBREVIATION")
            or data.get("AWAY_TEAM_ABBREVIATION")
            or data.get("VISITOR_TEAM_TRICODE")
            or data.get("AWAY_TEAM_TRICODE")
            or data.get("awayTeam", {}).get("teamTricode", "")
            if isinstance(data.get("awayTeam"), dict)
            else ""
        )

        # Method 2: Parse from MATCHUP field if direct fields are empty (e.g., "BOS @ LAL")
        if not home_team or not away_team:
            if matchup and " @ " in matchup:
                away_parsed, home_parsed = matchup.split(" @ ", 1)
                if not home_team:
                    home_team = home_parsed.strip()
                if not away_team:
                    away_team = away_parsed.strip()
            elif matchup and " vs. " in matchup:
                home_parsed, away_parsed = matchup.split(" vs. ", 1)
                if not home_team:
                    home_team = home_parsed.strip()
                if not away_team:
                    away_team = away_parsed.strip()
            elif matchup and " v " in matchup:
                home_parsed, away_parsed = matchup.split(" v ", 1)
                if not home_team:
                    home_team = home_parsed.strip()
                if not away_team:
                    away_team = away_parsed.strip()

        # Method 3: Try nested team objects
        if not home_team:
            home_obj = data.get("home") or data.get("homeTeam")
            if isinstance(home_obj, dict):
                home_team = (
                    home_obj.get("abbreviation")
                    or home_obj.get("tricode")
                    or home_obj.get("teamTricode")
                    or ""
                )

        if not away_team:
            away_obj = data.get("away") or data.get("awayTeam") or data.get("visitor")
            if isinstance(away_obj, dict):
                away_team = (
                    away_obj.get("abbreviation")
                    or away_obj.get("tricode")
                    or away_obj.get("teamTricode")
                    or ""
                )

        # Method 4: Try to map team IDs to tricodes if we still don't have them
        if not home_team:
            home_team_id = data.get("HOME_TEAM_ID")
            if home_team_id:
                home_team = cls._team_id_to_tricode(str(home_team_id))

        if not away_team:
            away_team_id = data.get("VISITOR_TEAM_ID") or data.get("AWAY_TEAM_ID")
            if away_team_id:
                away_team = cls._team_id_to_tricode(str(away_team_id))

        # Ensure we have valid tricodes (log warning if empty)
        if not home_team or not away_team:
            from ..nba_logging import get_logger

            logger = get_logger(__name__)
            logger.warning(
                "Missing team tricodes in NBA Stats data",
                game_id=data.get("GAME_ID"),
                home_team=home_team,
                away_team=away_team,
                matchup=matchup,
                available_fields=list(data.keys()),
            )

            # Set fallback values to prevent empty strings
            if not home_team:
                home_team = "UNK"
            if not away_team:
                away_team = "UNK"

        # Parse game date and determine timezone
        if game_date_est:
            try:
                game_date_utc = dt.fromisoformat(game_date_est.replace("Z", "+00:00"))
            except ValueError:
                # Try alternative date parsing
                from dateutil.parser import parse

                game_date_utc = parse(game_date_est)
        else:
            # Fallback to current time if no date provided
            game_date_utc = dt.now(UTC)

        # Get arena timezone
        arena_tz = cls._get_arena_timezone(home_team) if home_team else "America/New_York"

        # Convert to local date
        local_tz = tz.gettz(arena_tz)
        local_dt = game_date_utc.astimezone(local_tz)

        # Safely convert period to int
        period_raw = data.get("PERIOD", 0)
        try:
            period = int(period_raw) if period_raw is not None else 0
        except (ValueError, TypeError):
            period = 0

        # Safely handle status field - ensure it's always a string
        status_raw = data.get("GAME_STATUS_TEXT")
        if status_raw is None:
            status_value = "SCHEDULED"
        else:
            status_value = str(status_raw).strip() if str(status_raw).strip() else "SCHEDULED"

        return cls(
            game_id=str(data.get("GAME_ID", "")),
            season=season,
            game_date_utc=game_date_utc,
            game_date_local=local_dt.date(),
            arena_tz=arena_tz,
            home_team_tricode=home_team,
            away_team_tricode=away_team,
            home_team_id=str(data.get("HOME_TEAM_ID", "")),
            away_team_id=str(data.get("VISITOR_TEAM_ID", "") or data.get("AWAY_TEAM_ID", "")),
            status=status_value,  # Always pass a clean string value
            period=period,
            time_remaining=data.get("TIME", ""),
            source="nba_stats",
            source_url=source_url,
        )

    @classmethod
    def from_bref(cls, data: Dict[str, Any], source_url: str) -> "GameRow":
        """Create GameRow from Basketball Reference data."""
        # This would be implemented based on B-Ref HTML parsing
        # For now, return a basic structure
        return cls(
            game_id=data.get("game_id", ""),
            season=data.get("season", ""),
            game_date_utc=data.get("game_date_utc"),
            game_date_local=data.get("game_date_local"),
            arena_tz=data.get("arena_tz", "America/New_York"),
            home_team_tricode=data.get("home_team", ""),
            away_team_tricode=data.get("away_team", ""),
            bref_game_id=data.get("bref_game_id"),
            source="bref",
            source_url=source_url,
        )

    @classmethod
    def _get_arena_timezone(cls, team_tricode: str) -> str:
        """Get arena timezone for team (lookup from venues.csv)."""
        # This would lookup from venues.csv
        # For now, return default mappings
        tz_map = {
            "BOS": "America/New_York",
            "NYK": "America/New_York",
            "BRK": "America/New_York",
            "PHI": "America/New_York",
            "TOR": "America/Toronto",
            "CHI": "America/Chicago",
            "MIL": "America/Chicago",
            "IND": "America/New_York",
            "CLE": "America/New_York",
            "DET": "America/Detroit",
            "ATL": "America/New_York",
            "MIA": "America/New_York",
            "CHA": "America/New_York",
            "ORL": "America/New_York",
            "WAS": "America/New_York",
            "DAL": "America/Chicago",
            "SAS": "America/Chicago",
            "HOU": "America/Chicago",
            "MEM": "America/Chicago",
            "NOP": "America/Chicago",
            "MIN": "America/Chicago",
            "OKC": "America/Chicago",
            "DEN": "America/Denver",
            "UTA": "America/Denver",
            "POR": "America/Los_Angeles",
            "GSW": "America/Los_Angeles",
            "LAC": "America/Los_Angeles",
            "LAL": "America/Los_Angeles",
            "SAC": "America/Los_Angeles",
            "PHX": "America/Phoenix",
        }
        return tz_map.get(cls._normalize_tricode(team_tricode), "America/New_York")

    @classmethod
    def _team_id_to_tricode(cls, team_id: str) -> str:
        """Map NBA team ID to tricode."""
        # Common NBA team ID to tricode mappings
        id_map = {
            "1610612737": "ATL",
            "1610612738": "BOS",
            "1610612751": "BRK",
            "1610612766": "CHA",
            "1610612741": "CHI",
            "1610612739": "CLE",
            "1610612742": "DAL",
            "1610612743": "DEN",
            "1610612765": "DET",
            "1610612744": "GSW",
            "1610612745": "HOU",
            "1610612754": "IND",
            "1610612746": "LAC",
            "1610612747": "LAL",
            "1610612763": "MEM",
            "1610612748": "MIA",
            "1610612749": "MIL",
            "1610612750": "MIN",
            "1610612740": "NOP",
            "1610612752": "NYK",
            "1610612760": "OKC",
            "1610612753": "ORL",
            "1610612755": "PHI",
            "1610612756": "PHX",
            "1610612757": "POR",
            "1610612758": "SAC",
            "1610612759": "SAS",
            "1610612761": "TOR",
            "1610612762": "UTA",
            "1610612764": "WAS",
        }
        return id_map.get(team_id, "UNK")
