"""Schedule travel analytics transformer with circadian and altitude metrics."""

import csv
import math
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

from ..models.game_rows import GameRow
from ..models.derived_rows import ScheduleTravelRow
from ..logging import get_logger

logger = get_logger(__name__)


@dataclass
class VenueData:
    """Venue information for travel calculations."""
    team_id: str
    arena_name: str
    arena_tz: str
    lat: float
    lon: float
    altitude_m: float


@dataclass
class TravelLeg:
    """Represents a travel leg between venues."""
    from_venue: VenueData
    to_venue: VenueData
    distance_km: float
    timezone_shift_hours: float
    altitude_change_m: float
    
    @property
    def is_eastward(self) -> bool:
        """Check if travel is eastward (harder on circadian rhythm)."""
        return self.timezone_shift_hours > 0
    
    @property
    def is_westward(self) -> bool:
        """Check if travel is westward (easier on circadian rhythm)."""
        return self.timezone_shift_hours < 0


class ScheduleTravelTransformer:
    """Transformer for schedule difficulty and travel impact analytics."""
    
    def __init__(self, 
                 source: str = "schedule_travel",
                 venues_csv_path: Optional[Path] = None):
        """Initialize transformer with venue data.
        
        Args:
            source: Data source identifier
            venues_csv_path: Path to venues.csv file (defaults to workspace root)
        """
        self.source = source
        
        # Default to venues.csv in workspace root
        if venues_csv_path is None:
            venues_csv_path = Path(__file__).parent.parent.parent.parent / "venues.csv"
        
        self.venues = self._load_venues_data(venues_csv_path)
        logger.info("Loaded venue data", venue_count=len(self.venues))
    
    def transform(self, games: List[GameRow], source_url: str) -> List[ScheduleTravelRow]:
        """Transform game schedule into travel analytics.
        
        Args:
            games: List of games sorted by date for each team
            source_url: Source URL for provenance
            
        Returns:
            List of ScheduleTravelRow instances
        """
        if not games:
            return []
        
        # Group games by team for schedule analysis
        team_schedules = self._group_games_by_team(games)
        
        travel_rows = []
        
        for team_tricode, team_games in team_schedules.items():
            # Sort games chronologically for each team
            team_games.sort(key=lambda g: g.game_date_utc)
            
            for i, game in enumerate(team_games):
                travel_row = self._analyze_game_travel(
                    game, team_games[:i], team_tricode, source_url
                )
                if travel_row:
                    travel_rows.append(travel_row)
        
        logger.info("Generated travel analytics", 
                   total_games=len(games),
                   travel_analytics=len(travel_rows))
        
        return travel_rows
    
    def _load_venues_data(self, venues_csv_path: Path) -> Dict[str, VenueData]:
        """Load venue data from CSV file."""
        venues = {}
        
        try:
            with open(venues_csv_path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    venue = VenueData(
                        team_id=row['team_id'],
                        arena_name=row['arena_name'],
                        arena_tz=row['arena_tz'],
                        lat=float(row['lat']),
                        lon=float(row['lon']),
                        altitude_m=float(row['altitude_m'])
                    )
                    venues[venue.team_id] = venue
            
            logger.info("Successfully loaded venues data", count=len(venues))
            
        except Exception as e:
            logger.error("Failed to load venues data", 
                        path=str(venues_csv_path), error=str(e))
            raise
        
        return venues
    
    def _group_games_by_team(self, games: List[GameRow]) -> Dict[str, List[GameRow]]:
        """Group games by team (both home and away)."""
        team_schedules = {}
        
        for game in games:
            # Add game to home team's schedule
            if game.home_team_tricode not in team_schedules:
                team_schedules[game.home_team_tricode] = []
            team_schedules[game.home_team_tricode].append(game)
            
            # Add game to away team's schedule
            if game.away_team_tricode not in team_schedules:
                team_schedules[game.away_team_tricode] = []
            team_schedules[game.away_team_tricode].append(game)
        
        return team_schedules
    
    def _analyze_game_travel(self, 
                           current_game: GameRow,
                           previous_games: List[GameRow],
                           team_tricode: str,
                           source_url: str) -> Optional[ScheduleTravelRow]:
        """Analyze travel impact for a specific game."""
        
        if not previous_games:
            # First game of the season - no travel analysis
            return None
        
        # Find the most recent previous game
        prev_game = previous_games[-1]
        
        # Calculate days rest
        days_rest = (current_game.game_date_utc.date() - prev_game.game_date_utc.date()).days - 1
        
        # Determine current and previous venues
        current_venue = self._get_team_venue(current_game, team_tricode)
        prev_venue = self._get_team_venue(prev_game, team_tricode)
        
        if not current_venue or not prev_venue:
            logger.warning("Missing venue data", 
                          game_id=current_game.game_id,
                          team=team_tricode)
            return None
        
        # Calculate travel metrics
        travel_leg = self._calculate_travel_leg(prev_venue, current_venue)
        
        # Analyze schedule difficulty
        schedule_patterns = self._analyze_schedule_patterns(
            current_game, previous_games, team_tricode
        )
        
        # Calculate circadian impact
        circadian_index = self._calculate_circadian_index(
            travel_leg, days_rest, current_game.game_date_utc
        )
        
        return ScheduleTravelRow(
            game_id=current_game.game_id,
            team_tricode=team_tricode,
            is_back_to_back=schedule_patterns['is_back_to_back'],
            is_3_in_4=schedule_patterns['is_3_in_4'],
            is_5_in_7=schedule_patterns['is_5_in_7'],
            days_rest=days_rest,
            timezone_shift_hours=travel_leg.timezone_shift_hours,
            circadian_index=circadian_index,
            altitude_change_m=travel_leg.altitude_change_m,
            travel_distance_km=travel_leg.distance_km,
            prev_game_date=prev_game.game_date_utc.date(),
            prev_arena_tz=prev_venue.arena_tz,
            prev_lat=prev_venue.lat,
            prev_lon=prev_venue.lon,
            prev_altitude_m=prev_venue.altitude_m,
            source=self.source,
            source_url=source_url
        )
    
    def _get_team_venue(self, game: GameRow, team_tricode: str) -> Optional[VenueData]:
        """Get the venue for a team's game (home venue if home, away venue if away)."""
        if team_tricode == game.home_team_tricode:
            return self.venues.get(game.home_team_tricode)
        elif team_tricode == game.away_team_tricode:
            return self.venues.get(game.home_team_tricode)  # Away team plays at home team's venue
        return None
    
    def _calculate_travel_leg(self, from_venue: VenueData, to_venue: VenueData) -> TravelLeg:
        """Calculate travel metrics between two venues."""
        
        # Calculate haversine distance
        distance_km = self._haversine_distance(
            from_venue.lat, from_venue.lon,
            to_venue.lat, to_venue.lon
        )
        
        # Calculate timezone shift
        timezone_shift = self._calculate_timezone_shift(
            from_venue.arena_tz, to_venue.arena_tz
        )
        
        # Calculate altitude change
        altitude_change = to_venue.altitude_m - from_venue.altitude_m
        
        return TravelLeg(
            from_venue=from_venue,
            to_venue=to_venue,
            distance_km=distance_km,
            timezone_shift_hours=timezone_shift,
            altitude_change_m=altitude_change
        )
    
    def _haversine_distance(self, lat1: float, lon1: float, 
                           lat2: float, lon2: float) -> float:
        """Calculate great circle distance between two points using haversine formula."""
        
        if lat1 == lat2 and lon1 == lon2:
            return 0.0
        
        # Convert to radians
        lat1_rad = math.radians(lat1)
        lon1_rad = math.radians(lon1)
        lat2_rad = math.radians(lat2) 
        lon2_rad = math.radians(lon2)
        
        # Haversine formula
        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad
        
        a = (math.sin(dlat/2) ** 2 + 
             math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2) ** 2)
        c = 2 * math.asin(math.sqrt(a))
        
        # Earth radius in kilometers
        earth_radius_km = 6371.0
        
        return earth_radius_km * c
    
    def _calculate_timezone_shift(self, from_tz: str, to_tz: str) -> float:
        """Calculate timezone shift in hours (positive = eastward)."""
        
        # Simplified timezone mapping (hours from UTC)
        tz_offsets = {
            'America/Los_Angeles': -8,  # PST
            'America/Denver': -7,       # MST
            'America/Phoenix': -7,      # MST (no DST)
            'America/Chicago': -6,      # CST
            'America/Detroit': -5,      # EST
            'America/New_York': -5,     # EST
            'America/Toronto': -5,      # EST
        }
        
        from_offset = tz_offsets.get(from_tz, 0)
        to_offset = tz_offsets.get(to_tz, 0)
        
        # Positive = eastward travel (harder on body clock)
        return to_offset - from_offset
    
    def _analyze_schedule_patterns(self, 
                                 current_game: GameRow,
                                 previous_games: List[GameRow],
                                 team_tricode: str) -> Dict[str, bool]:
        """Analyze back-to-back and compressed schedule patterns."""
        
        current_date = current_game.game_date_utc.date()
        
        # Check for back-to-back (game yesterday)
        is_back_to_back = False
        if previous_games:
            prev_date = previous_games[-1].game_date_utc.date()
            is_back_to_back = (current_date - prev_date).days == 1
        
        # Check for 3-in-4 (3 games in 4 days)
        is_3_in_4 = False
        if len(previous_games) >= 2:
            game_dates = [g.game_date_utc.date() for g in previous_games[-2:]] + [current_date]
            date_span = (max(game_dates) - min(game_dates)).days + 1
            is_3_in_4 = date_span <= 4
        
        # Check for 5-in-7 (5 games in 7 days)
        is_5_in_7 = False
        if len(previous_games) >= 4:
            game_dates = [g.game_date_utc.date() for g in previous_games[-4:]] + [current_date]
            date_span = (max(game_dates) - min(game_dates)).days + 1
            is_5_in_7 = date_span <= 7
        
        return {
            'is_back_to_back': is_back_to_back,
            'is_3_in_4': is_3_in_4,
            'is_5_in_7': is_5_in_7
        }
    
    def _calculate_circadian_index(self, 
                                 travel_leg: TravelLeg,
                                 days_rest: int,
                                 game_datetime: datetime) -> float:
        """Calculate composite circadian disruption index (0.0 = no impact, 1.0 = maximum impact)."""
        
        base_disruption = 0.0
        
        # Timezone shift impact (eastward travel is worse)
        tz_shift_abs = abs(travel_leg.timezone_shift_hours)
        if tz_shift_abs > 0:
            tz_impact = min(tz_shift_abs / 3.0, 1.0)  # 3+ hour shifts = maximum impact
            
            # Eastward travel is 1.5x worse than westward
            if travel_leg.is_eastward:
                tz_impact *= 1.5
            
            base_disruption += tz_impact
        
        # Distance fatigue (long flights)
        if travel_leg.distance_km > 1000:  # Long-haul threshold
            distance_impact = min(travel_leg.distance_km / 5000.0, 0.3)  # Max 0.3 from distance
            base_disruption += distance_impact
        
        # Recovery time adjustment (more rest = less impact)
        if days_rest == 0:
            rest_multiplier = 1.5  # Back-to-back amplifies fatigue
        elif days_rest == 1:
            rest_multiplier = 1.0  # Normal impact
        elif days_rest >= 3:
            rest_multiplier = 0.5  # Good rest reduces impact
        else:
            rest_multiplier = 0.8  # Some rest helps
        
        base_disruption *= rest_multiplier
        
        # Game time adjustment (late games after eastward travel are worse)
        game_hour = game_datetime.hour
        if travel_leg.is_eastward and game_hour >= 22:  # 10 PM or later
            base_disruption *= 1.2
        
        # Altitude adjustment (going to high altitude is disruptive)
        if travel_leg.altitude_change_m > 1000:  # Significant altitude gain
            altitude_impact = min(travel_leg.altitude_change_m / 2000.0, 0.2)  # Max 0.2 from altitude
            base_disruption += altitude_impact
        
        # Cap at 1.0 and ensure non-negative
        return max(0.0, min(base_disruption, 1.0))