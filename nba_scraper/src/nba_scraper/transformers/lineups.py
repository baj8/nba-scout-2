"""Lineup transformation functions - pure, synchronous."""

from typing import Any, Dict, List

from ..models.lineups import LineupStint
from ..utils.coerce import to_int_or_none
from ..utils.preprocess import normalize_team_id, preprocess_nba_stats_data


def transform_lineups(raw: List[Dict[str, Any]], game_id: str) -> List[LineupStint]:
    """Transform extracted lineup data to validated LineupStint models.

    Args:
        raw: List of dictionaries from extract_lineups_from_response
        game_id: Game identifier for linking

    Returns:
        List of validated LineupStint instances
    """
    out = []

    for r in raw:
        # Apply preprocessing to handle mixed data types
        r = preprocess_nba_stats_data(r)

        try:
            # Extract team ID
            team_id = normalize_team_id(r.get("TEAM_ID"))
            if team_id is None:
                continue  # Skip if no valid team ID

            # Extract period using robust coercion
            period = to_int_or_none(r.get("PERIOD")) or 1

            # Extract player IDs - handle various formats
            player_ids_raw = r.get("PLAYER_IDS") or r.get("LINEUP") or []

            # Handle different player ID formats
            if isinstance(player_ids_raw, str):
                # Comma-separated string
                player_ids = []
                for pid_str in player_ids_raw.split(","):
                    pid = to_int_or_none(pid_str.strip())
                    if pid is not None:
                        player_ids.append(pid)
            elif isinstance(player_ids_raw, list):
                # Already a list
                player_ids = []
                for pid in player_ids_raw:
                    converted_pid = to_int_or_none(pid)
                    if converted_pid is not None:
                        player_ids.append(converted_pid)
            else:
                # Try individual player fields (PLAYER1_ID, PLAYER2_ID, etc.)
                player_ids = []
                for i in range(1, 6):  # NBA lineups have 5 players
                    pid = to_int_or_none(r.get(f"PLAYER{i}_ID"))
                    if pid is not None:
                        player_ids.append(pid)

            # Validate lineup size
            if len(player_ids) != 5:
                continue  # Skip invalid lineups

            # Extract seconds played using robust coercion
            seconds_played = (
                to_int_or_none(r.get("SECS")) or to_int_or_none(r.get("SECONDS_PLAYED")) or 0
            )

            lineup_stint = LineupStint(
                game_id=game_id,
                team_id=team_id,
                period=period,
                lineup=player_ids,
                seconds_played=seconds_played,
            )
            out.append(lineup_stint)

        except Exception:
            # Skip invalid lineup records
            continue

    return out
