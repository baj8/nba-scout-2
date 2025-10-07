"""Basketball Reference extraction functions."""

from typing import Any, Dict, List

from ..models import InjuryStatusRow, OutcomesRow, StartingLineupRow
from ..nba_logging import get_logger
from ..utils.coerce import to_int_or_none

logger = get_logger(__name__)


def extract_game_outcomes(
    html_content: str, game_id: str, home_team: str, away_team: str, source_url: str
) -> OutcomesRow:
    """Extract final scores and outcomes from B-Ref boxscore HTML.

    Args:
        html_content: Raw HTML content
        game_id: Game identifier
        home_team: Home team tricode
        away_team: Away team tricode
        source_url: Source URL for provenance

    Returns:
        Validated OutcomesRow instance
    """
    try:
        # Use the BRefClient parsing method we already implemented
        from ..io_clients.bref import BRefClient

        client = BRefClient()
        scores_data = client.parse_boxscore_scores(html_content)

        if not scores_data:
            logger.warning("No scores found in B-Ref HTML", game_id=game_id)
            # Return minimal outcomes row with defaults
            return OutcomesRow(
                game_id=game_id,
                home_team_tricode=home_team,
                away_team_tricode=away_team,
                final_home_points=0,
                final_away_points=0,
                total_points=0,
                home_win=False,
                margin=0,
                source="bref",
                source_url=source_url,
            )

        # Create OutcomesRow using the from_box_score class method
        outcomes = OutcomesRow.from_box_score(
            game_id=game_id,
            home_team=home_team,
            away_team=away_team,
            box_data=scores_data,
            source="bref",
            source_url=source_url,
        )

        logger.debug(
            "Extracted game outcomes",
            game_id=game_id,
            home_points=outcomes.final_home_points,
            away_points=outcomes.final_away_points,
        )

        return outcomes

    except Exception as e:
        logger.error("Failed to extract game outcomes", game_id=game_id, error=str(e))
        # Return minimal outcomes row
        return OutcomesRow(
            game_id=game_id,
            home_team_tricode=home_team,
            away_team_tricode=away_team,
            final_home_points=0,
            final_away_points=0,
            total_points=0,
            home_win=False,
            margin=0,
            source="bref_error",
            source_url=source_url,
        )


def extract_starting_lineups(
    html_content: str, game_id: str, home_team: str, away_team: str, source_url: str
) -> List[StartingLineupRow]:
    """Extract starting lineups from B-Ref boxscore HTML.

    Args:
        html_content: Raw HTML content
        game_id: Game identifier
        home_team: Home team tricode
        away_team: Away team tricode
        source_url: Source URL for provenance

    Returns:
        List of validated StartingLineupRow instances
    """
    lineups = []

    try:
        # Use the BRefClient parsing method
        from ..io_clients.bref import BRefClient

        client = BRefClient()
        lineup_data = client.parse_starting_lineups(html_content)

        # Process home team lineups
        home_players = lineup_data.get("home", [])
        for player_data in home_players:
            try:
                lineup_row = StartingLineupRow.from_bref(
                    game_id=game_id,
                    team_tricode=home_team,
                    player_data=player_data,
                    source_url=source_url,
                )
                lineups.append(lineup_row)
            except Exception as e:
                logger.warning(
                    "Failed to create home lineup row",
                    game_id=game_id,
                    player=player_data.get("player"),
                    error=str(e),
                )

        # Process away team lineups
        away_players = lineup_data.get("away", [])
        for player_data in away_players:
            try:
                lineup_row = StartingLineupRow.from_bref(
                    game_id=game_id,
                    team_tricode=away_team,
                    player_data=player_data,
                    source_url=source_url,
                )
                lineups.append(lineup_row)
            except Exception as e:
                logger.warning(
                    "Failed to create away lineup row",
                    game_id=game_id,
                    player=player_data.get("player"),
                    error=str(e),
                )

        logger.info("Extracted starting lineups from B-Ref", game_id=game_id, count=len(lineups))

    except Exception as e:
        logger.error("Failed to extract starting lineups", game_id=game_id, error=str(e))

    return lineups


def extract_injury_notes(
    html_content: str, game_id: str, home_team: str, away_team: str, source_url: str
) -> List[InjuryStatusRow]:
    """Extract injury status from B-Ref boxscore notes.

    Args:
        html_content: Raw HTML content
        game_id: Game identifier
        home_team: Home team tricode
        away_team: Away team tricode
        source_url: Source URL for provenance

    Returns:
        List of validated InjuryStatusRow instances
    """
    injury_rows = []

    try:
        # Use the BRefClient parsing method
        from ..io_clients.bref import BRefClient

        client = BRefClient()
        injury_data = client.parse_injury_notes(html_content)

        for injury_info in injury_data:
            try:
                player_name = injury_info.get("player", "")

                # Try to determine which team the player belongs to
                # This is heuristic-based since B-Ref doesn't always clearly indicate team
                team_tricode = _guess_player_team(player_name, home_team, away_team)

                injury_row = InjuryStatusRow.from_bref_notes(
                    game_id=game_id,
                    team_tricode=team_tricode,
                    player_data=injury_info,
                    source_url=source_url,
                )
                injury_rows.append(injury_row)

            except Exception as e:
                logger.warning(
                    "Failed to create injury status row",
                    game_id=game_id,
                    player=injury_info.get("player"),
                    error=str(e),
                )

        logger.info("Extracted injury notes from B-Ref", game_id=game_id, count=len(injury_rows))

    except Exception as e:
        logger.error("Failed to extract injury notes", game_id=game_id, error=str(e))

    return injury_rows


def extract_game_metadata(html_content: str, game_id: str) -> Dict[str, Any]:
    """Extract game metadata from B-Ref boxscore HTML.

    Args:
        html_content: Raw HTML content
        game_id: Game identifier

    Returns:
        Dictionary with metadata (attendance, arena, etc.)
    """
    metadata = {}

    try:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html_content, "lxml")

        # Extract attendance
        attendance_text = soup.find(string=lambda text: text and "Attendance:" in text)
        if attendance_text:
            import re

            attendance_match = re.search(r"Attendance:\s*([\d,]+)", attendance_text)
            if attendance_match:
                attendance_str = attendance_match.group(1).replace(",", "")
                # Use robust coercion for parsing attendance
                attendance = to_int_or_none(attendance_str)
                if attendance is not None:
                    metadata["attendance"] = attendance

        # Extract arena name
        arena_elements = soup.find_all(string=lambda text: text and "Arena" in text)
        for element in arena_elements:
            if "Arena" in element or "Center" in element or "Garden" in element:
                # Clean up arena name
                arena_name = element.strip()
                if len(arena_name) < 100:  # Reasonable length check
                    metadata["arena"] = arena_name
                break

        # Extract game duration or other metadata as needed

        logger.debug("Extracted game metadata", game_id=game_id, metadata=metadata)

    except Exception as e:
        logger.warning("Failed to extract game metadata", game_id=game_id, error=str(e))

    return metadata


def _guess_player_team(player_name: str, home_team: str, away_team: str) -> str:
    """Heuristic to guess which team a player belongs to.

    This is a simplified approach - in production you'd want a proper
    player-team mapping or more sophisticated logic.

    Args:
        player_name: Player name
        home_team: Home team tricode
        away_team: Away team tricode

    Returns:
        Best guess team tricode (defaults to home team)
    """
    # For now, just default to home team
    # In a full implementation, you'd have:
    # 1. Player roster lookups
    # 2. Context clues from surrounding HTML
    # 3. Historical player-team mappings

    return home_team
