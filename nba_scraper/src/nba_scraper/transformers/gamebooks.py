"""Gamebooks transformer for converting player stats with clock utilities."""

from typing import Any, Dict, List

from ..models.ref_rows import RefAlternateRow, RefAssignmentRow
from ..nba_logging import get_logger
from ..utils.coerce import to_float_or_none, to_int_or_none

logger = get_logger(__name__)


def transform_gamebook_player_stats(
    raw_player_stats: List[Dict[str, Any]], source_url: str
) -> List[Dict[str, Any]]:
    """Transform raw gamebook player stats with clock conversion.

    Args:
        raw_player_stats: List of raw player stats from gamebook parser
        source_url: Source URL for provenance

    Returns:
        List of transformed player stats with converted minutes
    """
    transformed_stats = []

    try:
        for player_stat in raw_player_stats:
            transformed_stat = player_stat.copy()

            # Convert minutes from MM:SS format to seconds and milliseconds
            minutes_str = player_stat.get("min", "0:00")

            if minutes_str and minutes_str != "0:00":
                try:
                    # Parse MM:SS format directly for total game minutes
                    # Don't use parse_clock_to_ms since that's for period-based time
                    if ":" in minutes_str:
                        parts = minutes_str.split(":")
                        if len(parts) == 2:
                            # Use robust coercion for parsing minutes and seconds
                            minutes = to_int_or_none(parts[0])
                            seconds = to_int_or_none(parts[1])

                            if minutes is None or seconds is None:
                                raise ValueError(f"Invalid time format: {minutes_str}")

                            total_seconds = minutes * 60 + seconds

                            # Convert to milliseconds and seconds
                            minutes_ms = total_seconds * 1000
                            minutes_seconds = to_float_or_none(total_seconds) or 0.0
                        else:
                            raise ValueError(f"Invalid time format: {minutes_str}")
                    else:
                        raise ValueError(f"Invalid time format: {minutes_str}")

                    # Add converted fields
                    transformed_stat["minutes_played_seconds"] = minutes_seconds
                    transformed_stat["minutes_played_ms"] = minutes_ms

                    # Keep original for reference
                    transformed_stat["min_raw"] = minutes_str

                except Exception as e:
                    logger.warning(
                        "Failed to convert minutes for player",
                        player=player_stat.get("player", "Unknown"),
                        minutes_str=minutes_str,
                        error=str(e),
                    )
                    # Keep original format if conversion fails
                    transformed_stat["minutes_played_seconds"] = None
                    transformed_stat["minutes_played_ms"] = None
                    transformed_stat["min_raw"] = minutes_str
            else:
                # No minutes played
                transformed_stat["minutes_played_seconds"] = 0.0
                transformed_stat["minutes_played_ms"] = 0
                transformed_stat["min_raw"] = "0:00"

            transformed_stats.append(transformed_stat)

        logger.info(
            "Transformed gamebook player stats",
            total_players=len(transformed_stats),
            source_url=source_url,
        )

    except Exception as e:
        logger.error(
            "Failed to transform gamebook player stats", source_url=source_url, error=str(e)
        )

    return transformed_stats


def transform_referee_assignments(
    raw_assignments: List[RefAssignmentRow], source_url: str
) -> List[Dict[str, Any]]:
    """Transform referee assignments to dictionary format.

    Args:
        raw_assignments: List of RefAssignmentRow instances
        source_url: Source URL for provenance

    Returns:
        List of referee assignment dictionaries
    """
    transformed_assignments = []

    try:
        for assignment in raw_assignments:
            transformed_assignment = {
                "game_id": assignment.game_id,
                "referee_name": assignment.referee_display_name,
                "referee_slug": assignment.referee_name_slug,
                "role": assignment.role.value if assignment.role else "REFEREE",
                "crew_position": assignment.crew_position,
                "source": assignment.source,
                "source_url": assignment.source_url,
            }
            transformed_assignments.append(transformed_assignment)

        logger.info(
            "Transformed referee assignments",
            count=len(transformed_assignments),
            source_url=source_url,
        )

    except Exception as e:
        logger.error("Failed to transform referee assignments", source_url=source_url, error=str(e))

    return transformed_assignments


def transform_referee_alternates(
    raw_alternates: List[RefAlternateRow], source_url: str
) -> List[Dict[str, Any]]:
    """Transform referee alternates to dictionary format.

    Args:
        raw_alternates: List of RefAlternateRow instances
        source_url: Source URL for provenance

    Returns:
        List of referee alternate dictionaries
    """
    transformed_alternates = []

    try:
        for alternate in raw_alternates:
            transformed_alternate = {
                "game_id": alternate.game_id,
                "referee_name": alternate.referee_display_name,
                "referee_slug": alternate.referee_name_slug,
                "source": alternate.source,
                "source_url": alternate.source_url,
            }
            transformed_alternates.append(transformed_alternate)

        logger.info(
            "Transformed referee alternates",
            count=len(transformed_alternates),
            source_url=source_url,
        )

    except Exception as e:
        logger.error("Failed to transform referee alternates", source_url=source_url, error=str(e))

    return transformed_alternates


def validate_transformed_stats(transformed_stats: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Validate transformed player statistics.

    Args:
        transformed_stats: List of transformed player stats

    Returns:
        Validation results dictionary
    """
    validation_result = {
        "is_valid": True,
        "errors": [],
        "warnings": [],
        "stats": {
            "total_players": len(transformed_stats),
            "players_with_minutes": 0,
            "players_with_conversion_errors": 0,
        },
    }

    try:
        for player_stat in transformed_stats:
            # Check if minutes conversion was successful
            if player_stat.get("minutes_played_seconds") is not None:
                validation_result["stats"]["players_with_minutes"] += 1
            elif player_stat.get("min_raw", "0:00") != "0:00":
                validation_result["stats"]["players_with_conversion_errors"] += 1
                validation_result["warnings"].append(
                    f"Minutes conversion failed for {player_stat.get('player', 'Unknown')}"
                )

        # Check for critical issues
        if (
            validation_result["stats"]["players_with_conversion_errors"]
            > len(transformed_stats) // 2
        ):
            validation_result["errors"].append(
                "More than half of players have minutes conversion errors"
            )
            validation_result["is_valid"] = False

        logger.debug("Validated transformed gamebook stats", **validation_result["stats"])

    except Exception as e:
        validation_result["errors"].append(f"Validation failed: {str(e)}")
        validation_result["is_valid"] = False
        logger.error("Failed to validate transformed stats", error=str(e))

    return validation_result
