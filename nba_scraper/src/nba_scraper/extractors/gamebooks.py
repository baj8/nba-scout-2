"""NBA Game Books PDF extraction functions with comprehensive player stats parsing."""

from typing import Any, Dict, List, Optional, Union

from ..models.enums import RefRole
from ..models.ref_rows import RefAlternateRow, RefAssignmentRow
from ..nba_logging import get_logger

logger = get_logger(__name__)

# Column header aliases for robust parsing
COLUMN_ALIASES = {
    "PLAYER": "player",
    "MIN": "min",
    "MINS": "min",
    "FGM-A": "fgm_fga",
    "FG": "fgm_fga",
    "3PT": "tpm_tpa",
    "3P": "tpm_tpa",
    "3PM-A": "tpm_tpa",
    "3PTM-A": "tpm_tpa",
    "FTM-A": "ftm_fta",
    "FT": "ftm_fta",
    "+/-": "plus_minus",
    "PLUS/MINUS": "plus_minus",
    "OREB": "oreb",
    "DREB": "dreb",
    "REB": "reb",
    "AST": "ast",
    "STL": "stl",
    "BLK": "blk",
    "TO": "tov",
    "TOV": "tov",
    "PF": "pf",
    "PTS": "pts",
}


def _coerce_number(val: str) -> Union[int, float, None]:
    """Coerce string value to number with robust handling of NBA stats formats.

    Args:
        val: String value to coerce

    Returns:
        Integer, float, or None if not a valid number
    """
    if not val or val in ("", "-", "—", "DNP*", "DNP", "DND"):
        return None

    # Strip whitespace and commas
    cleaned = str(val).strip().replace(",", "")

    try:
        # Try integer first
        if "." not in cleaned:
            return int(cleaned)
        else:
            return float(cleaned)
    except (ValueError, TypeError):
        return None


def _normalize_header(raw_header: str) -> Optional[str]:
    """Normalize column header using aliases.

    Args:
        raw_header: Raw header string from PDF parsing

    Returns:
        Canonical header name or None if unknown
    """
    if not raw_header:
        return None

    canon = COLUMN_ALIASES.get(raw_header.strip().upper())
    if not canon:
        logger.debug(
            "Unknown header dropped", raw_header=raw_header, extra={"gamebook_parsing": True}
        )
        return None

    return canon


def extract_player_stats_tables(
    parsed_gamebook: Dict[str, Any], source_url: str
) -> List[Dict[str, Any]]:
    """Extract player statistics tables from parsed gamebook data.

    Args:
        parsed_gamebook: Parsed gamebook data with player stats tables
        source_url: Source URL for provenance

    Returns:
        List of player stats dictionaries with validated totals
    """
    player_stats = []

    try:
        game_id = parsed_gamebook.get("game_id")
        tables_data = parsed_gamebook.get("player_stats_tables", [])

        if not game_id:
            logger.warning("No game_id in parsed gamebook", source_url=source_url)
            return player_stats

        for table_data in tables_data:
            team_tricode = table_data.get("team", "UNK")
            raw_headers = table_data.get("headers", [])
            raw_rows = table_data.get("rows", [])

            # Normalize headers with aliasing
            normalized_headers = []
            header_mapping = {}

            for i, raw_header in enumerate(raw_headers):
                canon_header = _normalize_header(raw_header)
                if canon_header:
                    normalized_headers.append(canon_header)
                    header_mapping[i] = len(normalized_headers) - 1

            if not normalized_headers:
                logger.warning(
                    "No valid headers found in table",
                    game_id=game_id,
                    team=team_tricode,
                    raw_headers=raw_headers[:5],
                )  # Log first 5 for debugging
                continue

            # Process player rows with numeric coercion
            team_players = []
            team_totals = None

            for raw_row in raw_rows:
                if not raw_row or len(raw_row) == 0:
                    continue

                # Check if this is a totals row
                player_name = str(raw_row[0]).strip() if raw_row[0] else ""
                is_totals = player_name.lower() in ("totals", "total", "team totals")

                # Build normalized row
                normalized_row = {}

                for raw_idx, raw_value in enumerate(raw_row):
                    if raw_idx in header_mapping:
                        norm_idx = header_mapping[raw_idx]
                        header_name = normalized_headers[norm_idx]

                        if header_name == "player":
                            normalized_row[header_name] = (
                                str(raw_value).strip() if raw_value else ""
                            )
                        elif header_name == "min":
                            # Keep minutes as raw MM:SS string for later conversion
                            normalized_row[header_name] = (
                                str(raw_value).strip() if raw_value else "0:00"
                            )
                        else:
                            # Apply numeric coercion
                            normalized_row[header_name] = _coerce_number(str(raw_value))

                if is_totals:
                    team_totals = normalized_row
                else:
                    # Regular player row
                    if normalized_row.get("player"):
                        normalized_row.update(
                            {
                                "game_id": game_id,
                                "team_tricode": team_tricode,
                                "source": "gamebook_pdf",
                                "source_url": source_url,
                            }
                        )
                        team_players.append(normalized_row)

            # Validate team totals by summing player stats
            if team_totals and team_players:
                _validate_team_totals(game_id, team_tricode, team_players, team_totals)

            player_stats.extend(team_players)

        logger.info(
            "Extracted player stats from gamebook", game_id=game_id, total_players=len(player_stats)
        )

    except Exception as e:
        logger.error(
            "Failed to extract player stats from gamebook", source_url=source_url, error=str(e)
        )

    return player_stats


def _validate_team_totals(
    game_id: str, team_tricode: str, player_stats: List[Dict[str, Any]], team_totals: Dict[str, Any]
) -> None:
    """Validate team totals against sum of player stats.

    Args:
        game_id: Game identifier
        team_tricode: Team tricode
        player_stats: List of player stat dictionaries
        team_totals: Team totals dictionary
    """
    try:
        # Define stats to validate (exclude minutes and player name)
        numeric_stats = ["pts", "reb", "ast", "stl", "blk", "tov", "pf"]

        mismatches = []

        for stat in numeric_stats:
            if stat not in team_totals:
                continue

            # Sum player stats for this category
            player_sum = sum(
                (player.get(stat) or 0) for player in player_stats if player.get(stat) is not None
            )

            team_total = team_totals.get(stat) or 0

            # Check for mismatch (tolerance of 1 point for rounding)
            if abs(player_sum - team_total) > 1:
                mismatches.append(
                    {
                        "stat": stat,
                        "player_sum": player_sum,
                        "team_total": team_total,
                        "difference": abs(player_sum - team_total),
                    }
                )

        if mismatches:
            logger.warning(
                "Gamebook totals mismatch detected",
                extra={
                    "game_id": game_id,
                    "team": team_tricode,
                    "mismatches": mismatches,
                    "player_count": len(player_stats),
                },
            )

    except Exception as e:
        logger.debug(
            "Failed to validate team totals", game_id=game_id, team=team_tricode, error=str(e)
        )


def extract_referee_assignments(
    parsed_gamebook: Dict[str, Any], source_url: str
) -> List[RefAssignmentRow]:
    """Extract referee assignments from parsed game book data.

    Args:
        parsed_gamebook: Parsed game book data from GamebooksClient
        source_url: Source URL for provenance

    Returns:
        List of validated RefAssignmentRow instances
    """
    assignments = []

    try:
        game_id = parsed_gamebook.get("game_id")
        refs_data = parsed_gamebook.get("refs", [])
        parsing_confidence = parsed_gamebook.get("parsing_confidence", 0.0)

        if not game_id:
            logger.warning("No game_id in parsed gamebook", source_url=source_url)
            return assignments

        if parsing_confidence < 0.3:
            logger.warning(
                "Low parsing confidence for referee assignments",
                game_id=game_id,
                confidence=parsing_confidence,
                source_url=source_url,
            )

        for ref_data in refs_data:
            try:
                # Map string roles to RefRole enum
                role_str = ref_data.get("role", "OFFICIAL")
                role = _map_role_string_to_enum(role_str)

                assignment_row = RefAssignmentRow(
                    game_id=game_id,
                    referee_display_name=ref_data.get("name", ""),
                    referee_name_slug=_create_name_slug(ref_data.get("name", "")),
                    role=role,
                    crew_position=ref_data.get("position", 1),
                    source="gamebook_pdf",
                    source_url=source_url,
                )

                # Validate the assignment
                if _validate_referee_assignment(assignment_row):
                    assignments.append(assignment_row)
                else:
                    logger.warning(
                        "Invalid referee assignment data",
                        game_id=game_id,
                        ref_name=ref_data.get("name"),
                        role=role_str,
                    )

            except Exception as e:
                logger.warning(
                    "Failed to create referee assignment",
                    game_id=game_id,
                    ref_name=ref_data.get("name"),
                    error=str(e),
                )

        logger.info(
            "Extracted referee assignments from gamebook",
            game_id=game_id,
            count=len(assignments),
            confidence=parsing_confidence,
        )

    except Exception as e:
        logger.error("Failed to extract referee assignments", source_url=source_url, error=str(e))

    return assignments


def extract_referee_alternates(
    parsed_gamebook: Dict[str, Any], source_url: str
) -> List[RefAlternateRow]:
    """Extract referee alternates from parsed game book data.

    Args:
        parsed_gamebook: Parsed game book data from GamebooksClient
        source_url: Source URL for provenance

    Returns:
        List of validated RefAlternateRow instances
    """
    alternates = []

    try:
        game_id = parsed_gamebook.get("game_id")
        alternates_data = parsed_gamebook.get("alternates", [])
        parsing_confidence = parsed_gamebook.get("parsing_confidence", 0.0)

        if not game_id:
            logger.warning("No game_id in parsed gamebook", source_url=source_url)
            return alternates

        if not alternates_data and parsing_confidence > 0.5:
            # High confidence parsing but no alternates found - this is normal
            logger.debug(
                "No alternates found in high-confidence parsing",
                game_id=game_id,
                confidence=parsing_confidence,
            )

        for alt_name in alternates_data:
            try:
                if not alt_name or not isinstance(alt_name, str):
                    logger.warning(
                        "Invalid alternate name data", game_id=game_id, alt_data=alt_name
                    )
                    continue

                alternate_row = RefAlternateRow(
                    game_id=game_id,
                    referee_display_name=alt_name,
                    referee_name_slug=_create_name_slug(alt_name),
                    source="gamebook_pdf",
                    source_url=source_url,
                )

                # Validate the alternate
                if _validate_referee_alternate(alternate_row):
                    alternates.append(alternate_row)
                else:
                    logger.warning(
                        "Invalid referee alternate data", game_id=game_id, ref_name=alt_name
                    )

            except Exception as e:
                logger.warning(
                    "Failed to create referee alternate",
                    game_id=game_id,
                    ref_name=alt_name,
                    error=str(e),
                )

        logger.info(
            "Extracted referee alternates from gamebook",
            game_id=game_id,
            count=len(alternates),
            confidence=parsing_confidence,
        )

    except Exception as e:
        logger.error("Failed to extract referee alternates", source_url=source_url, error=str(e))

    return alternates


def extract_gamebook_metadata(parsed_gamebook: Dict[str, Any]) -> Dict[str, Any]:
    """Extract metadata from parsed game book.

    Args:
        parsed_gamebook: Parsed game book data

    Returns:
        Dictionary with enhanced metadata
    """
    metadata = {}

    try:
        game_id = parsed_gamebook.get("game_id")

        # Basic counts
        refs_count = len(parsed_gamebook.get("refs", []))
        alternates_count = len(parsed_gamebook.get("alternates", []))
        players_count = len(parsed_gamebook.get("player_stats", []))
        parsing_confidence = parsed_gamebook.get("parsing_confidence", 0.0)

        metadata.update(
            {
                "game_id": game_id,
                "refs_count": refs_count,
                "alternates_count": alternates_count,
                "players_count": players_count,
                "parsing_confidence": parsing_confidence,
                "total_officials": refs_count + alternates_count,
            }
        )

        # PDF-specific metadata
        pdf_metadata = parsed_gamebook.get("pdf_metadata", {})
        if pdf_metadata:
            metadata.update(
                {
                    "pdf_title": pdf_metadata.get("title", ""),
                    "pdf_author": pdf_metadata.get("author", ""),
                    "pdf_creator": pdf_metadata.get("creator", ""),
                    "pdf_page_count": pdf_metadata.get("page_count", 0),
                    "pdf_file_size": pdf_metadata.get("file_size", 0),
                }
            )

        # Text analysis metadata
        text_sections = parsed_gamebook.get("text_sections", {})
        if text_sections:
            metadata["text_sections_found"] = list(text_sections.keys())
            metadata["has_officials_section"] = "officials" in text_sections
            metadata["has_player_stats"] = "player_stats" in text_sections

        # Quality indicators
        metadata["extraction_quality"] = _assess_extraction_quality(
            refs_count, alternates_count, players_count, parsing_confidence, pdf_metadata
        )

        logger.debug(
            "Extracted enhanced gamebook metadata",
            game_id=game_id,
            quality=metadata["extraction_quality"],
            metadata_keys=len(metadata),
        )

    except Exception as e:
        logger.warning("Failed to extract gamebook metadata", error=str(e))

    return metadata


def validate_gamebook_data(parsed_gamebook: Dict[str, Any]) -> Dict[str, Any]:
    """Validate and assess quality of parsed gamebook data.

    Args:
        parsed_gamebook: Parsed game book data

    Returns:
        Dictionary with validation results and quality metrics
    """
    validation_result = {
        "is_valid": False,
        "errors": [],
        "warnings": [],
        "quality_score": 0.0,
        "recommended_action": "reject",
    }

    try:
        game_id = parsed_gamebook.get("game_id")
        refs = parsed_gamebook.get("refs", [])
        player_stats = parsed_gamebook.get("player_stats", [])
        confidence = parsed_gamebook.get("parsing_confidence", 0.0)

        # Check required fields
        if not game_id:
            validation_result["errors"].append("Missing game_id")
        elif not _is_valid_game_id_format(game_id):
            validation_result["warnings"].append("Unusual game_id format")

        # Check referee data quality
        if len(refs) == 0:
            validation_result["errors"].append("No referees found")
        elif len(refs) < 2:
            validation_result["warnings"].append("Fewer than 2 referees found (typical minimum)")
        elif len(refs) > 5:
            validation_result["warnings"].append("More than 5 referees found (unusual)")

        # Check player stats quality
        if len(player_stats) > 0:
            if len(player_stats) < 10:
                validation_result["warnings"].append(
                    "Fewer than 10 players found (might be incomplete)"
                )
            elif len(player_stats) > 30:
                validation_result["warnings"].append(
                    "More than 30 players found (might include duplicates)"
                )

        # Check parsing confidence
        if confidence < 0.2:
            validation_result["errors"].append("Very low parsing confidence")
        elif confidence < 0.5:
            validation_result["warnings"].append("Low parsing confidence")

        # Calculate quality score
        quality_score = 0.0

        # Base score from confidence
        quality_score += confidence * 0.4

        # Referee count bonus (2-4 refs is typical)
        ref_count = len(refs)
        if 2 <= ref_count <= 4:
            quality_score += 0.2
        elif ref_count >= 1:
            quality_score += 0.1

        # Player stats bonus
        if len(player_stats) >= 10:
            quality_score += 0.2
        elif len(player_stats) > 0:
            quality_score += 0.1

        # Game ID format bonus
        if game_id and _is_valid_game_id_format(game_id):
            quality_score += 0.2

        validation_result["quality_score"] = min(quality_score, 1.0)

        # Determine if valid and recommended action
        has_critical_errors = any(
            "Missing game_id" in err or "No referees found" in err
            for err in validation_result["errors"]
        )

        if not has_critical_errors and quality_score >= 0.4:
            validation_result["is_valid"] = True
            if quality_score >= 0.7:
                validation_result["recommended_action"] = "accept"
            else:
                validation_result["recommended_action"] = "accept_with_review"
        else:
            validation_result["recommended_action"] = "reject"

        logger.debug(
            "Validated gamebook data",
            game_id=game_id,
            is_valid=validation_result["is_valid"],
            quality_score=quality_score,
            errors_count=len(validation_result["errors"]),
            warnings_count=len(validation_result["warnings"]),
        )

    except Exception as e:
        validation_result["errors"].append(f"Validation failed: {str(e)}")
        logger.error("Failed to validate gamebook data", error=str(e))

    return validation_result


def _map_role_string_to_enum(role_str: str) -> RefRole:
    """Map string role to RefRole enum with case-insensitive matching."""
    if not role_str:
        return RefRole.REFEREE

    # Normalize the input text - handle both raw text and enum-style strings
    normalized = role_str.strip().lower()

    # Map case-insensitive text labels to RefRole enum
    if normalized == "crew chief":
        return RefRole.CREW_CHIEF
    elif normalized == "referee":
        return RefRole.REFEREE
    elif normalized == "umpire":
        return RefRole.UMPIRE
    elif normalized in ["official", "official_1", "official_2", "official_3"]:
        return RefRole.REFEREE  # Map officials to REFEREE as tests expect

    # Fallback for uppercase enum-style strings
    role_mapping = {
        "CREW_CHIEF": RefRole.CREW_CHIEF,
        "REFEREE": RefRole.REFEREE,
        "UMPIRE": RefRole.UMPIRE,
        "OFFICIAL": RefRole.REFEREE,  # Changed default mapping
        "OFFICIAL_1": RefRole.REFEREE,
        "OFFICIAL_2": RefRole.REFEREE,
        "OFFICIAL_3": RefRole.REFEREE,
    }

    return role_mapping.get(role_str.upper(), RefRole.REFEREE)


def _create_name_slug(name: str) -> str:
    """Create name slug from display name."""
    if not name:
        return ""

    # Remove punctuation and convert to lowercase
    import re

    slug = re.sub(r"[^\w\s]", "", name)
    slug = re.sub(r"\s+", "", slug)  # Remove all spaces

    return slug.lower() if slug else ""


def _validate_referee_assignment(assignment: RefAssignmentRow) -> bool:
    """Validate a referee assignment row."""
    if not assignment.game_id:
        return False

    if not assignment.referee_display_name or len(assignment.referee_display_name.strip()) < 3:
        return False

    if not assignment.referee_name_slug:
        return False

    if assignment.crew_position and (assignment.crew_position < 1 or assignment.crew_position > 10):
        return False

    return True


def _validate_referee_alternate(alternate: RefAlternateRow) -> bool:
    """Validate a referee alternate row."""
    if not alternate.game_id:
        return False

    if not alternate.referee_display_name or len(alternate.referee_display_name.strip()) < 3:
        return False

    if not alternate.referee_name_slug:
        return False

    return True


def _assess_extraction_quality(
    refs_count: int,
    alternates_count: int,
    players_count: int,
    confidence: float,
    pdf_metadata: Dict[str, Any],
) -> str:
    """Assess overall extraction quality."""
    if confidence >= 0.8 and refs_count >= 2 and players_count >= 10:
        return "excellent"
    elif confidence >= 0.6 and refs_count >= 2 and players_count >= 5:
        return "good"
    elif confidence >= 0.4 and refs_count >= 1:
        return "fair"
    elif refs_count >= 1 or players_count > 0:
        return "poor"
    else:
        return "failed"


def _is_valid_game_id_format(game_id: str) -> bool:
    """Check if game_id follows expected NBA format."""
    if not game_id:
        return False

    # NBA regular season games typically start with 0022, but be more flexible
    if len(game_id) == 10 and game_id.isdigit():
        # Allow any 10-digit game ID, not just those starting with 0022
        return True

    # Allow other reasonable formats (8-12 alphanumeric characters)
    if 8 <= len(game_id) <= 12 and game_id.replace("-", "").replace("_", "").isalnum():
        return True

    return False
