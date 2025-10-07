from typing import Any


def _validate_model_object(obj: Any, table_name: str) -> None:
    """
    Validate that a model object has required fields before insertion.

    Args:
        obj: The model object to validate
        table_name: Name of the target table

    Raises:
        ValueError: If required fields are missing or invalid
    """
    # All models must have game_id
    if not hasattr(obj, "game_id") or not obj.game_id:
        raise ValueError(f"Model object for {table_name} missing required field: game_id")

    # Check for team identification fields based on the model type
    has_team_tricode = hasattr(obj, "team_tricode") and obj.team_tricode
    has_home_away = (
        hasattr(obj, "home_team_tricode")
        and obj.home_team_tricode
        and hasattr(obj, "away_team_tricode")
        and obj.away_team_tricode
    )

    if not (has_team_tricode or has_home_away):
        raise ValueError(
            f"Model object for {table_name} missing required team identification fields. "
            f"Must have either 'team_tricode' or both 'home_team_tricode' and 'away_team_tricode'"
        )

    # All models must have source and source_url
    if not hasattr(obj, "source") or not obj.source:
        raise ValueError(f"Model object for {table_name} missing required field: source")

    if not hasattr(obj, "source_url") or not obj.source_url:
        raise ValueError(f"Model object for {table_name} missing required field: source_url")
