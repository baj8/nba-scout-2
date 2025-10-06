def normalize_status(status_value):
    """Normalize status to GameStatus enum."""
    if not status_value or status_value.strip() == "":
        return GameStatus.SCHEDULED
    
    # If it's already a GameStatus enum, return it as-is
    if isinstance(status_value, GameStatus):
        return status_value
    
    # Convert string to uppercase for matching
    status_str = str(status_value).upper().strip()
    
    # Map common status strings to enum values
    status_mapping = {
        'SCHEDULED': GameStatus.SCHEDULED,
        'FINAL': GameStatus.FINAL,
        'LIVE': GameStatus.LIVE,
        'IN_PROGRESS': GameStatus.LIVE,
        'POSTPONED': GameStatus.POSTPONED,
        'CANCELLED': GameStatus.POSTPONED,
        'PPD': GameStatus.POSTPONED
    }
    
    return status_mapping.get(status_str, GameStatus.SCHEDULED)