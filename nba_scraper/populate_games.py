#!/usr/bin/env python3
"""
Simple script to populate games from NBA Stats API scoreboard data.
This fills the gap where daily/backfill pipelines expect games to already exist.
"""

import asyncio
import sys
from datetime import datetime

from src.nba_scraper.config import get_settings
from src.nba_scraper.io_clients.nba_stats import NBAStatsClient
from src.nba_scraper.extractors.nba_stats import extract_games_from_scoreboard
from src.nba_scraper.loaders.games import GameLoader
from src.nba_scraper.logging import get_logger

logger = get_logger(__name__)

async def populate_games_for_date(date_str: str):
    """Populate games for a specific date from NBA Stats API."""
    
    # Initialize components
    nba_client = NBAStatsClient()
    game_loader = GameLoader()
    
    try:
        logger.info(f"Fetching scoreboard data for {date_str}")
        
        # Convert date string to datetime for the API call
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        
        # Fetch scoreboard data
        scoreboard_data = await nba_client.fetch_scoreboard_by_date(date_obj)
        
        if not scoreboard_data:
            logger.warning(f"No scoreboard data found for {date_str}")
            return
            
        # Extract games
        source_url = f"https://stats.nba.com/stats/scoreboard/?GameDate={date_str}"
        games = extract_games_from_scoreboard(scoreboard_data, source_url)
        
        if not games:
            logger.warning(f"No games extracted from scoreboard for {date_str}")
            return
            
        logger.info(f"Extracted {len(games)} games for {date_str}")
        
        # Load games into database
        inserted_count = await game_loader.upsert_games(games)
        logger.info(f"Upserted {inserted_count} games for {date_str}")
        
        # Print summary
        for game in games:
            print(f"  {game.away_team_tricode} @ {game.home_team_tricode} - {game.status.value}")
            
    except Exception as e:
        logger.error(f"Failed to populate games for {date_str}", error=str(e))
        raise
        
    finally:
        await nba_client.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python populate_games.py YYYY-MM-DD")
        sys.exit(1)
        
    date_str = sys.argv[1]
    
    # Validate date format
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        print("Invalid date format. Use YYYY-MM-DD")
        sys.exit(1)
        
    asyncio.run(populate_games_for_date(date_str))
