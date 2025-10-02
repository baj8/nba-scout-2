#!/usr/bin/env python3
"""Test script to populate games from NBA Stats API."""

import asyncio
from datetime import datetime
from nba_scraper.config import Settings
from nba_scraper.db import get_connection
from nba_scraper.io_clients.nba_stats import NBAStatsClient
from nba_scraper.extractors.nba_stats import extract_games_from_scoreboard
from nba_scraper.loaders.games import GameLoader
from nba_scraper.logging import get_logger

logger = get_logger(__name__)

async def populate_games_for_date(date_str: str):
    """Populate games for a specific date."""
    # Convert date string to datetime object
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    
    # Initialize NBA Stats client
    client = NBAStatsClient()
    
    try:
        # Fetch scoreboard data (pass datetime object)
        logger.info(f"Fetching scoreboard for {date_str}")
        scoreboard_data = await client.fetch_scoreboard_by_date(date_obj)
        
        if not scoreboard_data:
            logger.warning(f"No scoreboard data returned for {date_str}")
            return
        
        # Extract games
        source_url = f"https://stats.nba.com/stats/scoreboard?GameDate={date_obj.strftime('%m/%d/%Y')}"
        games = extract_games_from_scoreboard(scoreboard_data, source_url)
        
        if not games:
            logger.warning(f"No games extracted for {date_str}")
            return
        
        logger.info(f"Extracted {len(games)} games for {date_str}")
        
        # Load games into database using GameLoader
        loader = GameLoader()
        updated_count = await loader.upsert_games(games)
        logger.info(f"Successfully loaded {len(games)} games into database ({updated_count} updated)")
        
        # Print summary
        for game in games:
            logger.info(f"Game: {game.away_team_tricode} @ {game.home_team_tricode} - {game.status}")
            
    except Exception as e:
        logger.error(f"Failed to populate games: {e}")
        raise
    finally:
        # Properly await the async close method
        if hasattr(client, 'close'):
            await client.close()

if __name__ == "__main__":
    # Test with February 14, 2024 (we know this date has 13 games)
    test_date = "2024-02-14"
    asyncio.run(populate_games_for_date(test_date))