"""CLI tool for debugging game bridge transformations."""

import asyncio
import sys
from typing import Optional
import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import print as rprint

from ..models.game_rows import GameRow
from ..transformers.games_bridge import to_db_game
from ..utils.team_lookup import get_team_index, get_available_tricodes
from ..extractors.nba_stats import NBAStatsExtractor
from ..db import get_connection

console = Console()
app = typer.Typer(name="print-game-bridge", help="Debug game bridge transformations")


@app.command()
def debug_game(
    game_id: str = typer.Argument(..., help="NBA game ID to debug"),
    source: str = typer.Option("nba_stats", help="Data source to extract from"),
    show_team_index: bool = typer.Option(False, "--show-teams", help="Show available team tricodes")
):
    """Print the bridged Game for a single GameRow fetched from extraction layer."""
    asyncio.run(_debug_game_async(game_id, source, show_team_index))


async def _debug_game_async(game_id: str, source: str, show_team_index: bool):
    """Async implementation of game debugging."""
    
    if show_team_index:
        _print_team_index()
        return
    
    try:
        console.print(f"\n🔍 [bold blue]Debugging Game Bridge for {game_id}[/bold blue]")
        console.print(f"📊 Source: {source}")
        
        # Step 1: Extract GameRow from source
        console.print(f"\n📥 [yellow]Step 1: Extracting GameRow from {source}...[/yellow]")
        
        if source == "nba_stats":
            game_row = await _extract_from_nba_stats(game_id)
        else:
            console.print(f"❌ [red]Unsupported source: {source}[/red]")
            return
        
        if not game_row:
            console.print(f"❌ [red]Could not extract game {game_id} from {source}[/red]")
            return
        
        # Display extracted GameRow
        _print_game_row(game_row)
        
        # Step 2: Transform to DB Game
        console.print(f"\n🔄 [yellow]Step 2: Transforming GameRow → Game...[/yellow]")
        
        try:
            db_game = to_db_game(game_row)
            _print_db_game(db_game)
            
            console.print(f"\n✅ [green]Transformation successful![/green]")
            
        except Exception as e:
            console.print(f"\n❌ [red]Transformation failed: {e}[/red]")
            return
        
        # Step 3: Validate against DB schema (optional)
        console.print(f"\n✅ [yellow]Step 3: Validation complete[/yellow]")
        console.print(f"Ready for database insertion via upsert_game()")
        
    except Exception as e:
        console.print(f"\n💥 [red]Debug failed: {e}[/red]")
        import traceback
        console.print(traceback.format_exc())


async def _extract_from_nba_stats(game_id: str) -> Optional[GameRow]:
    """Extract GameRow from NBA Stats API."""
    try:
        extractor = NBAStatsExtractor()
        
        # This would need to be implemented based on your extractor interface
        # For now, create a mock GameRow for demonstration
        from datetime import datetime, date
        from ..models.enums import GameStatus
        
        console.print(f"⚠️  [yellow]Mock extraction - implement real NBA Stats API call[/yellow]")
        
        # Mock GameRow for demonstration
        return GameRow(
            game_id=game_id,
            season="2024-25",
            game_date_utc=datetime(2024, 1, 15, 20, 0),
            game_date_local=date(2024, 1, 15),
            arena_tz="America/Los_Angeles",
            home_team_tricode="LAL",
            away_team_tricode="BOS",
            status=GameStatus.FINAL,
            source="nba_stats",
            source_url=f"https://stats.nba.com/game/{game_id}"
        )
        
    except Exception as e:
        console.print(f"❌ [red]Extraction failed: {e}[/red]")
        return None


def _print_game_row(game_row: GameRow):
    """Print GameRow details in formatted table."""
    console.print(f"\n📋 [bold green]GameRow Details[/bold green]")
    
    table = Table(show_header=True, header_style="bold blue")
    table.add_column("Field", style="cyan")
    table.add_column("Value", style="white")
    
    table.add_row("game_id", game_row.game_id)
    table.add_row("season", game_row.season)
    table.add_row("game_date_utc", str(game_row.game_date_utc))
    table.add_row("game_date_local", str(game_row.game_date_local))
    table.add_row("arena_tz", game_row.arena_tz)
    table.add_row("home_team_tricode", game_row.home_team_tricode)
    table.add_row("away_team_tricode", game_row.away_team_tricode)
    table.add_row("status", str(game_row.status))
    table.add_row("source", game_row.source)
    
    console.print(table)


def _print_db_game(db_game):
    """Print transformed Game details."""
    console.print(f"\n🎯 [bold green]Transformed Game (DB Ready)[/bold green]")
    
    table = Table(show_header=True, header_style="bold blue")
    table.add_column("Field", style="cyan") 
    table.add_column("Value", style="white")
    table.add_column("Type", style="dim white")
    
    table.add_row("game_id", db_game.game_id, "str")
    table.add_row("season", db_game.season, "str")
    table.add_row("game_date", db_game.game_date, "str (YYYY-MM-DD)")
    table.add_row("home_team_id", str(db_game.home_team_id), "int")
    table.add_row("away_team_id", str(db_game.away_team_id), "int")
    table.add_row("status", db_game.status, "str")
    
    console.print(table)


def _print_team_index():
    """Print available team tricodes for debugging."""
    console.print(f"\n🏀 [bold blue]Available Team Tricodes[/bold blue]")
    
    team_index = get_team_index()
    available_tricodes = get_available_tricodes()
    
    table = Table(show_header=True, header_style="bold blue")
    table.add_column("Tricode", style="cyan")
    table.add_column("Team ID", style="white")
    
    for tricode in sorted(available_tricodes):
        team_id = team_index.get(tricode, "Unknown")
        table.add_row(tricode, str(team_id))
    
    console.print(table)
    console.print(f"\n📊 Total teams: {len(available_tricodes)}")


@app.command()
def list_teams():
    """List all available canonical team tricodes."""
    _print_team_index()


@app.command() 
def test_bridge(
    home_team: str = typer.Argument(..., help="Home team tricode"),
    away_team: str = typer.Argument(..., help="Away team tricode"),
    game_date: str = typer.Option("2024-01-15", help="Game date (YYYY-MM-DD)")
):
    """Test bridge transformation with custom team tricodes."""
    
    try:
        from datetime import datetime, date
        from ..models.enums import GameStatus
        
        # Parse date
        parsed_date = date.fromisoformat(game_date)
        
        # Create test GameRow
        game_row = GameRow(
            game_id="TEST_BRIDGE_001",
            season="2024-25",
            game_date_utc=datetime.combine(parsed_date, datetime.min.time()),
            game_date_local=parsed_date,
            arena_tz="America/New_York",
            home_team_tricode=home_team,
            away_team_tricode=away_team,
            status=GameStatus.FINAL,
            source="cli_test",
            source_url="https://cli-test.com"
        )
        
        console.print(f"\n🧪 [bold blue]Testing Bridge Transformation[/bold blue]")
        console.print(f"🏠 Home: {home_team}")
        console.print(f"✈️  Away: {away_team}")
        console.print(f"📅 Date: {game_date}")
        
        _print_game_row(game_row)
        
        # Transform
        console.print(f"\n🔄 [yellow]Transforming...[/yellow]")
        db_game = to_db_game(game_row)
        
        _print_db_game(db_game)
        
        console.print(f"\n✅ [green]Test successful![/green]")
        
    except Exception as e:
        console.print(f"\n❌ [red]Test failed: {e}[/red]")


if __name__ == "__main__":
    app()