"""NBA Scraper CLI using Typer."""

import asyncio
from typing import List, Optional

import typer
from typing_extensions import Annotated

app = typer.Typer(help="NBA data scraping and processing CLI")

@app.command()
def backfill(
    seasons: Annotated[str, typer.Option(help="Comma-separated seasons (e.g., '2021-22,2022-23')")] = "2021-22,2022-23,2023-24,2024-25",
    start_date: Annotated[Optional[str], typer.Option("--start-date", help="Start date (YYYY-MM-DD)")] = None,
    end_date: Annotated[Optional[str], typer.Option("--end-date", help="End date (YYYY-MM-DD)")] = None,
    rate_limit: Annotated[float, typer.Option(help="API rate limit (requests per second)")] = 5.0,
    batch_size: Annotated[int, typer.Option(help="Games per batch")] = 100,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Preview mode without DB writes")] = False,
    retry_quarantined: Annotated[bool, typer.Option("--retry-quarantined", help="Retry previously failed items")] = False,
    raw_root: Annotated[str, typer.Option(help="Root directory for raw data")] = "raw"
):
    """Backfill NBA data for seasons or date ranges."""
    
    # Validate mutually exclusive options
    if start_date and seasons != "2021-22,2022-23,2023-24,2024-25":
        typer.echo("Error: Cannot specify both --seasons and --start-date", err=True)
        raise typer.Exit(1)
    
    if start_date and not end_date:
        typer.echo("Error: --end-date is required when using --start-date", err=True)
        raise typer.Exit(1)
    
    # Run the backfill
    asyncio.run(_run_backfill(
        seasons=seasons,
        start_date=start_date,
        end_date=end_date,
        rate_limit=rate_limit,
        batch_size=batch_size,
        dry_run=dry_run,
        retry_quarantined=retry_quarantined,
        raw_root=raw_root
    ))


async def _run_backfill(
    seasons: str,
    start_date: Optional[str],
    end_date: Optional[str],
    rate_limit: float,
    batch_size: int,
    dry_run: bool,
    retry_quarantined: bool,
    raw_root: str
):
    """Execute the backfill operation."""
    
    # Import orchestrator at runtime to avoid heavy imports
    from .pipelines.backfill import BackfillOrchestrator
    from .nba_logging import get_logger
    
    logger = get_logger(__name__)
    
    # Create orchestrator
    orchestrator = BackfillOrchestrator(
        rate_limit=rate_limit,
        batch_size=batch_size,
        raw_root=raw_root
    )
    
    try:
        # Check DB connection if not dry run
        if not dry_run:
            typer.echo("📡 Checking database connection...")
            # Import only when needed
            from .db import get_connection
            try:
                conn = await get_connection()
                await conn.close()
                typer.echo("✅ Database connection verified")
            except Exception as e:
                typer.echo(f"❌ Database connection failed: {e}", err=True)
                raise typer.Exit(1)
        
        if start_date and end_date:
            # Date range mode
            typer.echo(f"🗓️  Starting date range backfill: {start_date} to {end_date}")
            if dry_run:
                typer.echo("🔍 DRY RUN MODE - No data will be written")
            
            result = await orchestrator.backfill_date_range(
                start_date=start_date,
                end_date=end_date,
                dry_run=dry_run,
                retry_quarantined=retry_quarantined
            )
            
            # Print summary
            typer.echo(f"\n📊 Date Range Summary:")
            typer.echo(f"   Dates processed: {result['dates_processed']}")
            typer.echo(f"   Total games: {result['total_games']}")
            
        else:
            # Season mode
            season_list = [s.strip() for s in seasons.split(',')]
            typer.echo(f"🏀 Starting season backfill: {', '.join(season_list)}")
            if dry_run:
                typer.echo("🔍 DRY RUN MODE - No data will be written")
            
            result = await orchestrator.backfill_seasons(
                seasons=season_list,
                dry_run=dry_run,
                retry_quarantined=retry_quarantined
            )
            
            # Print summary
            typer.echo(f"\n📊 Season Summary:")
            for season_info in result['seasons_processed']:
                status_emoji = "✅" if season_info['status'] == 'completed' else "❌"
                typer.echo(f"   {status_emoji} {season_info['season']}: {season_info['games']} games")
            
            typer.echo(f"\n📈 Totals:")
            typer.echo(f"   Total dates: {result['total_dates']}")
            typer.echo(f"   Total games: {result['total_games']}")
        
        # Report errors
        if result.get('errors'):
            typer.echo(f"\n⚠️  {len(result['errors'])} errors occurred:")
            for error in result['errors'][:5]:  # Show first 5 errors
                typer.echo(f"   - {error}")
            if len(result['errors']) > 5:
                typer.echo(f"   ... and {len(result['errors']) - 5} more errors")
        
        if result.get('errors'):
            typer.echo("\n⚠️  Backfill completed with errors")
            raise typer.Exit(1)
        else:
            typer.echo("\n🎉 Backfill completed successfully!")
        
    except Exception as e:
        logger.error(f"Backfill failed: {e}")
        typer.echo(f"❌ Backfill failed: {e}", err=True)
        raise typer.Exit(1)


def main():
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    app()