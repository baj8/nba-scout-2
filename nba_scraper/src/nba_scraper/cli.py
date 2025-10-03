"""NBA Scraper CLI with Typer commands."""

import asyncio
from datetime import date, datetime, timedelta
from typing import List, Optional

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from .config import get_settings, validate_configuration
from .db import check_connection, create_tables, get_connection
from .nba_logging import configure_logging, get_logger, metrics, health_checker
from .nba_logging import alert_manager
from .monitoring import get_health_status, get_metrics_summary, prometheus_exporter
from .pipelines.backfill import BackfillPipeline
from .pipelines.daily import DailyPipeline
from .pipelines.derive import DerivePipeline
from .pipelines.validate import ValidationPipeline

# Configure logging
configure_logging()
logger = get_logger(__name__)

# Create Typer app
app = typer.Typer(
    name="nba-scraper",
    help="NBA Historical Scraping & Ingestion Engine",
    rich_markup_mode="rich",
)

console = Console()


@app.command()
def backfill(
    seasons: str = typer.Option(
        "2021-22,2022-23,2023-24,2024-25",
        "--seasons",
        help="Comma-separated list of seasons to backfill"
    ),
    skip_games: bool = typer.Option(False, "--skip-games", help="Skip games backfill"),
    skip_refs: bool = typer.Option(False, "--skip-refs", help="Skip referee backfill"),
    skip_lineups: bool = typer.Option(False, "--skip-lineups", help="Skip lineups backfill"),
    skip_pbp: bool = typer.Option(False, "--skip-pbp", help="Skip play-by-play backfill"),
    resume: bool = typer.Option(False, "--resume", help="Resume from last checkpoint"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Dry run without database writes"),
) -> None:
    """Backfill historical NBA data for specified seasons."""
    
    async def run_backfill() -> None:
        settings = get_settings()
        
        # Parse seasons
        season_list = [s.strip() for s in seasons.split(",")]
        
        console.print(f"🏀 [bold blue]NBA Scraper Backfill[/bold blue]")
        console.print(f"Seasons: {', '.join(season_list)}")
        console.print(f"Resume: {resume}")
        console.print(f"Dry run: {dry_run}")
        
        # Check database connection
        if not await check_connection():
            console.print("❌ [red]Database connection failed[/red]")
            raise typer.Exit(1)
        
        # Create tables if needed
        if not dry_run:
            await create_tables()
            console.print("✅ [green]Database tables ready[/green]")
        
        # Initialize pipeline
        pipeline = BackfillPipeline()
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            
            if not skip_games:
                task = progress.add_task("Backfilling games...", total=None)
                await pipeline.backfill_games(season_list, resume=resume, dry_run=dry_run)
                progress.update(task, completed=True)
            
            if not skip_refs:
                task = progress.add_task("Backfilling referees...", total=None)
                await pipeline.backfill_refs(season_list, resume=resume, dry_run=dry_run)
                progress.update(task, completed=True)
            
            if not skip_lineups:
                task = progress.add_task("Backfilling lineups...", total=None)
                await pipeline.backfill_lineups_and_injuries(season_list, resume=resume, dry_run=dry_run)
                progress.update(task, completed=True)
            
            if not skip_pbp:
                task = progress.add_task("Backfilling play-by-play...", total=None)
                await pipeline.backfill_pbp(season_list, resume=resume, dry_run=dry_run)
                progress.update(task, completed=True)
        
        console.print("🎉 [bold green]Backfill completed successfully![/bold green]")
    
    try:
        asyncio.run(run_backfill())
    except KeyboardInterrupt:
        console.print("⚠️  [yellow]Backfill interrupted by user[/yellow]")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"❌ [red]Backfill failed: {e}[/red]")
        logger.error("Backfill failed", error=str(e))
        raise typer.Exit(1)


@app.command()
def daily(
    date_range: Optional[str] = typer.Option(
        None,
        "--date-range",
        help="Date range to process (YYYY-MM-DD..YYYY-MM-DD or single date)"
    ),
    force: bool = typer.Option(False, "--force", help="Force reprocessing of existing data"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Dry run without database writes"),
) -> None:
    """Run daily incremental data ingestion."""
    
    async def run_daily() -> None:
        # Parse date range
        if date_range:
            if ".." in date_range:
                start_str, end_str = date_range.split("..")
                start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
                end_date = datetime.strptime(end_str, "%Y-%m-%d").date()
            else:
                start_date = end_date = datetime.strptime(date_range, "%Y-%m-%d").date()
        else:
            # Default to yesterday
            yesterday = date.today() - timedelta(days=1)
            start_date = end_date = yesterday
        
        console.print(f"📅 [bold blue]NBA Scraper Daily Run[/bold blue]")
        console.print(f"Date range: {start_date} to {end_date}")
        console.print(f"Force: {force}")
        console.print(f"Dry run: {dry_run}")
        
        # Check database connection
        if not await check_connection():
            console.print("❌ [red]Database connection failed[/red]")
            raise typer.Exit(1)
        
        # Initialize pipeline
        pipeline = DailyPipeline()
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            
            task = progress.add_task("Running daily ingestion...", total=None)
            
            current_date = start_date
            while current_date <= end_date:
                await pipeline.run_daily(current_date, force=force, dry_run=dry_run)
                current_date += timedelta(days=1)
            
            progress.update(task, completed=True)
        
        console.print("🎉 [bold green]Daily run completed successfully![/bold green]")
    
    try:
        asyncio.run(run_daily())
    except KeyboardInterrupt:
        console.print("⚠️  [yellow]Daily run interrupted by user[/yellow]")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"❌ [red]Daily run failed: {e}[/red]")
        logger.error("Daily run failed", error=str(e))
        raise typer.Exit(1)


@app.command()
def derive(
    date_range: str = typer.Option(
        ...,
        "--date-range",
        help="Date range to derive analytics for (YYYY-MM-DD..YYYY-MM-DD)"
    ),
    tables: Optional[str] = typer.Option(
        None,
        "--tables",
        help="Comma-separated list of tables to derive (q1_window,early_shocks,schedule_travel,outcomes)"
    ),
    force: bool = typer.Option(False, "--force", help="Force recomputation of existing data"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Dry run without database writes"),
) -> None:
    """Derive analytics tables from raw data."""
    
    async def run_derive() -> None:
        # Parse date range
        start_str, end_str = date_range.split("..")
        start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_str, "%Y-%m-%d").date()
        
        # Parse tables
        table_list = None
        if tables:
            table_list = [t.strip() for t in tables.split(",")]
        
        console.print(f"📊 [bold blue]NBA Scraper Derive Analytics[/bold blue]")
        console.print(f"Date range: {start_date} to {end_date}")
        console.print(f"Tables: {table_list or 'all'}")
        console.print(f"Force: {force}")
        console.print(f"Dry run: {dry_run}")
        
        # Check database connection
        if not await check_connection():
            console.print("❌ [red]Database connection failed[/red]")
            raise typer.Exit(1)
        
        # Initialize pipeline
        pipeline = DerivePipeline()
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            
            task = progress.add_task("Deriving analytics...", total=None)
            await pipeline.derive_all(start_date, end_date, tables=table_list, force=force, dry_run=dry_run)
            progress.update(task, completed=True)
        
        console.print("🎉 [bold green]Analytics derivation completed successfully![/bold green]")
    
    try:
        asyncio.run(run_derive())
    except KeyboardInterrupt:
        console.print("⚠️  [yellow]Derive interrupted by user[/yellow]")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"❌ [red]Derive failed: {e}[/red]")
        logger.error("Derive failed", error=str(e))
        raise typer.Exit(1)


@app.command()
def validate(
    since: str = typer.Option(
        "2023-10-01",
        "--since",
        help="Validate data since this date (YYYY-MM-DD)"
    ),
    verbose: bool = typer.Option(False, "--verbose", help="Verbose validation output"),
) -> None:
    """Run data quality validation checks."""
    
    async def run_validate() -> None:
        since_date = datetime.strptime(since, "%Y-%m-%d").date()
        
        console.print(f"🔍 [bold blue]NBA Scraper Data Validation[/bold blue]")
        console.print(f"Since: {since_date}")
        console.print(f"Verbose: {verbose}")
        
        # Check database connection
        if not await check_connection():
            console.print("❌ [red]Database connection failed[/red]")
            raise typer.Exit(1)
        
        # Initialize validation pipeline
        pipeline = ValidationPipeline()
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            
            task = progress.add_task("Running validation checks...", total=None)
            results = await pipeline.validate_all(since_date, verbose=verbose)
            progress.update(task, completed=True)
        
        # Display results
        table = Table(title="Validation Results")
        table.add_column("Check", style="cyan")
        table.add_column("Status", style="green")
        table.add_column("Details", style="white")
        
        for check_name, result in results.items():
            status = "✅ PASS" if result['passed'] else "❌ FAIL"
            details = result.get('details', '')
            table.add_row(check_name, status, details)
        
        console.print(table)
        
        # Overall status
        failed_checks = [name for name, result in results.items() if not result['passed']]
        if failed_checks:
            console.print(f"❌ [red]{len(failed_checks)} validation checks failed[/red]")
            raise typer.Exit(1)
        else:
            console.print("🎉 [bold green]All validation checks passed![/bold green]")
    
    try:
        asyncio.run(run_validate())
    except KeyboardInterrupt:
        console.print("⚠️  [yellow]Validation interrupted by user[/yellow]")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"❌ [red]Validation failed: {e}[/red]")
        logger.error("Validation failed", error=str(e))
        raise typer.Exit(1)


@app.command()
def config(
    show_secrets: bool = typer.Option(False, "--show-secrets", help="Show API keys and secrets (use with caution)"),
    validate_only: bool = typer.Option(False, "--validate-only", help="Only run validation checks")
) -> None:
    """Show current configuration and validate settings."""
    
    try:
        settings = get_settings()
        
        console.print(f"⚙️  [bold blue]NBA Scraper Configuration[/bold blue]")
        console.print()
        
        if not validate_only:
            # Environment information
            console.print("[bold cyan]Environment Information[/bold cyan]")
            env_table = Table(show_header=False)
            env_table.add_column("Setting", style="cyan")
            env_table.add_column("Value", style="green")
            
            env_table.add_row("Environment", settings.environment.value)
            env_table.add_row("App Name", settings.app_name)
            env_table.add_row("App Version", settings.app_version)
            env_table.add_row("Debug Mode", "✅ Enabled" if settings.debug else "❌ Disabled")
            
            console.print(env_table)
            console.print()
            
            # Database configuration
            console.print("[bold cyan]Database Configuration[/bold cyan]")
            db_table = Table(show_header=False)
            db_table.add_column("Setting", style="cyan")
            db_table.add_column("Value", style="green")
            
            # Mask password in database URL for security
            db_url = settings.database.url
            if not show_secrets and '@' in db_url:
                # Hide password: postgresql://user:***@host:port/db
                parts = db_url.split('@')
                if len(parts) > 1:
                    before_at = parts[0]
                    if ':' in before_at:
                        scheme_user = before_at.rsplit(':', 1)[0]
                        db_url = f"{scheme_user}:***@{'@'.join(parts[1:])}"
            
            db_table.add_row("Database URL", db_url)
            db_table.add_row("Pool Size", str(settings.database.pool_size))
            db_table.add_row("Max Overflow", str(settings.database.max_overflow))
            db_table.add_row("SSL Mode", settings.database.ssl_mode)
            
            console.print(db_table)
            console.print()
            
            # Pipeline configuration
            console.print("[bold cyan]Pipeline Configuration[/bold cyan]")
            pipeline_table = Table(show_header=False)
            pipeline_table.add_column("Setting", style="cyan")
            pipeline_table.add_column("Value", style="green")
            
            pipeline_table.add_row("Requests/Min", str(settings.requests_per_min))
            pipeline_table.add_row("Max Concurrent", str(settings.max_concurrent_requests))
            pipeline_table.add_row("Backfill Chunk Size", str(settings.backfill_chunk_size))
            pipeline_table.add_row("Checkpoints", "✅ Enabled" if settings.checkpoint_enabled else "❌ Disabled")
            
            console.print(pipeline_table)
            console.print()
            
            # Cache configuration
            console.print("[bold cyan]Cache Configuration[/bold cyan]")
            cache_table = Table(show_header=False)
            cache_table.add_column("Setting", style="cyan")
            cache_table.add_column("Value", style="green")
            
            cache_table.add_row("Cache Directory", str(settings.cache.dir))
            cache_table.add_row("HTTP Cache TTL", f"{settings.cache.http_ttl}s")
            cache_table.add_row("HTTP Cache", "✅ Enabled" if settings.cache.enable_http_cache else "❌ Disabled")
            
            if settings.cache.redis_url:
                redis_url = settings.cache.redis_url
                if not show_secrets and '@' in redis_url:
                    # Hide password in Redis URL
                    parts = redis_url.split('@')
                    if len(parts) > 1:
                        before_at = parts[0]
                        if ':' in before_at:
                            scheme_user = before_at.rsplit(':', 1)[0]
                            redis_url = f"{scheme_user}:***@{'@'.join(parts[1:])}"
                cache_table.add_row("Redis URL", redis_url)
                cache_table.add_row("Redis TTL", f"{settings.cache.redis_ttl}s")
            else:
                cache_table.add_row("Redis", "❌ Not configured")
            
            console.print(cache_table)
            console.print()
            
            # Monitoring configuration
            console.print("[bold cyan]Monitoring Configuration[/bold cyan]")
            monitoring_table = Table(show_header=False)
            monitoring_table.add_column("Setting", style="cyan")
            monitoring_table.add_column("Value", style="green")
            
            monitoring_table.add_row("Metrics", "✅ Enabled" if settings.monitoring.enable_metrics else "❌ Disabled")
            monitoring_table.add_row("Health Checks", "✅ Enabled" if settings.monitoring.enable_health_checks else "❌ Disabled")
            monitoring_table.add_row("Tracing", "✅ Enabled" if settings.monitoring.enable_tracing else "❌ Disabled")
            
            if settings.monitoring.enable_metrics:
                monitoring_table.add_row("Metrics Port", str(settings.monitoring.metrics_port))
            if settings.monitoring.enable_health_checks:
                monitoring_table.add_row("Health Port", str(settings.monitoring.health_check_port))
            
            console.print(monitoring_table)
            console.print()
            
            # API Keys status
            console.print("[bold cyan]API Keys Status[/bold cyan]")
            api_table = Table(show_header=False)
            api_table.add_column("Service", style="cyan")
            api_table.add_column("Status", style="green")
            
            # Check which API keys are configured
            api_keys = settings.api_keys
            
            nba_status = "✅ Configured" if api_keys.nba_stats_api_key else "❌ Not configured"
            bref_status = "✅ Configured" if api_keys.bref_api_key else "❌ Not configured"
            sentry_status = "✅ Configured" if api_keys.sentry_dsn else "❌ Not configured"
            slack_status = "✅ Configured" if api_keys.slack_webhook_url else "❌ Not configured"
            
            api_table.add_row("NBA Stats API", nba_status)
            api_table.add_row("Basketball Reference", bref_status)
            api_table.add_row("Sentry DSN", sentry_status)
            api_table.add_row("Slack Webhook", slack_status)
            
            if show_secrets:
                console.print("[yellow]⚠️  Showing secrets (use with caution):[/yellow]")
                if api_keys.nba_stats_api_key:
                    api_table.add_row("NBA Stats Key", api_keys.nba_stats_api_key.get_secret_value()[:10] + "...")
                if api_keys.sentry_dsn:
                    api_table.add_row("Sentry DSN", api_keys.sentry_dsn.get_secret_value()[:20] + "...")
            
            console.print(api_table)
            console.print()
        
        # Configuration validation
        console.print("[bold cyan]Configuration Validation[/bold cyan]")
        
        validation_results = validate_configuration()
        
        validation_table = Table()
        validation_table.add_column("Check", style="cyan")
        validation_table.add_column("Status", style="green")
        validation_table.add_column("Details", style="white")
        
        for check_name, result in validation_results.items():
            if check_name == 'validation_error':
                status = "❌ ERROR"
                details = str(result)
            else:
                status = "✅ PASS" if result else "❌ FAIL"
                details = "Valid" if result else "Invalid or missing"
            
            validation_table.add_row(check_name.replace('_', ' ').title(), status, details)
        
        console.print(validation_table)
        
        # Overall validation status
        failed_validations = [name for name, result in validation_results.items() 
                            if (name != 'validation_error' and not result) or 
                               (name == 'validation_error')]
        
        if failed_validations:
            console.print(f"\n❌ [red]{len(failed_validations)} validation check(s) failed[/red]")
            console.print("[yellow]Fix the issues above before running the application[/yellow]")
            raise typer.Exit(1)
        else:
            console.print("\n🎉 [bold green]All configuration checks passed![/bold green]")
        
    except Exception as e:
        console.print(f"❌ [red]Configuration check failed: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def status() -> None:
    """Show current system status and statistics."""
    
    async def show_status() -> None:
        console.print(f"📊 [bold blue]NBA Scraper Status[/bold blue]")
        
        # Check database connection
        db_connected = await check_connection()
        db_status = "🟢 Connected" if db_connected else "🔴 Disconnected"
        console.print(f"Database: {db_status}")
        
        if not db_connected:
            console.print("❌ Cannot retrieve system statistics without database connection")
            return
        
        settings = get_settings()
        console.print(f"Rate limit: {settings.requests_per_min} req/min")
        console.print(f"Cache dir: {settings.cache.dir}")
        console.print()
        
        try:
            conn = await get_connection()
            
            # Database statistics
            console.print("[bold cyan]Database Statistics[/bold cyan]")
            
            stats_query = """
            WITH table_stats AS (
                SELECT 'games' as table_name, COUNT(*) as record_count, 
                       MAX(ingested_at_utc) as last_ingested,
                       COUNT(CASE WHEN game_date_local >= CURRENT_DATE - INTERVAL '7 days' THEN 1 END) as recent_records
                FROM games
                UNION ALL
                SELECT 'pbp_events' as table_name, COUNT(*) as record_count,
                       MAX(ingested_at_utc) as last_ingested,
                       COUNT(CASE WHEN ingested_at_utc >= NOW() - INTERVAL '7 days' THEN 1 END) as recent_records
                FROM pbp_events
                UNION ALL
                SELECT 'outcomes' as table_name, COUNT(*) as record_count,
                       MAX(ingested_at_utc) as last_ingested,
                       COUNT(CASE WHEN ingested_at_utc >= NOW() - INTERVAL '7 days' THEN 1 END) as recent_records
                FROM outcomes
                UNION ALL
                SELECT 'q1_window_12_8' as table_name, COUNT(*) as record_count,
                       MAX(ingested_at_utc) as last_ingested,
                       COUNT(CASE WHEN ingested_at_utc >= NOW() - INTERVAL '7 days' THEN 1 END) as recent_records
                FROM q1_window_12_8
                UNION ALL
                SELECT 'early_shocks' as table_name, COUNT(*) as record_count,
                       MAX(ingested_at_utc) as last_ingested,
                       COUNT(CASE WHEN ingested_at_utc >= NOW() - INTERVAL '7 days' THEN 1 END) as recent_records
                FROM early_shocks
                UNION ALL
                SELECT 'schedule_travel' as table_name, COUNT(*) as record_count,
                       MAX(ingested_at_utc) as last_ingested,
                       COUNT(CASE WHEN ingested_at_utc >= NOW() - INTERVAL '7 days' THEN 1 END) as recent_records
                FROM schedule_travel
            )
            SELECT table_name, record_count, last_ingested, recent_records,
                   CASE 
                       WHEN last_ingested IS NULL THEN NULL
                       ELSE EXTRACT(EPOCH FROM (NOW() - last_ingested)) / 3600
                   END as hours_since_last
            FROM table_stats
            ORDER BY table_name
            """
            
            rows = await conn.fetch(stats_query)
            
            # Create statistics table
            stats_table = Table()
            stats_table.add_column("Table", style="cyan")
            stats_table.add_column("Records", justify="right", style="green")
            stats_table.add_column("Last Ingested", style="yellow")
            stats_table.add_column("Recent (7d)", justify="right", style="blue")
            
            for row in rows:
                table_name = row['table_name']
                record_count = f"{row['record_count']:,}"
                recent_count = f"{row['recent_records']:,}"
                
                if row['last_ingested']:
                    hours_ago = row['hours_since_last']
                    if hours_ago < 1:
                        last_ingested = "< 1 hour ago"
                    elif hours_ago < 24:
                        last_ingested = f"{hours_ago:.1f} hours ago"
                    else:
                        last_ingested = f"{hours_ago/24:.1f} days ago"
                else:
                    last_ingested = "Never"
                
                stats_table.add_row(table_name, record_count, last_ingested, recent_count)
            
            console.print(stats_table)
            console.print()
            
            # Current season info
            console.print("[bold cyan]Current Season Info[/bold cyan]")
            
            season_query = """
            WITH season_stats AS (
                SELECT 
                    season,
                    COUNT(*) as total_games,
                    COUNT(CASE WHEN status = 'FINAL' THEN 1 END) as final_games,
                    COUNT(CASE WHEN status = 'LIVE' THEN 1 END) as live_games,
                    COUNT(CASE WHEN status = 'SCHEDULED' THEN 1 END) as scheduled_games,
                    MIN(game_date_local) as season_start,
                    MAX(game_date_local) as season_end,
                    MAX(CASE WHEN status = 'FINAL' THEN game_date_local END) as last_final_game
                FROM games 
                WHERE season IN (
                    SELECT DISTINCT season 
                    FROM games 
                    ORDER BY season DESC 
                    LIMIT 2
                )
                GROUP BY season
                ORDER BY season DESC
            )
            SELECT * FROM season_stats
            """
            
            season_rows = await conn.fetch(season_query)
            
            if season_rows:
                season_table = Table()
                season_table.add_column("Season", style="cyan")
                season_table.add_column("Total Games", justify="right", style="green")
                season_table.add_column("Final", justify="right", style="blue")
                season_table.add_column("Live", justify="right", style="red")
                season_table.add_column("Scheduled", justify="right", style="yellow")
                season_table.add_column("Last Final Game", style="white")
                
                for row in season_rows:
                    season_table.add_row(
                        row['season'],
                        str(row['total_games']),
                        str(row['final_games']),
                        str(row['live_games']),
                        str(row['scheduled_games']),
                        str(row['last_final_game']) if row['last_final_game'] else "None"
                    )
                
                console.print(season_table)
                console.print()
            
            # Data quality summary
            console.print("[bold cyan]Data Quality Summary[/bold cyan]")
            
            quality_query = """
            WITH quality_stats AS (
                SELECT 
                    COUNT(*) as total_final_games,
                    COUNT(CASE WHEN home_score IS NOT NULL AND away_score IS NOT NULL THEN 1 END) as games_with_scores,
                    COUNT(CASE WHEN q1_home_points IS NOT NULL AND q1_away_points IS NOT NULL THEN 1 END) as games_with_q1,
                    (SELECT COUNT(DISTINCT game_id) FROM pbp_events WHERE game_id IN (SELECT game_id FROM games WHERE status = 'FINAL')) as games_with_pbp,
                    (SELECT COUNT(DISTINCT game_id) FROM outcomes) as games_with_outcomes
                FROM games 
                WHERE status = 'FINAL' 
                  AND game_date_local >= CURRENT_DATE - INTERVAL '30 days'
            )
            SELECT *,
                   CASE WHEN total_final_games > 0 THEN games_with_scores::float / total_final_games ELSE 0 END as score_coverage,
                   CASE WHEN total_final_games > 0 THEN games_with_q1::float / total_final_games ELSE 0 END as q1_coverage,
                   CASE WHEN total_final_games > 0 THEN games_with_pbp::float / total_final_games ELSE 0 END as pbp_coverage,
                   CASE WHEN total_final_games > 0 THEN games_with_outcomes::float / total_final_games ELSE 0 END as outcome_coverage
            FROM quality_stats
            """
            
            quality_row = await conn.fetchrow(quality_query)
            
            if quality_row:
                console.print(f"Recent games (30 days): {quality_row['total_final_games']}")
                console.print(f"Score coverage: {quality_row['score_coverage']:.1%}")
                console.print(f"Q1 coverage: {quality_row['q1_coverage']:.1%}")
                console.print(f"PBP coverage: {quality_row['pbp_coverage']:.1%}")
                console.print(f"Outcome coverage: {quality_row['outcome_coverage']:.1%}")
                console.print()
            
            # Recent activity
            console.print("[bold cyan]Recent Activity[/bold cyan]")
            
            activity_query = """
            SELECT 
                DATE_TRUNC('day', ingested_at_utc) as ingestion_date,
                COUNT(*) as records_ingested,
                COUNT(DISTINCT game_id) as games_affected
            FROM (
                SELECT game_id, ingested_at_utc FROM games WHERE ingested_at_utc >= NOW() - INTERVAL '7 days'
                UNION ALL
                SELECT game_id, ingested_at_utc FROM pbp_events WHERE ingested_at_utc >= NOW() - INTERVAL '7 days'
                UNION ALL
                SELECT game_id, ingested_at_utc FROM outcomes WHERE ingested_at_utc >= NOW() - INTERVAL '7 days'
            ) recent_activity
            GROUP BY DATE_TRUNC('day', ingested_at_utc)
            ORDER BY ingestion_date DESC
            LIMIT 7
            """
            
            activity_rows = await conn.fetch(activity_query)
            
            if activity_rows:
                activity_table = Table()
                activity_table.add_column("Date", style="cyan")
                activity_table.add_column("Records Ingested", justify="right", style="green")
                activity_table.add_column("Games Affected", justify="right", style="blue")
                
                for row in activity_rows:
                    date_str = row['ingestion_date'].strftime('%Y-%m-%d')
                    activity_table.add_row(
                        date_str,
                        f"{row['records_ingested']:,}",
                        str(row['games_affected'])
                    )
                
                console.print(activity_table)
            else:
                console.print("No recent ingestion activity found")
                
        except Exception as e:
            console.print(f"❌ [red]Failed to retrieve system statistics: {e}[/red]")
    
    try:
        asyncio.run(show_status())
    except Exception as e:
        console.print(f"❌ [red]Status check failed: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def health(
    detailed: bool = typer.Option(False, "--detailed", help="Show detailed health check results")
) -> None:
    """Check application health status."""
    
    async def run_health_check() -> None:
        console.print(f"🏥 [bold blue]NBA Scraper Health Check[/bold blue]")
        
        try:
            # Run comprehensive health checks
            health_status = await health_checker.run_all_checks()
            
            # Display overall status
            if health_status['status'] == 'healthy':
                console.print("✅ [bold green]System is healthy[/bold green]")
            else:
                console.print("❌ [bold red]System is unhealthy[/bold red]")
            
            console.print(f"Timestamp: {health_status['timestamp']}")
            console.print()
            
            # Display individual check results
            if detailed or health_status['status'] != 'healthy':
                console.print("[bold cyan]Health Check Details[/bold cyan]")
                
                health_table = Table()
                health_table.add_column("Check", style="cyan")
                health_table.add_column("Status", style="green")
                health_table.add_column("Message", style="white")
                health_table.add_column("Duration (ms)", justify="right", style="yellow")
                
                for check_name, check_result in health_status['checks'].items():
                    status = "✅ HEALTHY" if check_result['healthy'] else "❌ UNHEALTHY"
                    health_table.add_row(
                        check_name.replace('_', ' ').title(),
                        status,
                        check_result['message'],
                        str(check_result['duration_ms'])
                    )
                
                console.print(health_table)
            
            # Exit with error code if unhealthy
            if health_status['status'] != 'healthy':
                raise typer.Exit(1)
                
        except Exception as e:
            console.print(f"❌ [red]Health check failed: {e}[/red]")
            raise typer.Exit(1)
    
    try:
        asyncio.run(run_health_check())
    except Exception as e:
        console.print(f"❌ [red]Health check error: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def metrics(
    format: str = typer.Option("json", "--format", help="Output format: json, prometheus, summary"),
    output_file: Optional[str] = typer.Option(None, "--output", help="Write metrics to file")
) -> None:
    """Display current application metrics."""
    
    try:
        console.print(f"📈 [bold blue]NBA Scraper Metrics[/bold blue]")
        
        if format == "summary":
            # Show metrics summary
            summary = get_metrics_summary()
            
            console.print(f"Timestamp: {summary.get('timestamp', 'N/A')}")
            console.print(f"Counters: {summary.get('counters_count', 0)}")
            console.print(f"Gauges: {summary.get('gauges_count', 0)}")
            console.print(f"Histograms: {summary.get('histograms_count', 0)}")
            console.print(f"Timers: {summary.get('timers_count', 0)}")
            
            if 'sample_counters' in summary:
                console.print("\n[bold cyan]Sample Counters[/bold cyan]")
                for metric_name, value in summary['sample_counters'].items():
                    console.print(f"  {metric_name}: {value}")
            
        elif format == "prometheus":
            # Export in Prometheus format
            prometheus_metrics = prometheus_exporter.export_metrics()
            
            if output_file:
                with open(output_file, 'w') as f:
                    f.write(prometheus_metrics)
                console.print(f"✅ Prometheus metrics written to {output_file}")
            else:
                console.print("[bold cyan]Prometheus Metrics[/bold cyan]")
                console.print(prometheus_metrics)
        
        else:  # json format
            # Get all metrics
            all_metrics = metrics.get_metrics()
            
            if output_file:
                import json
                with open(output_file, 'w') as f:
                    json.dump(all_metrics, f, indent=2, default=str)
                console.print(f"✅ JSON metrics written to {output_file}")
            else:
                import json
                console.print("[bold cyan]Current Metrics[/bold cyan]")
                console.print(json.dumps(all_metrics, indent=2, default=str))
        
    except Exception as e:
        console.print(f"❌ [red]Failed to retrieve metrics: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def monitor(
    start: bool = typer.Option(False, "--start", help="Start monitoring server"),
    stop: bool = typer.Option(False, "--stop", help="Stop monitoring server"),
    port: Optional[int] = typer.Option(None, "--port", help="Override monitoring server port")
) -> None:
    """Manage monitoring server for health checks and metrics."""
    
    async def manage_monitoring() -> None:
        from .monitoring import start_monitoring, stop_monitoring, monitoring_server
        
        settings = get_settings()
        
        if not settings.monitoring.enable_health_checks:
            console.print("❌ [red]Health checks are disabled in configuration[/red]")
            console.print("Enable with: MONITORING_ENABLE_HEALTH_CHECKS=true")
            raise typer.Exit(1)
        
        if start:
            console.print(f"🚀 [bold blue]Starting Monitoring Server[/bold blue]")
            
            if port:
                # Temporarily override port
                settings.monitoring.health_check_port = port
            
            try:
                await start_monitoring()
                console.print(f"✅ [green]Monitoring server started on port {settings.monitoring.health_check_port}[/green]")
                console.print(f"Health endpoint: http://localhost:{settings.monitoring.health_check_port}/health")
                console.print(f"Metrics endpoint: http://localhost:{settings.monitoring.health_check_port}/metrics")
                console.print(f"Readiness probe: http://localhost:{settings.monitoring.health_check_port}/health/ready")
                console.print(f"Liveness probe: http://localhost:{settings.monitoring.health_check_port}/health/live")
                
                # Keep server running
                console.print("\n[yellow]Press Ctrl+C to stop the server[/yellow]")
                try:
                    while True:
                        await asyncio.sleep(1)
                except KeyboardInterrupt:
                    console.print("\n🛑 [yellow]Stopping monitoring server...[/yellow]")
                    await stop_monitoring()
                    console.print("✅ [green]Monitoring server stopped[/green]")
                    
            except Exception as e:
                console.print(f"❌ [red]Failed to start monitoring server: {e}[/red]")
                raise typer.Exit(1)
        
        elif stop:
            console.print("🛑 [bold blue]Stopping Monitoring Server[/bold blue]")
            try:
                await stop_monitoring()
                console.print("✅ [green]Monitoring server stopped[/green]")
            except Exception as e:
                console.print(f"❌ [red]Failed to stop monitoring server: {e}[/red]")
                raise typer.Exit(1)
        
        else:
            console.print("📊 [bold blue]Monitoring Server Status[/bold blue]")
            
            # Show current configuration
            console.print(f"Health checks: {'✅ Enabled' if settings.monitoring.enable_health_checks else '❌ Disabled'}")
            console.print(f"Metrics: {'✅ Enabled' if settings.monitoring.enable_metrics else '❌ Disabled'}")
            console.print(f"Health check port: {settings.monitoring.health_check_port}")
            console.print(f"Metrics port: {settings.monitoring.metrics_port}")
            
            console.print("\n[bold cyan]Available Commands[/bold cyan]")
            console.print("  nba-scraper monitor --start    # Start monitoring server")
            console.print("  nba-scraper monitor --stop     # Stop monitoring server")
            console.print("  nba-scraper health             # Run health checks")
            console.print("  nba-scraper metrics            # View current metrics")
    
    try:
        asyncio.run(manage_monitoring())
    except KeyboardInterrupt:
        console.print("\n⚠️  [yellow]Monitoring command interrupted[/yellow]")
    except Exception as e:
        console.print(f"❌ [red]Monitoring command failed: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def alerts(
    test: bool = typer.Option(False, "--test", help="Send test alerts to configured channels"),
    list_config: bool = typer.Option(False, "--list", help="Show alert configuration")
) -> None:
    """Manage alerting configuration and test alerts."""
    
    async def manage_alerts() -> None:
        from .logging import alert_manager
        
        settings = get_settings()
        
        if list_config:
            console.print("🔔 [bold blue]Alert Configuration[/bold blue]")
            
            alert_table = Table(show_header=False)
            alert_table.add_column("Service", style="cyan")
            alert_table.add_column("Status", style="green")
            
            # Check alert configurations
            slack_configured = bool(settings.api_keys.slack_webhook_url)
            pagerduty_configured = bool(settings.api_keys.pagerduty_integration_key)
            sentry_configured = bool(settings.api_keys.sentry_dsn)
            
            alert_table.add_row("Slack", "✅ Configured" if slack_configured else "❌ Not configured")
            alert_table.add_row("PagerDuty", "✅ Configured" if pagerduty_configured else "❌ Not configured")
            alert_table.add_row("Sentry", "✅ Configured" if sentry_configured else "❌ Not configured")
            
            console.print(alert_table)
            
            if not any([slack_configured, pagerduty_configured, sentry_configured]):
                console.print("\n[yellow]No alert channels configured[/yellow]")
                console.print("Configure alerts by setting:")
                console.print("  API_SLACK_WEBHOOK_URL=https://hooks.slack.com/...")
                console.print("  API_PAGERDUTY_INTEGRATION_KEY=your_key")
                console.print("  API_SENTRY_DSN=https://your_dsn@sentry.io/...")
        
        elif test:
            console.print("🧪 [bold blue]Testing Alert Channels[/bold blue]")
            
            try:
                # Send test alerts
                await alert_manager.send_alert(
                    "Test Alert",
                    "This is a test alert from the NBA Scraper CLI",
                    "info",
                    {
                        "test": True,
                        "environment": settings.environment.value,
                        "timestamp": datetime.utcnow().isoformat()
                    }
                )
                
                console.print("✅ [green]Test alerts sent successfully[/green]")
                console.print("Check your configured alert channels for the test message")
                
            except Exception as e:
                console.print(f"❌ [red]Failed to send test alerts: {e}[/red]")
                raise typer.Exit(1)
        
        else:
            console.print("🔔 [bold blue]NBA Scraper Alerts[/bold blue]")
            console.print("Use --list to show configuration or --test to send test alerts")
    
    try:
        asyncio.run(manage_alerts())
    except Exception as e:
        console.print(f"❌ [red]Alerts command failed: {e}[/red]")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()