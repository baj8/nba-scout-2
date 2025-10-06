#!/usr/bin/env python3
"""Game rollback tool for safely removing games and their child records."""

import argparse
import asyncio
import sys
from datetime import datetime, UTC
from pathlib import Path
from typing import List, Dict, Any

from ..db import get_connection
from ..nba_logging import get_logger
from ..pipelines.foundation import _maybe_transaction

logger = get_logger(__name__)


class GameRollbackTool:
    """Tool for safely rolling back games and their child records."""
    
    def __init__(self, ops_dir: str = "./ops"):
        self.ops_dir = Path(ops_dir)
        self.ops_dir.mkdir(exist_ok=True)
    
    async def rollback_game(self, game_id: str, dry_run: bool = False) -> Dict[str, Any]:
        """Rollback a single game with comprehensive logging.
        
        Args:
            game_id: NBA game ID to rollback
            dry_run: If True, only show what would be deleted
            
        Returns:
            Dictionary with rollback results
        """
        logger.info(f"{'DRY RUN: ' if dry_run else ''}Rolling back game {game_id}")
        
        result = {
            'game_id': game_id,
            'dry_run': dry_run,
            'timestamp': datetime.now(UTC).isoformat(),
            'records_deleted': {},
            'success': False,
            'error': None
        }
        
        try:
            async with get_connection() as conn:
                # Check what exists before deletion
                counts_before = await self._get_record_counts(conn, game_id)
                result['records_before'] = counts_before
                
                if not any(counts_before.values()):
                    logger.info(f"Game {game_id} has no records to delete")
                    result['success'] = True
                    return result
                
                if dry_run:
                    logger.info(f"DRY RUN: Would delete {counts_before}")
                    result['success'] = True
                    result['records_deleted'] = counts_before
                    return result
                
                # Perform actual deletion in transaction
                async with _maybe_transaction(conn):
                    # Delete children first to avoid FK violations
                    deleted_counts = {}
                    
                    # PBP events
                    deleted_pbp = await conn.execute(
                        "DELETE FROM pbp_events WHERE game_id = $1", game_id
                    )
                    deleted_counts['pbp_events'] = self._extract_delete_count(deleted_pbp)
                    
                    # Lineup stints
                    deleted_lineups = await conn.execute(
                        "DELETE FROM lineup_stints WHERE game_id = $1", game_id
                    )
                    deleted_counts['lineup_stints'] = self._extract_delete_count(deleted_lineups)
                    
                    # Shots
                    deleted_shots = await conn.execute(
                        "DELETE FROM shots WHERE game_id = $1", game_id
                    )
                    deleted_counts['shots'] = self._extract_delete_count(deleted_shots)
                    
                    # Advanced metrics (if table exists)
                    try:
                        deleted_adv = await conn.execute(
                            "DELETE FROM adv_metrics WHERE game_id = $1", game_id
                        )
                        deleted_counts['adv_metrics'] = self._extract_delete_count(deleted_adv)
                    except Exception:
                        # Table might not exist
                        deleted_counts['adv_metrics'] = 0
                    
                    # Finally delete parent game
                    deleted_game = await conn.execute(
                        "DELETE FROM games WHERE game_id = $1", game_id
                    )
                    deleted_counts['games'] = self._extract_delete_count(deleted_game)
                
                result['records_deleted'] = deleted_counts
                result['success'] = True
                
                total_deleted = sum(deleted_counts.values())
                logger.info(f"✅ Rolled back game {game_id}: {total_deleted} records deleted")
                
        except Exception as e:
            logger.error(f"❌ Rollback failed for game {game_id}: {e}")
            result['error'] = str(e)
            result['success'] = False
        
        # Log the rollback
        await self._write_rollback_log(result)
        
        return result
    
    async def rollback_multiple_games(self, game_ids: List[str], dry_run: bool = False) -> Dict[str, Any]:
        """Rollback multiple games with batch logging."""
        logger.info(f"{'DRY RUN: ' if dry_run else ''}Rolling back {len(game_ids)} games")
        
        results = {
            'total_games': len(game_ids),
            'successful_rollbacks': 0,
            'failed_rollbacks': 0,
            'dry_run': dry_run,
            'timestamp': datetime.now(UTC).isoformat(),
            'total_records_deleted': {},
            'failed_games': []
        }
        
        for game_id in game_ids:
            try:
                game_result = await self.rollback_game(game_id, dry_run)
                
                if game_result['success']:
                    results['successful_rollbacks'] += 1
                    # Accumulate deleted record counts
                    for table, count in game_result.get('records_deleted', {}).items():
                        results['total_records_deleted'][table] = (
                            results['total_records_deleted'].get(table, 0) + count
                        )
                else:
                    results['failed_rollbacks'] += 1
                    results['failed_games'].append({
                        'game_id': game_id,
                        'error': game_result.get('error', 'Unknown error')
                    })
                
            except Exception as e:
                logger.error(f"Exception rolling back game {game_id}: {e}")
                results['failed_rollbacks'] += 1
                results['failed_games'].append({
                    'game_id': game_id,
                    'error': str(e)
                })
        
        # Write batch rollback log
        await self._write_batch_rollback_log(results)
        
        return results
    
    async def _get_record_counts(self, conn, game_id: str) -> Dict[str, int]:
        """Get current record counts for a game across all tables."""
        counts = {}
        
        # Count games
        counts['games'] = await conn.fetchval(
            "SELECT COUNT(*) FROM games WHERE game_id = $1", game_id
        )
        
        # Count PBP events
        counts['pbp_events'] = await conn.fetchval(
            "SELECT COUNT(*) FROM pbp_events WHERE game_id = $1", game_id
        )
        
        # Count lineup stints
        counts['lineup_stints'] = await conn.fetchval(
            "SELECT COUNT(*) FROM lineup_stints WHERE game_id = $1", game_id
        )
        
        # Count shots
        counts['shots'] = await conn.fetchval(
            "SELECT COUNT(*) FROM shots WHERE game_id = $1", game_id
        )
        
        # Count advanced metrics (if table exists)
        try:
            counts['adv_metrics'] = await conn.fetchval(
                "SELECT COUNT(*) FROM adv_metrics WHERE game_id = $1", game_id
            )
        except Exception:
            counts['adv_metrics'] = 0
        
        return counts
    
    def _extract_delete_count(self, delete_result: str) -> int:
        """Extract number of deleted rows from DELETE result."""
        # PostgreSQL returns "DELETE n" where n is the count
        if isinstance(delete_result, str) and delete_result.startswith('DELETE '):
            try:
                return int(delete_result.split(' ')[1])
            except (IndexError, ValueError):
                return 0
        return 0
    
    async def _write_rollback_log(self, result: Dict[str, Any]) -> None:
        """Write individual game rollback to ops log."""
        log_file = self.ops_dir / "rollback_log.txt"
        
        deleted_summary = ", ".join([
            f"{table}: {count}" for table, count in result.get('records_deleted', {}).items()
            if count > 0
        ])
        
        log_entry = (
            f"[{result['timestamp']}] "
            f"{'DRY RUN ' if result['dry_run'] else ''}ROLLBACK {result['game_id']} | "
            f"Success: {result['success']} | "
            f"Deleted: {deleted_summary or 'None'}"
        )
        
        if result.get('error'):
            log_entry += f" | Error: {result['error']}"
        
        log_entry += "\n"
        
        with open(log_file, 'a') as f:
            f.write(log_entry)
    
    async def _write_batch_rollback_log(self, results: Dict[str, Any]) -> None:
        """Write batch rollback summary to ops log."""
        log_file = self.ops_dir / "batch_rollback_log.txt"
        
        total_deleted_summary = ", ".join([
            f"{table}: {count}" for table, count in results.get('total_records_deleted', {}).items()
            if count > 0
        ])
        
        log_content = f"""[{results['timestamp']}] {'DRY RUN ' if results['dry_run'] else ''}BATCH ROLLBACK SUMMARY
Games processed: {results['total_games']}
Successful: {results['successful_rollbacks']}
Failed: {results['failed_rollbacks']}
Total records deleted: {total_deleted_summary or 'None'}

Failed games:
"""
        
        for failed_game in results.get('failed_games', []):
            log_content += f"- {failed_game['game_id']}: {failed_game['error']}\n"
        
        log_content += "\n"
        
        with open(log_file, 'a') as f:
            f.write(log_content)


async def main():
    """CLI entry point for game rollback tool."""
    parser = argparse.ArgumentParser(description="Rollback NBA games and their child records")
    parser.add_argument("--game-id", help="Single game ID to rollback")
    parser.add_argument("--game-ids-file", help="File with game IDs (one per line)")
    parser.add_argument("--dry-run", action="store_true", 
                       help="Show what would be deleted without actually deleting")
    parser.add_argument("--ops-dir", default="./ops",
                       help="Directory for ops logs (default: ./ops)")
    
    args = parser.parse_args()
    
    if not args.game_id and not args.game_ids_file:
        parser.error("Must specify either --game-id or --game-ids-file")
    
    # Create rollback tool
    tool = GameRollbackTool(ops_dir=args.ops_dir)
    
    try:
        if args.game_id:
            # Single game rollback
            result = await tool.rollback_game(args.game_id, dry_run=args.dry_run)
            
            if result['success']:
                deleted = result.get('records_deleted', {})
                total = sum(deleted.values())
                print(f"✅ {'DRY RUN: Would delete' if args.dry_run else 'Deleted'} {total} records for game {args.game_id}")
                for table, count in deleted.items():
                    if count > 0:
                        print(f"   {table}: {count}")
            else:
                print(f"❌ Rollback failed for game {args.game_id}: {result.get('error', 'Unknown error')}")
                sys.exit(1)
        
        elif args.game_ids_file:
            # Multi-game rollback
            game_ids_file = Path(args.game_ids_file)
            if not game_ids_file.exists():
                print(f"❌ Game IDs file not found: {game_ids_file}")
                sys.exit(1)
            
            with open(game_ids_file, 'r') as f:
                game_ids = [line.strip() for line in f if line.strip() and not line.startswith('#')]
            
            if not game_ids:
                print(f"❌ No game IDs found in file: {game_ids_file}")
                sys.exit(1)
            
            result = await tool.rollback_multiple_games(game_ids, dry_run=args.dry_run)
            
            successful = result['successful_rollbacks']
            failed = result['failed_rollbacks']
            total_deleted = sum(result.get('total_records_deleted', {}).values())
            
            print(f"✅ Batch rollback complete: {successful} successful, {failed} failed")
            if total_deleted > 0:
                print(f"   {'Would delete' if args.dry_run else 'Deleted'} {total_deleted} total records")
            
            if failed > 0:
                print(f"⚠️  {failed} games failed - check ops logs")
                sys.exit(1)
        
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Rollback tool failed: {e}")
        print(f"❌ Rollback tool failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())