#!/usr/bin/env python3
"""Validation script for the NBA Historical Backfill workflow."""

import asyncio
import sys
from pathlib import Path
from typing import Dict, Any, List

from nba_scraper.tools import SingleGameRunner, GameRollbackTool
from nba_scraper.pipelines.backfill import BackfillOrchestrator
from nba_scraper.nba_logging import get_logger

logger = get_logger(__name__)


class BackfillWorkflowValidator:
    """Validates the complete backfill workflow is ready for production."""
    
    def __init__(self):
        self.test_results = []
    
    def log_test(self, test_name: str, success: bool, details: str = "") -> None:
        """Log a test result."""
        status = "✅ PASS" if success else "❌ FAIL"
        message = f"{status}: {test_name}"
        if details:
            message += f" - {details}"
        
        print(message)
        self.test_results.append({
            'test': test_name,
            'success': success,
            'details': details
        })
    
    async def validate_all(self) -> bool:
        """Run all validation tests."""
        print("🔍 NBA Historical Backfill Workflow - Validation")
        print("=" * 60)
        
        # Test 1: Validate directory structure
        await self._test_directory_structure()
        
        # Test 2: Validate runbook exists
        await self._test_runbook_exists()
        
        # Test 3: Validate SQL files exist
        await self._test_sql_files()
        
        # Test 4: Validate tool imports
        await self._test_tool_imports()
        
        # Test 5: Validate pipeline imports  
        await self._test_pipeline_imports()
        
        # Test 6: Test tool initialization (dry run)
        await self._test_tool_initialization()
        
        # Test 7: Test ops logging structure
        await self._test_ops_logging()
        
        # Test 8: Validate CLI argument parsing
        await self._test_cli_parsing()
        
        # Summary
        print("\n📊 Validation Summary:")
        print("=" * 60)
        
        passed = sum(1 for result in self.test_results if result['success'])
        total = len(self.test_results)
        
        for result in self.test_results:
            status = "✅" if result['success'] else "❌"
            print(f"{status} {result['test']}")
        
        print(f"\n📈 Results: {passed}/{total} tests passed ({passed/total*100:.1f}%)")
        
        if passed == total:
            print("\n🎉 ALL TESTS PASSED - Backfill workflow is ready!")
            return True
        else:
            print(f"\n⚠️  {total - passed} tests failed - Review issues above")
            return False
    
    async def _test_directory_structure(self) -> None:
        """Test that required directories exist or can be created."""
        required_dirs = [
            "docs/runbooks",
            "sql", 
            "ops",
            "src/nba_scraper/tools",
            "src/nba_scraper/pipelines"
        ]
        
        missing_dirs = []
        for dir_path in required_dirs:
            if not Path(dir_path).exists():
                missing_dirs.append(dir_path)
        
        if missing_dirs:
            self.log_test(
                "Directory Structure", 
                False, 
                f"Missing directories: {', '.join(missing_dirs)}"
            )
        else:
            self.log_test("Directory Structure", True, "All required directories exist")
    
    async def _test_runbook_exists(self) -> None:
        """Test that the backfill runbook exists and is complete."""
        runbook_path = Path("docs/runbooks/backfill_checklist.md")
        
        if not runbook_path.exists():
            self.log_test("Backfill Runbook", False, "Runbook file missing")
            return
        
        # Check for required sections
        with open(runbook_path, 'r') as f:
            content = f.read()
        
        required_sections = [
            "Pre-flight Readiness",
            "Single-Game Smoke Test", 
            "Pilot Week",
            "Batch/Season Backfill",
            "Raw Payload Persistence",
            "Ops Log Template",
            "Quick SQL Health Checks",
            "Rollback",
            "Troubleshooting",
            "Sign-off Criteria"
        ]
        
        missing_sections = [section for section in required_sections 
                           if section not in content]
        
        if missing_sections:
            self.log_test(
                "Backfill Runbook", 
                False, 
                f"Missing sections: {', '.join(missing_sections)}"
            )
        else:
            self.log_test("Backfill Runbook", True, "All required sections present")
    
    async def _test_sql_files(self) -> None:
        """Test that required SQL files exist."""
        sql_files = [
            "sql/health_checks.sql",
            "sql/rollback_game.sql"
        ]
        
        missing_files = []
        for sql_file in sql_files:
            if not Path(sql_file).exists():
                missing_files.append(sql_file)
        
        if missing_files:
            self.log_test(
                "SQL Files", 
                False, 
                f"Missing files: {', '.join(missing_files)}"
            )
        else:
            self.log_test("SQL Files", True, "All required SQL files exist")
    
    async def _test_tool_imports(self) -> None:
        """Test that tool modules can be imported."""
        try:
            from nba_scraper.tools import SingleGameRunner, GameRollbackTool
            self.log_test("Tool Imports", True, "SingleGameRunner and GameRollbackTool")
        except ImportError as e:
            self.log_test("Tool Imports", False, f"Import error: {e}")
    
    async def _test_pipeline_imports(self) -> None:
        """Test that pipeline modules can be imported."""
        try:
            from nba_scraper.pipelines.backfill import BackfillOrchestrator
            self.log_test("Pipeline Imports", True, "BackfillOrchestrator")
        except ImportError as e:
            self.log_test("Pipeline Imports", False, f"Import error: {e}")
    
    async def _test_tool_initialization(self) -> None:
        """Test that tools can be initialized."""
        try:
            # Test SingleGameRunner
            runner = SingleGameRunner(raw_dir="./test_raw", log_level="INFO")
            
            # Test GameRollbackTool  
            rollback_tool = GameRollbackTool(ops_dir="./test_ops")
            
            # Test BackfillOrchestrator
            orchestrator = BackfillOrchestrator(
                raw_dir="./test_raw",
                ops_dir="./test_ops", 
                rate_limit=1.0,
                batch_size=10,
                dry_run=True
            )
            
            self.log_test("Tool Initialization", True, "All tools initialize successfully")
            
            # Cleanup test directories
            import shutil
            for test_dir in ["./test_raw", "./test_ops"]:
                if Path(test_dir).exists():
                    shutil.rmtree(test_dir)
                    
        except Exception as e:
            self.log_test("Tool Initialization", False, f"Initialization error: {e}")
    
    async def _test_ops_logging(self) -> None:
        """Test that ops logging structure works."""
        try:
            ops_dir = Path("./test_ops_validation")
            ops_dir.mkdir(exist_ok=True)
            
            # Test ops log file creation
            test_log = ops_dir / "test.log"
            with open(test_log, 'w') as f:
                f.write("Test ops log entry\n")
            
            # Test quarantine file creation  
            quarantine_file = ops_dir / "quarantine_game_ids.txt"
            with open(quarantine_file, 'w') as f:
                f.write("0022300001  # Test quarantine entry\n")
            
            # Verify files exist
            if test_log.exists() and quarantine_file.exists():
                self.log_test("Ops Logging", True, "Log file creation works")
            else:
                self.log_test("Ops Logging", False, "Failed to create log files")
            
            # Cleanup
            import shutil
            shutil.rmtree(ops_dir)
            
        except Exception as e:
            self.log_test("Ops Logging", False, f"Logging error: {e}")
    
    async def _test_cli_parsing(self) -> None:
        """Test CLI argument parsing for tools."""
        try:
            # This is a basic test - we can't easily test argparse without sys.argv manipulation
            # But we can check that the argument parsers are defined correctly
            
            # Import the main functions to verify they exist
            from nba_scraper.tools.run_single_game import main as single_game_main
            from nba_scraper.tools.rollback_game import main as rollback_main  
            from nba_scraper.pipelines.backfill import main as backfill_main
            
            self.log_test("CLI Parsing", True, "All CLI entry points accessible")
            
        except ImportError as e:
            self.log_test("CLI Parsing", False, f"CLI import error: {e}")


async def main():
    """Run the backfill workflow validation."""
    validator = BackfillWorkflowValidator()
    success = await validator.validate_all()
    
    if success:
        print("\n🚀 Backfill workflow is ready for pilot testing!")
        print("\nNext steps:")
        print("1. Run validate_patches.py to ensure core functionality")
        print("2. Test single game: python3 -m nba_scraper.tools.run_single_game --game-id 0022300001")
        print("3. Test dry-run backfill: python3 -m nba_scraper.pipelines.backfill --start 2024-01-15 --end 2024-01-22 --dry-run")
        sys.exit(0)
    else:
        print("\n❌ Workflow validation failed - fix issues above before proceeding")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())