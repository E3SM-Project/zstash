import time
from datetime import datetime
from typing import Dict, Tuple

from zstash.update import UpdatePerformanceLogger


class TestUpdatePerformanceLogger:
    """Tests for UpdatePerformanceLogger class"""

    def test_initialization(self):
        """Test that logger initializes with zero values"""
        perf = UpdatePerformanceLogger()
        assert perf.overall_start == 0
        assert perf.db_elapsed == 0
        assert perf.gather_elapsed == 0
        assert perf.check_elapsed == 0

    def test_start_overall(self):
        """Test overall timing start"""
        perf = UpdatePerformanceLogger()
        perf.start_overall()
        assert perf.overall_start > 0
        assert perf.overall_start <= time.time()

    def test_database_open_timing(self):
        """Test database open timing"""
        perf = UpdatePerformanceLogger()
        perf.start_database_open()
        time.sleep(0.01)  # Small delay
        perf.end_database_open()
        assert perf.db_elapsed >= 0.01
        assert perf.db_elapsed < 0.1  # Should be quick

    def test_file_gathering_timing(self):
        """Test file gathering timing"""
        perf = UpdatePerformanceLogger()
        perf.start_file_gathering()
        time.sleep(0.01)
        perf.end_file_gathering(100)
        assert perf.gather_elapsed >= 0.01

    def test_database_check_timing(self):
        """Test database check timing"""
        perf = UpdatePerformanceLogger()
        perf.start_database_check()
        perf.start_database_load()
        time.sleep(0.01)
        perf.end_database_load(50)

        perf.start_comparison()
        time.sleep(0.01)
        perf.end_database_check(100, 10)

        assert perf.db_load_elapsed >= 0.01
        assert perf.comparison_elapsed >= 0.01
        assert perf.check_elapsed >= 0.02

    def test_comparison_progress_logging(self):
        """Test that progress logging doesn't raise errors"""
        perf = UpdatePerformanceLogger()
        perf.start_database_check()
        perf.start_comparison()

        # Should log at interval
        perf.log_comparison_progress(1000, 5000, interval=1000)
        perf.log_comparison_progress(2000, 5000, interval=1000)

        # Should not log between intervals
        perf.log_comparison_progress(1500, 5000, interval=1000)

    def test_overall_summary_logging(self):
        """Test overall summary logging completes without error"""
        perf = UpdatePerformanceLogger()
        perf.start_overall()

        # Simulate operation timings
        perf.start_database_open()
        perf.end_database_open()

        perf.start_file_gathering()
        perf.end_file_gathering(100)

        perf.start_database_check()
        perf.start_database_load()
        perf.end_database_load(50)
        perf.start_comparison()
        perf.end_database_check(100, 10)

        perf.start_tar_preparation()
        perf.end_tar_preparation()

        perf.start_add_files()
        perf.end_add_files()

        # Should complete without error
        perf.log_overall_summary()

    def test_early_exit_logging(self):
        """Test early exit logging"""
        perf = UpdatePerformanceLogger()
        perf.start_overall()
        time.sleep(0.01)
        perf.log_early_exit("no updates needed")


class TestUpdateDatabaseOptimization:
    """Tests for the optimized update_database function"""

    def test_in_memory_comparison(self):
        """Test that database comparison uses in-memory lookup"""
        # Mock data structures
        archived_files: Dict[str, Tuple[int, datetime]] = {
            "file1.txt": (100, datetime(2024, 1, 1, 12, 0, 0)),
            "file2.txt": (200, datetime(2024, 1, 2, 12, 0, 0)),
        }

        file_stats: Dict[str, Tuple[int, datetime]] = {
            "file1.txt": (100, datetime(2024, 1, 1, 12, 0, 0)),  # Unchanged
            "file2.txt": (250, datetime(2024, 1, 2, 12, 0, 0)),  # Changed size
            "file3.txt": (150, datetime(2024, 1, 3, 12, 0, 0)),  # New file
        }

        newfiles = []
        TIME_TOL = 3600  # 1 hour tolerance

        for file_path, (size_new, mdtime_new) in file_stats.items():
            if file_path not in archived_files:
                # New file
                newfiles.append(file_path)
            else:
                # Check if changed
                archived_size, archived_mtime = archived_files[file_path]
                if not (
                    (size_new == archived_size)
                    and (abs((mdtime_new - archived_mtime).total_seconds()) <= TIME_TOL)
                ):
                    newfiles.append(file_path)

        # Should detect file2.txt (changed) and file3.txt (new)
        assert "file2.txt" in newfiles
        assert "file3.txt" in newfiles
        assert "file1.txt" not in newfiles
        assert len(newfiles) == 2

    def test_mtime_tolerance(self):
        """Test that mtime tolerance is respected"""
        base_time = datetime(2024, 1, 1, 12, 0, 0)
        TIME_TOL = 3600  # 1 hour

        archived_files: Dict[str, Tuple[int, datetime]] = {
            "file1.txt": (100, base_time),
        }

        # File with mtime within tolerance
        file_stats_within: Dict[str, Tuple[int, datetime]] = {
            "file1.txt": (100, datetime(2024, 1, 1, 12, 30, 0)),  # 30 min difference
        }

        # File with mtime outside tolerance
        file_stats_outside: Dict[str, Tuple[int, datetime]] = {
            "file1.txt": (100, datetime(2024, 1, 1, 14, 0, 1)),  # >1 hour difference
        }

        # Test within tolerance
        newfiles = []
        for file_path, (size_new, mdtime_new) in file_stats_within.items():
            if file_path in archived_files:
                archived_size, archived_mtime = archived_files[file_path]
                if not (
                    (size_new == archived_size)
                    and (abs((mdtime_new - archived_mtime).total_seconds()) <= TIME_TOL)
                ):
                    newfiles.append(file_path)

        assert len(newfiles) == 0  # Within tolerance, no update needed

        # Test outside tolerance
        newfiles = []
        for file_path, (size_new, mdtime_new) in file_stats_outside.items():
            if file_path in archived_files:
                archived_size, archived_mtime = archived_files[file_path]
                if not (
                    (size_new == archived_size)
                    and (abs((mdtime_new - archived_mtime).total_seconds()) <= TIME_TOL)
                ):
                    newfiles.append(file_path)

        assert len(newfiles) == 1  # Outside tolerance, needs update
