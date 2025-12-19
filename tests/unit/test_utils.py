import os
import tempfile
import time
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from zstash.utils import (
    DirectoryScanner,
    FileGatheringPerformanceLogger,
    get_files_to_archive_with_stats,
)


class TestFileGatheringPerformanceLogger:
    """Tests for FileGatheringPerformanceLogger class"""

    @patch("zstash.utils.logger")
    def test_log_scandir_progress(self, mock_logger):
        """Test scandir progress logging"""
        perf = FileGatheringPerformanceLogger()

        perf.log_scandir_progress(1000, 5000, 10.5)
        assert mock_logger.debug.called

    @patch("zstash.utils.logger")
    def test_log_scandir_complete(self, mock_logger):
        """Test scandir completion logging"""
        perf = FileGatheringPerformanceLogger()

        perf.log_scandir_complete(100, 500, 10, 5.5)
        assert mock_logger.debug.call_count >= 5

    @patch("zstash.utils.logger")
    def test_log_filter(self, mock_logger):
        """Test filter logging"""
        perf = FileGatheringPerformanceLogger()

        perf.log_filter("include", "*.py", 0.5, 100, 50)
        assert mock_logger.debug.call_count >= 2


class TestDirectoryScanner:
    """Tests for DirectoryScanner class"""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory structure for testing"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test structure
            base = Path(tmpdir)

            # Regular files
            (base / "file1.txt").write_text("content1")
            (base / "file2.txt").write_text("content2")

            # Subdirectory with files
            subdir = base / "subdir"
            subdir.mkdir()
            (subdir / "file3.txt").write_text("content3")

            # Empty directory
            empty = base / "empty"
            empty.mkdir()

            # Cache directory (should be excluded)
            cache = base / "cache"
            cache.mkdir()
            (cache / "cached.txt").write_text("cached")

            # Symlink
            symlink = base / "link.txt"
            symlink.symlink_to(base / "file1.txt")

            yield tmpdir

    def test_scan_basic_structure(self, temp_dir):
        """Test scanning basic directory structure"""
        perf_logger = FileGatheringPerformanceLogger()

        cache_path = os.path.join(temp_dir, "cache")
        scanner = DirectoryScanner(cache_path, perf_logger, time.time())

        # Change to temp dir and scan
        original_dir = os.getcwd()
        try:
            os.chdir(temp_dir)
            scanner.scan_directory(".")

            # Should find files but not cache contents
            assert scanner.file_count >= 3  # file1, file2, file3, link
            assert scanner.dir_count >= 2  # root, subdir, empty
            assert scanner.empty_dir_count >= 1  # empty dir

            # Cache file should not be in results
            cache_file = os.path.normpath(os.path.join(cache_path, "cached.txt"))
            assert cache_file not in scanner.file_stats

            # Regular files should be in results
            file1 = os.path.normpath(os.path.join(".", "file1.txt"))
            assert file1 in scanner.file_stats or "./file1.txt" in scanner.file_stats

        finally:
            os.chdir(original_dir)


class TestGetFilesToArchiveWithStats:
    """Tests for get_files_to_archive_with_stats function"""

    @pytest.fixture
    def temp_archive_dir(self):
        """Create a temporary directory with files to archive"""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)

            # Create various files
            (base / "data.txt").write_text("data")
            (base / "script.py").write_text("print('hello')")
            (base / "config.json").write_text('{"key": "value"}')

            # Create subdirectory
            subdir = base / "logs"
            subdir.mkdir()
            (subdir / "log1.txt").write_text("log entry 1")
            (subdir / "log2.txt").write_text("log entry 2")

            # Create cache directory
            cache = base / "zstash_cache"
            cache.mkdir()
            (cache / "index.db").write_text("database")

            yield tmpdir

    def test_basic_file_gathering(self, temp_archive_dir):
        """Test basic file gathering with stats"""
        original_dir = os.getcwd()
        try:
            os.chdir(temp_archive_dir)

            result = get_files_to_archive_with_stats("zstash_cache", None, None)

            # Should return dict mapping paths to (size, mtime) tuples
            assert isinstance(result, dict)
            assert len(result) > 0

            # Check structure of results
            for path, stats in result.items():
                assert isinstance(path, str)
                assert isinstance(stats, tuple)
                assert len(stats) == 2
                size, mtime = stats
                assert isinstance(size, int)
                assert isinstance(mtime, datetime)

            # Cache directory files should be excluded
            for path in result.keys():
                assert "zstash_cache" not in path

        finally:
            os.chdir(original_dir)

    def test_include_pattern(self, temp_archive_dir):
        """Test include pattern filtering"""
        original_dir = os.getcwd()
        try:
            os.chdir(temp_archive_dir)

            # Only include .txt files
            result = get_files_to_archive_with_stats("zstash_cache", "*.txt", None)

            # All results should be .txt files
            for path in result.keys():
                if path and not path.endswith("/"):  # Not empty dir
                    assert path.endswith(".txt")

        finally:
            os.chdir(original_dir)

    def test_exclude_pattern(self, temp_archive_dir):
        """Test exclude pattern filtering"""
        original_dir = os.getcwd()
        try:
            os.chdir(temp_archive_dir)

            # Exclude .py files
            result = get_files_to_archive_with_stats("zstash_cache", None, "*.py")

            # No .py files should be in results
            for path in result.keys():
                assert not path.endswith(".py")

        finally:
            os.chdir(original_dir)

    def test_include_and_exclude(self, temp_archive_dir):
        """Test both include and exclude patterns"""
        original_dir = os.getcwd()
        try:
            os.chdir(temp_archive_dir)

            # Include all .txt, but exclude logs
            result = get_files_to_archive_with_stats("zstash_cache", "*.txt", "logs/*")

            # Should have .txt files but not from logs directory
            for path in result.keys():
                if path and not path.endswith("/"):
                    assert path.endswith(".txt")
                    assert "logs" not in path

        finally:
            os.chdir(original_dir)

    def test_returns_ordered_dict(self, temp_archive_dir):
        """Test that results maintain sorted order"""
        original_dir = os.getcwd()
        try:
            os.chdir(temp_archive_dir)

            result = get_files_to_archive_with_stats("zstash_cache", None, None)

            # Keys should be in sorted order
            keys = list(result.keys())
            sorted_keys = sorted(keys)
            assert keys == sorted_keys

        finally:
            os.chdir(original_dir)

    def test_empty_directory_handling(self, temp_archive_dir):
        """Test handling of empty directories"""
        original_dir = os.getcwd()
        try:
            os.chdir(temp_archive_dir)

            # Create an empty directory
            empty_dir = Path(temp_archive_dir) / "empty_folder"
            empty_dir.mkdir()

            result = get_files_to_archive_with_stats("zstash_cache", None, None)

            # Empty directory should be in results with size 0
            empty_path = os.path.normpath("./empty_folder")
            if empty_path in result:
                size, mtime = result[empty_path]
                assert size == 0
                assert isinstance(mtime, datetime)

        finally:
            os.chdir(original_dir)
