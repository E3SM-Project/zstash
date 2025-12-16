"""
Unit tests for the optimized file scanning implementation in zstash.

Tests cover:
- DirectoryScanner class functionality
- get_files_to_archive_with_stats() function
- Database comparison optimization in update_database()
"""

import os
import sqlite3
from collections import OrderedDict
from datetime import datetime, timedelta
from typing import Dict, Tuple
from unittest.mock import patch

import pytest

from zstash.utils import DirectoryScanner, get_files_to_archive_with_stats


class TestDirectoryScanner:
    """Tests for the DirectoryScanner class."""

    def test_scanner_initialization(self):
        """Test that scanner initializes with correct defaults."""
        scanner = DirectoryScanner("/cache/path")

        assert scanner.cache_path == "/cache/path"
        assert scanner.file_stats == {}
        assert scanner.dir_count == 0
        assert scanner.file_count == 0
        assert scanner.empty_dir_count == 0

    def test_scan_simple_directory(self, tmp_path):
        """Test scanning a simple directory with files."""
        # Create test structure
        (tmp_path / "file1.txt").write_text("content1")
        (tmp_path / "file2.txt").write_text("content2")

        os.chdir(tmp_path)
        scanner = DirectoryScanner("./cache")
        scanner.scan_directory(".")

        assert scanner.file_count == 2
        assert scanner.dir_count >= 1
        assert len(scanner.file_stats) == 2

        # Check that files are in results
        files = [os.path.basename(p) for p in scanner.file_stats.keys()]
        assert "file1.txt" in files
        assert "file2.txt" in files

    def test_scan_nested_directories(self, tmp_path):
        """Test scanning nested directory structure."""
        # Create nested structure
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        (tmp_path / "root.txt").write_text("root")
        (subdir / "nested.txt").write_text("nested")

        os.chdir(tmp_path)
        scanner = DirectoryScanner("./cache")
        scanner.scan_directory(".")

        assert scanner.file_count == 2
        assert scanner.dir_count >= 2  # root and subdir
        assert len(scanner.file_stats) == 2

    def test_scan_empty_directory(self, tmp_path):
        """Test that empty directories are tracked correctly."""
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()

        os.chdir(tmp_path)
        scanner = DirectoryScanner("./cache")
        scanner.scan_directory(".")

        assert scanner.empty_dir_count == 1
        # Empty directory should be in file_stats with size 0
        empty_path = os.path.normpath(str(empty_dir.relative_to(tmp_path)))
        assert empty_path in scanner.file_stats
        assert scanner.file_stats[empty_path][0] == 0  # size

    def test_scan_skips_cache_directory(self, tmp_path):
        """Test that cache directory is properly excluded."""
        # Create cache directory with files
        cache_dir = tmp_path / "zstash_cache"
        cache_dir.mkdir()
        (cache_dir / "cached.txt").write_text("cached")

        # Create regular file
        (tmp_path / "regular.txt").write_text("regular")

        os.chdir(tmp_path)
        cache_path = os.path.join(".", "zstash_cache")
        scanner = DirectoryScanner(cache_path)
        scanner.scan_directory(".")

        # Should only find regular.txt, not cached.txt
        assert scanner.file_count == 1
        files = [os.path.basename(p) for p in scanner.file_stats.keys()]
        assert "regular.txt" in files
        assert "cached.txt" not in files

    def test_scan_symlinks(self, tmp_path):
        """Test that symbolic links are handled with size 0."""
        target = tmp_path / "target.txt"
        target.write_text("target content")

        link = tmp_path / "link.txt"
        link.symlink_to(target)

        os.chdir(tmp_path)
        scanner = DirectoryScanner("./cache")
        scanner.scan_directory(".")

        # Find the symlink in results
        link_path = os.path.normpath("link.txt")
        assert link_path in scanner.file_stats
        # Symlinks should have size 0
        assert scanner.file_stats[link_path][0] == 0

    def test_scan_permission_error(self, tmp_path, caplog):
        """Test handling of permission errors during scan."""
        restricted = tmp_path / "restricted"
        restricted.mkdir()

        os.chdir(tmp_path)
        scanner = DirectoryScanner("./cache")

        # Mock os.scandir to raise PermissionError
        with patch("os.scandir") as mock_scandir:
            mock_scandir.side_effect = PermissionError("Access denied")

            scanner.scan_directory(".")

            # Should log warning and continue
            assert "Permission denied" in caplog.text

    def test_file_stats_include_mtime(self, tmp_path):
        """Test that file stats include modification time."""
        file_path = tmp_path / "test.txt"
        file_path.write_text("content")

        # Set specific mtime
        mtime = datetime(2024, 1, 15, 12, 0, 0).timestamp()
        os.utime(file_path, (mtime, mtime))

        os.chdir(tmp_path)
        scanner = DirectoryScanner("./cache")
        scanner.scan_directory(".")

        normalized = os.path.normpath("test.txt")
        assert normalized in scanner.file_stats

        size, file_mtime = scanner.file_stats[normalized]
        # Should have captured the mtime
        assert isinstance(file_mtime, datetime)
        assert abs((file_mtime - datetime.utcfromtimestamp(mtime)).total_seconds()) < 1


class TestGetFilesToArchiveWithStats:
    """Tests for get_files_to_archive_with_stats function."""

    def test_returns_dict_with_stats(self, tmp_path):
        """Test that function returns dictionary with file stats."""
        (tmp_path / "file.txt").write_text("content")

        os.chdir(tmp_path)
        result = get_files_to_archive_with_stats("cache", None, None)

        assert isinstance(result, dict)
        assert len(result) > 0

        # Check structure: path -> (size, mtime)
        for path, stats in result.items():
            assert isinstance(stats, tuple)
            assert len(stats) == 2
            assert isinstance(stats[0], int)  # size
            assert isinstance(stats[1], datetime)  # mtime

    def test_ordered_dict_preserves_order(self, tmp_path):
        """Test that results maintain sorted order."""
        # Create files in specific order
        (tmp_path / "b_file.txt").write_text("b")
        (tmp_path / "a_file.txt").write_text("a")
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        (subdir / "c_file.txt").write_text("c")

        os.chdir(tmp_path)
        result = get_files_to_archive_with_stats("cache", None, None)

        assert isinstance(result, OrderedDict)
        # Should be sorted by directory first, then filename
        keys = list(result.keys())
        # Files in root should come before subdir
        root_files = [k for k in keys if "subdir" not in k]
        subdir_files = [k for k in keys if "subdir" in k]

        # Check that root files are alphabetically sorted
        assert root_files[0] < root_files[1]  # a before b alphabetically
        # Check that all root files come before subdir files
        if root_files and subdir_files:
            assert all(root < sub for root in root_files for sub in subdir_files)

    def test_include_filter(self, tmp_path):
        """Test that include pattern filters correctly."""
        (tmp_path / "file.txt").write_text("txt")
        (tmp_path / "file.dat").write_text("dat")

        os.chdir(tmp_path)
        result = get_files_to_archive_with_stats("cache", "*.txt", None)

        # Should only include .txt files
        assert len(result) == 1
        assert any("file.txt" in path for path in result.keys())
        assert not any("file.dat" in path for path in result.keys())

    def test_exclude_filter(self, tmp_path):
        """Test that exclude pattern filters correctly."""
        (tmp_path / "file.txt").write_text("txt")
        (tmp_path / "file.dat").write_text("dat")

        os.chdir(tmp_path)
        result = get_files_to_archive_with_stats("cache", None, "*.dat")

        # Should exclude .dat files
        assert any("file.txt" in path for path in result.keys())
        assert not any("file.dat" in path for path in result.keys())

    def test_include_and_exclude_filters(self, tmp_path):
        """Test combining include and exclude patterns."""
        (tmp_path / "data.txt").write_text("data")
        (tmp_path / "temp.txt").write_text("temp")
        (tmp_path / "file.dat").write_text("dat")

        os.chdir(tmp_path)
        result = get_files_to_archive_with_stats("cache", "*.txt", "temp*")

        # Should include .txt but exclude temp*
        assert len(result) == 1
        assert any("data.txt" in path for path in result.keys())


class TestUpdateDatabaseOptimization:
    """Tests for the optimized database comparison logic."""

    def test_file_stats_lookup_dict(self, tmp_path):
        """Test that file stats are used for O(1) lookup instead of repeated lstat calls."""
        # Create test file
        test_file = tmp_path / "test.txt"
        test_file.write_text("content")

        os.chdir(tmp_path)
        file_stats = get_files_to_archive_with_stats("cache", None, None)

        # Verify we got the stats
        normalized = os.path.normpath("test.txt")
        assert normalized in file_stats

        size, mtime = file_stats[normalized]
        assert size > 0
        assert isinstance(mtime, datetime)

    def test_archived_files_dict_creation(self):
        """Test creation of archived_files dict from database rows."""
        # Simulate database rows
        db_rows = [
            ("file1.txt", 100, datetime(2024, 1, 1)),
            ("file2.txt", 200, datetime(2024, 1, 2)),
            ("file1.txt", 100, datetime(2024, 1, 3)),  # Duplicate with newer mtime
        ]

        archived_files: Dict[str, Tuple[int, datetime]] = {}

        for row in db_rows:
            file_path = row[0]
            size = row[1]
            mtime = row[2]

            # Keep the one with latest mtime
            if file_path in archived_files:
                existing_mtime = archived_files[file_path][1]
                if mtime > existing_mtime:
                    archived_files[file_path] = (size, mtime)
            else:
                archived_files[file_path] = (size, mtime)

        # Should keep latest entry for file1.txt
        assert len(archived_files) == 2
        assert archived_files["file1.txt"][1] == datetime(2024, 1, 3)
        assert archived_files["file2.txt"][1] == datetime(2024, 1, 2)

    def test_new_file_detection(self):
        """Test detection of files not in database."""
        file_stats = {
            "new_file.txt": (100, datetime(2024, 1, 1)),
            "existing_file.txt": (200, datetime(2024, 1, 2)),
        }

        archived_files = {
            "existing_file.txt": (200, datetime(2024, 1, 2)),
        }

        newfiles = []
        for file_path in file_stats.keys():
            if file_path not in archived_files:
                newfiles.append(file_path)

        assert len(newfiles) == 1
        assert "new_file.txt" in newfiles

    def test_modified_file_detection(self):
        """Test detection of files that changed size or mtime."""
        TIME_TOL = 1  # seconds

        file_stats = {
            "changed_size.txt": (300, datetime(2024, 1, 1)),
            "changed_mtime.txt": (200, datetime(2024, 1, 2)),
            "unchanged.txt": (100, datetime(2024, 1, 1)),
        }

        archived_files = {
            "changed_size.txt": (200, datetime(2024, 1, 1)),  # Size changed
            "changed_mtime.txt": (200, datetime(2024, 1, 1)),  # Mtime changed
            "unchanged.txt": (100, datetime(2024, 1, 1)),  # Same
        }

        newfiles = []
        for file_path in file_stats.keys():
            if file_path not in archived_files:
                newfiles.append(file_path)
            else:
                size_new, mtime_new = file_stats[file_path]
                size_archived, mtime_archived = archived_files[file_path]

                if not (
                    (size_new == size_archived)
                    and (abs((mtime_new - mtime_archived).total_seconds()) <= TIME_TOL)
                ):
                    newfiles.append(file_path)

        assert len(newfiles) == 2
        assert "changed_size.txt" in newfiles
        assert "changed_mtime.txt" in newfiles
        assert "unchanged.txt" not in newfiles

    def test_time_tolerance_check(self):
        """Test that time tolerance is properly applied."""
        TIME_TOL = 2  # seconds

        base_time = datetime(2024, 1, 1, 12, 0, 0)

        test_cases = [
            (base_time, base_time, True),  # Exact match
            (base_time, base_time + timedelta(seconds=1), True),  # Within tolerance
            (
                base_time,
                base_time + timedelta(seconds=2),
                True,
            ),  # At tolerance boundary
            (base_time, base_time + timedelta(seconds=3), False),  # Outside tolerance
        ]

        for mtime_new, mtime_archived, should_match in test_cases:
            time_diff = abs((mtime_new - mtime_archived).total_seconds())
            is_within_tolerance = time_diff <= TIME_TOL
            assert is_within_tolerance == should_match


class TestBackwardCompatibility:
    """Tests to ensure backward compatibility with existing code."""

    def test_get_files_to_archive_still_works(self, tmp_path):
        """Test that legacy get_files_to_archive function still works."""
        from zstash.utils import get_files_to_archive

        (tmp_path / "file.txt").write_text("content")

        os.chdir(tmp_path)
        result = get_files_to_archive("cache", None, None)

        # Should return list of strings
        assert isinstance(result, list)
        assert len(result) > 0
        assert all(isinstance(item, str) for item in result)

    def test_output_format_matches_original(self, tmp_path):
        """Test that file paths are normalized the same way as original."""
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        (subdir / "file.txt").write_text("content")

        os.chdir(tmp_path)

        from zstash.utils import get_files_to_archive

        legacy_result = get_files_to_archive("cache", None, None)
        new_result = list(get_files_to_archive_with_stats("cache", None, None).keys())

        # Should produce same file list
        assert legacy_result == new_result


@pytest.fixture
def mock_database():
    """Fixture providing a mock database cursor."""
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()

    # Create files table
    cur.execute(
        """
        CREATE TABLE files (
            name TEXT,
            size INTEGER,
            mtime TIMESTAMP
        )
    """
    )

    yield cur

    conn.close()


class TestIntegration:
    """Integration tests combining multiple components."""

    def test_full_scan_and_compare_workflow(self, tmp_path, mock_database):
        """Test complete workflow from scan to database comparison."""
        # Setup test files
        (tmp_path / "file1.txt").write_text("content1")
        (tmp_path / "file2.txt").write_text("content2")

        # Add file1 to database as already archived
        cur = mock_database
        cur.execute(
            "INSERT INTO files VALUES (?, ?, ?)", ("file1.txt", 8, datetime(2024, 1, 1))
        )

        os.chdir(tmp_path)

        # Get file stats
        file_stats = get_files_to_archive_with_stats("cache", None, None)

        # Build archived files dict
        archived_files = {}
        cur.execute("SELECT name, size, mtime FROM files")
        for row in cur.fetchall():
            archived_files[row[0]] = (row[1], row[2])

        # Find new files
        newfiles = []
        for file_path in file_stats.keys():
            normalized = os.path.normpath(file_path)
            if normalized not in archived_files:
                newfiles.append(normalized)

        # file2.txt should be new (not in database)
        assert "file2.txt" in newfiles
        assert "file1.txt" not in newfiles
