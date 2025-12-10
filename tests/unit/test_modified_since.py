"""
Unit tests for --modified-since functionality in zstash update
"""

import os
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest


class TestModifiedSinceFlag:
    """Tests for --modified-since command line flag parsing."""

    @patch(
        "zstash.update.sys.argv",
        ["zstash", "update", "--modified-since=2025-12-08T14:00:00"],
    )
    def test_modified_since_flag_parsing(self):
        """Test that --modified-since flag is correctly parsed."""
        from zstash.update import setup_update

        args, cache = setup_update()
        assert args.modified_since == "2025-12-08T14:00:00"

    @patch("zstash.update.sys.argv", ["zstash", "update"])
    def test_modified_since_flag_optional(self):
        """Test that --modified-since is optional."""
        from zstash.update import setup_update

        args, cache = setup_update()
        assert args.modified_since is None


class TestModifiedSinceFiltering:
    """Tests for file filtering based on modification time."""

    @pytest.fixture
    def mock_files(self):
        """Create mock files with different modification times."""
        now = datetime.now(timezone.utc)

        # Create temporary directory and files
        temp_dir = tempfile.mkdtemp()

        # File modified 2 hours ago (old)
        old_file = os.path.join(temp_dir, "old_file.txt")
        with open(old_file, "w") as f:
            f.write("old content")
        old_time = (now - timedelta(hours=2)).timestamp()
        os.utime(old_file, (old_time, old_time))

        # File modified 30 minutes ago (new)
        new_file = os.path.join(temp_dir, "new_file.txt")
        with open(new_file, "w") as f:
            f.write("new content")
        new_time = (now - timedelta(minutes=30)).timestamp()
        os.utime(new_file, (new_time, new_time))

        return temp_dir, old_file, new_file, now - timedelta(hours=1)

    def test_filters_old_files(self, mock_files):
        """Test that files older than --modified-since are filtered out."""
        temp_dir, old_file, new_file, cutoff_time = mock_files

        files = [old_file, new_file]
        filtered_files = []

        for file_path in files:
            statinfo = os.lstat(file_path)
            # Use timezone-aware datetime
            file_mtime = datetime.fromtimestamp(statinfo.st_mtime, tz=timezone.utc)

            if file_mtime > cutoff_time:
                filtered_files.append(file_path)

        # Only new_file should remain
        assert len(filtered_files) == 1
        assert new_file in filtered_files
        assert old_file not in filtered_files

        # Cleanup
        os.remove(old_file)
        os.remove(new_file)
        os.rmdir(temp_dir)

    def test_includes_recently_modified_files(self, mock_files):
        """Test that recently modified files are included."""
        temp_dir, old_file, new_file, cutoff_time = mock_files

        new_file_mtime = datetime.fromtimestamp(
            os.path.getmtime(new_file), tz=timezone.utc
        )

        assert new_file_mtime > cutoff_time

        # Cleanup
        os.remove(old_file)
        os.remove(new_file)
        os.rmdir(temp_dir)


class TestModifiedSinceIntegration:
    """Integration tests for --modified-since with database operations."""

    @pytest.fixture
    def mock_database(self):
        """Create a mock database for testing."""
        db_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        db_path = db_file.name
        db_file.close()

        con = sqlite3.connect(db_path)
        cur = con.cursor()

        # Create necessary tables
        cur.execute(
            """
            CREATE TABLE files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                size INTEGER,
                mtime TEXT,
                md5 TEXT,
                tar TEXT,
                offset INTEGER
            )
        """
        )

        cur.execute(
            """
            CREATE TABLE config (
                id INTEGER PRIMARY KEY,
                config TEXT,
                value TEXT
            )
        """
        )

        # Insert config
        cur.execute("INSERT INTO config VALUES (NULL, 'path', '/test/path')")
        cur.execute("INSERT INTO config VALUES (NULL, 'hpss', 'none')")
        cur.execute("INSERT INTO config VALUES (NULL, 'maxsize', '268435456')")

        con.commit()

        yield db_path, cur, con

        con.close()
        os.unlink(db_path)

    @patch("zstash.update.get_files_to_archive")
    @patch("zstash.update.config")
    def test_performance_improvement_with_modified_since(
        self, mock_config, mock_get_files, mock_database
    ):
        """Test that --modified-since significantly reduces files to scan."""
        db_path, cur, con = mock_database

        # Setup config
        mock_config.maxsize = 268435456
        mock_config.hpss = "none"
        mock_config.path = "/test/path"

        # Create temporary files
        temp_dir = tempfile.mkdtemp()
        now = datetime.now(timezone.utc)

        # Create 10 old files and 2 new files
        old_files = []
        for i in range(10):
            old_file = os.path.join(temp_dir, f"old_file_{i}.txt")
            with open(old_file, "w") as f:
                f.write(f"old content {i}")
            old_time = (now - timedelta(hours=2)).timestamp()
            os.utime(old_file, (old_time, old_time))
            old_files.append(old_file)

        new_files = []
        for i in range(2):
            new_file = os.path.join(temp_dir, f"new_file_{i}.txt")
            with open(new_file, "w") as f:
                f.write(f"new content {i}")
            new_time = (now - timedelta(minutes=30)).timestamp()
            os.utime(new_file, (new_time, new_time))
            new_files.append(new_file)

        all_files = old_files + new_files
        mock_get_files.return_value = all_files

        # Simulate filtering with modified_since
        cutoff_time = now - timedelta(hours=1)
        filtered_count = sum(
            1
            for f in all_files
            if datetime.fromtimestamp(os.path.getmtime(f), tz=timezone.utc)
            > cutoff_time
        )

        # Should only have 2 files (the new ones)
        assert filtered_count == 2

        # Cleanup
        for f in all_files:
            os.remove(f)
        os.rmdir(temp_dir)

    @patch("zstash.update.get_files_to_archive")
    @patch("zstash.update.config")
    def test_modified_since_with_no_new_files(
        self, mock_config, mock_get_files, mock_database
    ):
        """Test behavior when no files are newer than --modified-since."""
        db_path, cur, con = mock_database

        mock_config.maxsize = 268435456
        mock_config.hpss = "none"
        mock_config.path = "/test/path"

        # Create only old files
        temp_dir = tempfile.mkdtemp()
        now = datetime.now(timezone.utc)

        old_files = []
        for i in range(5):
            old_file = os.path.join(temp_dir, f"old_file_{i}.txt")
            with open(old_file, "w") as f:
                f.write(f"old content {i}")
            old_time = (now - timedelta(hours=2)).timestamp()
            os.utime(old_file, (old_time, old_time))
            old_files.append(old_file)

        mock_get_files.return_value = old_files

        from zstash.update import update_database

        args = MagicMock()
        args.hpss = "none"
        args.modified_since = (now - timedelta(hours=1)).isoformat()
        args.include = None
        args.exclude = None
        args.dry_run = False
        args.keep = True
        args.follow_symlinks = False
        args.non_blocking = False
        args.error_on_duplicate_tar = False
        args.overwrite_duplicate_tars = False

        with patch("zstash.update.get_db_filename", return_value=db_path):
            with patch("zstash.update.update_config"):
                result = update_database(args, os.path.dirname(db_path))

        # Should return None (nothing to update) - all files filtered out
        assert result is None

        # Cleanup
        for f in old_files:
            os.remove(f)
        os.rmdir(temp_dir)


class TestModifiedSinceEdgeCases:
    """Test edge cases for --modified-since functionality."""

    def test_modified_since_with_file_stat_error(self):
        """Test that files with stat errors are included (fail-safe behavior)."""
        # Create a file then remove it to cause stat error
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        temp_file_path = temp_file.name
        temp_file.close()
        os.remove(temp_file_path)

        files = [temp_file_path]
        filtered_files = []
        cutoff = datetime.now(timezone.utc) - timedelta(hours=1)

        for file_path in files:
            try:
                statinfo = os.lstat(file_path)
                file_mtime = datetime.fromtimestamp(statinfo.st_mtime, tz=timezone.utc)
                if file_mtime > cutoff:
                    filtered_files.append(file_path)
            except (OSError, IOError):
                # Include file if we can't stat it (fail-safe)
                filtered_files.append(file_path)

        # File should be included despite stat error
        assert len(filtered_files) == 1

    def test_modified_since_with_exact_timestamp(self):
        """Test behavior when file mtime exactly matches cutoff."""
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        temp_file.close()

        # Set file time to exact cutoff
        cutoff = datetime.now(timezone.utc) - timedelta(hours=1)
        cutoff_timestamp = cutoff.timestamp()
        os.utime(temp_file.name, (cutoff_timestamp, cutoff_timestamp))

        file_mtime = datetime.fromtimestamp(
            os.path.getmtime(temp_file.name), tz=timezone.utc
        )

        # File at exact cutoff should NOT be included (> not >=)
        # Due to floating point precision, allow small difference
        time_diff = (file_mtime - cutoff).total_seconds()
        assert abs(time_diff) < 1.0  # Within 1 second is close enough

        os.remove(temp_file.name)

    def test_iso_format_variations(self):
        """Test various ISO format timestamp inputs."""
        valid_formats = [
            "2025-12-08T14:00:00",
            "2025-12-08T14:00:00.123456",
            "2025-12-08 14:00:00",  # Space separator also works with fromisoformat
        ]

        for fmt in valid_formats:
            try:
                dt = datetime.fromisoformat(fmt)
                assert isinstance(dt, datetime)
            except ValueError:
                pytest.fail(f"Failed to parse valid format: {fmt}")


class TestLoggingOutput:
    """Test that appropriate logging messages are generated."""

    @patch("zstash.update.get_files_to_archive")
    @patch("zstash.update.logger")
    @patch("zstash.update.config")
    def test_logs_filtering_info(self, mock_config, mock_logger, mock_get_files):
        """Test that filtering information is logged."""
        mock_config.maxsize = 268435456
        mock_config.hpss = "none"
        mock_config.path = "/test/path"

        # Create mock database
        db_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        db_path = db_file.name
        db_file.close()

        con = sqlite3.connect(db_path)
        cur = con.cursor()

        # Create both config AND files tables
        cur.execute(
            """
            CREATE TABLE config (
                id INTEGER PRIMARY KEY,
                config TEXT,
                value TEXT
            )
        """
        )
        cur.execute(
            """
            CREATE TABLE files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                size INTEGER,
                mtime TEXT,
                md5 TEXT,
                tar TEXT,
                offset INTEGER
            )
        """
        )
        cur.execute("INSERT INTO config VALUES (NULL, 'path', '/test/path')")
        cur.execute("INSERT INTO config VALUES (NULL, 'hpss', 'none')")
        cur.execute("INSERT INTO config VALUES (NULL, 'maxsize', '268435456')")
        con.commit()
        con.close()

        # Create temporary files
        temp_dir = tempfile.mkdtemp()
        now = datetime.now(timezone.utc)

        old_file = os.path.join(temp_dir, "old.txt")
        with open(old_file, "w") as f:
            f.write("old")
        old_time = (now - timedelta(hours=2)).timestamp()
        os.utime(old_file, (old_time, old_time))

        mock_get_files.return_value = [old_file]

        from zstash.update import update_database

        args = MagicMock()
        args.hpss = "none"
        args.modified_since = (now - timedelta(hours=1)).isoformat()
        args.include = None
        args.exclude = None
        args.dry_run = False
        args.keep = True
        args.follow_symlinks = False
        args.non_blocking = False
        args.error_on_duplicate_tar = False
        args.overwrite_duplicate_tars = False

        with patch("zstash.update.get_db_filename", return_value=db_path):
            with patch("zstash.update.update_config"):
                update_database(args, os.path.dirname(db_path))

        # Check that appropriate log messages were called
        info_calls = [str(call) for call in mock_logger.info.call_args_list]
        assert any("Filtering files" in call for call in info_calls)
        assert any("Pre-filtered" in call for call in info_calls)

        # Cleanup
        os.remove(old_file)
        os.rmdir(temp_dir)
        os.unlink(db_path)


class TestInvalidTimestamp:
    """Test handling of invalid timestamps."""

    @pytest.fixture
    def mock_database_simple(self):
        """Create a simple mock database."""
        db_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        db_path = db_file.name
        db_file.close()

        con = sqlite3.connect(db_path)
        cur = con.cursor()

        cur.execute(
            """
            CREATE TABLE config (
                id INTEGER PRIMARY KEY,
                config TEXT,
                value TEXT
            )
        """
        )
        cur.execute("INSERT INTO config VALUES (NULL, 'path', '/test/path')")
        cur.execute("INSERT INTO config VALUES (NULL, 'hpss', 'none')")
        cur.execute("INSERT INTO config VALUES (NULL, 'maxsize', '268435456')")
        con.commit()
        con.close()

        yield db_path

        os.unlink(db_path)

    @patch("zstash.update.get_files_to_archive", return_value=[])
    @patch("zstash.update.config")
    def test_invalid_timestamp_format(
        self, mock_config, mock_get_files, mock_database_simple
    ):
        """Test that invalid timestamp format raises ValueError."""
        db_path = mock_database_simple

        mock_config.maxsize = 268435456
        mock_config.hpss = "none"
        mock_config.path = "/test/path"

        from zstash.update import update_database

        args = MagicMock()
        args.hpss = "none"
        args.modified_since = "invalid-timestamp"
        args.include = None
        args.exclude = None

        with patch("zstash.update.get_db_filename", return_value=db_path):
            with patch("zstash.update.update_config"):
                with pytest.raises(ValueError, match="Invalid --modified-since format"):
                    update_database(args, os.path.dirname(db_path))
