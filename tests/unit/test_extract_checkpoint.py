"""
Tests for extract.py checkpoint functionality.

These tests focus on extract/check-specific checkpoint behavior that isn't
covered by test_checkpoint.py or test_update_checkpoint.py.
"""

import os
import sqlite3
import tempfile
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from zstash import checkpoint, extract
from zstash.settings import FilesRow


@pytest.fixture
def mock_extract_db():
    """Create a mock database with files across multiple tars for extract testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    con = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
    cur = con.cursor()

    # Create files table
    cur.execute(
        """
        CREATE TABLE files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            size INTEGER,
            mtime DATETIME,
            md5 TEXT,
            tar TEXT,
            offset INTEGER
        )
    """
    )

    # Create config table
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

    # Insert test files across 5 tars
    now = datetime.utcnow()
    test_files = [
        ("file1.txt", 100, now, "hash1", "000001.tar", 0),
        ("file2.txt", 200, now, "hash2", "000001.tar", 512),
        ("file3.txt", 300, now, "hash3", "000002.tar", 0),
        ("file4.txt", 400, now, "hash4", "000003.tar", 0),
        ("file5.txt", 500, now, "hash5", "000003.tar", 512),
        ("file6.txt", 600, now, "hash6", "000004.tar", 0),
        ("file7.txt", 700, now, "hash7", "000005.tar", 0),
    ]

    for f in test_files:
        cur.execute("INSERT INTO files VALUES (NULL, ?, ?, ?, ?, ?, ?)", f)

    con.commit()

    yield db_path, cur, con

    con.close()
    os.unlink(db_path)


class TestHandleCheckpointResume:
    """Tests for handle_checkpoint_resume function."""

    def test_resume_calculates_correct_tar_range(self, mock_extract_db):
        """Test that resume correctly calculates the tar range from checkpoint."""
        db_path, cur, con = mock_extract_db

        # Create checkpoint at tar 000002 (index 2)
        checkpoint.save_checkpoint(cur, con, "check", "000002.tar", 3, 7, "in_progress")

        args = MagicMock()
        args.clear_checkpoint = False
        args.resume = True
        args.tars = None

        extract.handle_checkpoint_resume(args, cur, con, "check")

        # Should resume from 000003 (next after 000002) to 000005 (last)
        assert args.tars == "000003-000005"

    def test_resume_with_checkpoint_at_last_tar(self, mock_extract_db):
        """Test resume when checkpoint is at the last tar."""
        db_path, cur, con = mock_extract_db

        # Create checkpoint at the last tar
        checkpoint.save_checkpoint(cur, con, "check", "000005.tar", 7, 7, "in_progress")

        args = MagicMock()
        args.clear_checkpoint = False
        args.resume = True
        args.tars = None

        extract.handle_checkpoint_resume(args, cur, con, "check")

        # Should try to resume from 000006 to 000005, which is invalid
        # but will result in no matches (graceful handling)
        assert args.tars == "000006-000005"

    def test_resume_does_not_override_explicit_tars(self, mock_extract_db):
        """Test that resume respects user's explicit --tars setting."""
        db_path, cur, con = mock_extract_db

        checkpoint.save_checkpoint(cur, con, "check", "000002.tar", 3, 7, "in_progress")

        args = MagicMock()
        args.clear_checkpoint = False
        args.resume = True
        args.tars = "000001-000003"  # User explicitly set

        extract.handle_checkpoint_resume(args, cur, con, "check")

        # Should NOT override
        assert args.tars == "000001-000003"

    def test_clear_and_resume_together(self, mock_extract_db):
        """Test using both --clear-checkpoint and --resume."""
        db_path, cur, con = mock_extract_db

        # Create checkpoint
        checkpoint.save_checkpoint(cur, con, "check", "000002.tar", 3, 7, "in_progress")

        args = MagicMock()
        args.clear_checkpoint = True
        args.resume = True
        args.tars = None

        extract.handle_checkpoint_resume(args, cur, con, "check")

        # Checkpoint should be cleared first, then resume finds nothing
        ckpt = checkpoint.load_latest_checkpoint(cur, "check")
        assert ckpt is None
        assert args.tars is None


class TestExtractFilesCheckpointSaving:
    """Tests for checkpoint saving in extractFiles function."""

    @patch("zstash.extract.tarfile.open")
    @patch("zstash.extract.hpss_get")
    @patch("zstash.extract.os.path.exists")
    @patch("zstash.extract.should_extract_file")
    @patch("zstash.extract.check_sizes_match")
    def test_checkpoint_saved_after_each_tar_not_each_file(
        self,
        mock_check_sizes,
        mock_should_extract,
        mock_exists,
        mock_hpss_get,
        mock_tarfile_open,
        mock_extract_db,
    ):
        """Test that checkpoint is saved per tar, not per file."""
        db_path, cur, con = mock_extract_db

        # Setup mocks
        mock_exists.return_value = True
        mock_check_sizes.return_value = True
        mock_should_extract.return_value = False

        mock_tar = MagicMock()
        mock_tar.fileobj = MagicMock()
        mock_tarinfo = MagicMock()
        mock_tarinfo.isfile.return_value = True
        mock_tarinfo.name = "test.txt"
        mock_tar.tarinfo.fromtarfile.return_value = mock_tarinfo

        mock_extracted = MagicMock()
        mock_extracted.read.return_value = b""
        mock_tar.extractfile.return_value = mock_extracted

        mock_tarfile_open.return_value = mock_tar

        # Create files from 2 different tars
        now = datetime.utcnow()
        files = [
            FilesRow((1, "file1.txt", 100, now, "abc", "000001.tar", 0)),
            FilesRow((2, "file2.txt", 200, now, "def", "000001.tar", 512)),
            FilesRow((3, "file3.txt", 300, now, "ghi", "000002.tar", 0)),
        ]

        args = MagicMock()
        args.retries = 1
        args.error_on_duplicate_tar = False

        extract.extractFiles(
            files,
            keep_files=False,
            keep_tars=False,
            cache="/tmp/cache",
            cur=cur,
            args=args,
            multiprocess_worker=None,
            con=con,
            operation="check",
            total_files=3,
        )

        # Verify checkpoint was saved twice (once per tar, not per file)
        cur.execute("SELECT COUNT(*) FROM checkpoints WHERE operation = 'check'")
        count = cur.fetchone()[0]
        assert count == 2

        # Verify latest checkpoint is for the last tar
        ckpt = checkpoint.load_latest_checkpoint(cur, "check")
        assert ckpt["last_tar"] == "000002.tar"
        assert ckpt["files_processed"] == 3

    @patch("zstash.extract.tarfile.open")
    @patch("zstash.extract.hpss_get")
    @patch("zstash.extract.os.path.exists")
    @patch("zstash.extract.should_extract_file")
    @patch("zstash.extract.check_sizes_match")
    def test_checkpoint_tracks_files_processed_correctly(
        self,
        mock_check_sizes,
        mock_should_extract,
        mock_exists,
        mock_hpss_get,
        mock_tarfile_open,
        mock_extract_db,
    ):
        """Test that files_processed counter increments correctly."""
        db_path, cur, con = mock_extract_db

        # Setup mocks
        mock_exists.return_value = True
        mock_check_sizes.return_value = True
        mock_should_extract.return_value = False

        mock_tar = MagicMock()
        mock_tar.fileobj = MagicMock()
        mock_tarinfo = MagicMock()
        mock_tarinfo.isfile.return_value = True
        mock_tar.tarinfo.fromtarfile.return_value = mock_tarinfo
        mock_extracted = MagicMock()
        mock_extracted.read.return_value = b""
        mock_tar.extractfile.return_value = mock_extracted
        mock_tarfile_open.return_value = mock_tar

        now = datetime.utcnow()
        files = [
            FilesRow((1, "f1.txt", 100, now, "a", "000001.tar", 0)),
            FilesRow((2, "f2.txt", 200, now, "b", "000001.tar", 512)),
            FilesRow((3, "f3.txt", 300, now, "c", "000002.tar", 0)),
            FilesRow((4, "f4.txt", 400, now, "d", "000002.tar", 512)),
            FilesRow((5, "f5.txt", 500, now, "e", "000002.tar", 1024)),
        ]

        args = MagicMock()
        args.retries = 1
        args.error_on_duplicate_tar = False

        extract.extractFiles(
            files, False, False, "/tmp", cur, args, None, con, "check", 5
        )

        # After first tar (2 files processed)
        cur.execute(
            "SELECT files_processed FROM checkpoints WHERE last_tar = '000001.tar'"
        )
        count1 = cur.fetchone()[0]
        assert count1 == 2

        # After second tar (5 files total processed)
        cur.execute(
            "SELECT files_processed FROM checkpoints WHERE last_tar = '000002.tar'"
        )
        count2 = cur.fetchone()[0]
        assert count2 == 5

    @patch("zstash.extract.tarfile.open")
    @patch("zstash.extract.hpss_get")
    @patch("zstash.extract.os.path.exists")
    @patch("zstash.extract.should_extract_file")
    @patch("zstash.extract.check_sizes_match")
    def test_no_checkpoint_saved_for_extract_operation(
        self,
        mock_check_sizes,
        mock_should_extract,
        mock_exists,
        mock_hpss_get,
        mock_tarfile_open,
        mock_extract_db,
    ):
        """Test that checkpoints are NOT saved during extract (only check)."""
        db_path, cur, con = mock_extract_db

        mock_exists.return_value = True
        mock_check_sizes.return_value = True
        mock_should_extract.return_value = False

        mock_tar = MagicMock()
        mock_tar.fileobj = MagicMock()
        mock_tarinfo = MagicMock()
        mock_tarinfo.isfile.return_value = True
        mock_tar.tarinfo.fromtarfile.return_value = mock_tarinfo
        mock_extracted = MagicMock()
        mock_extracted.read.return_value = b""
        mock_tar.extractfile.return_value = mock_extracted
        mock_tarfile_open.return_value = mock_tar

        files = [FilesRow((1, "f.txt", 100, datetime.utcnow(), "a", "000001.tar", 0))]
        args = MagicMock()
        args.retries = 1
        args.error_on_duplicate_tar = False

        # operation="extract", not "check"
        extract.extractFiles(
            files, True, False, "/tmp", cur, args, None, con, "extract", 1
        )

        # No checkpoint should be saved
        ckpt = checkpoint.load_latest_checkpoint(cur, "extract")
        assert ckpt is None
        ckpt = checkpoint.load_latest_checkpoint(cur, "check")
        assert ckpt is None

    @patch("zstash.extract.tarfile.open")
    @patch("zstash.extract.hpss_get")
    @patch("zstash.extract.os.path.exists")
    @patch("zstash.extract.should_extract_file")
    @patch("zstash.extract.check_sizes_match")
    def test_no_checkpoint_with_multiprocessing(
        self,
        mock_check_sizes,
        mock_should_extract,
        mock_exists,
        mock_hpss_get,
        mock_tarfile_open,
        mock_extract_db,
    ):
        """Test that checkpoints are NOT saved when using multiprocessing."""
        db_path, cur, con = mock_extract_db

        mock_exists.return_value = True
        mock_check_sizes.return_value = True
        mock_should_extract.return_value = False

        mock_tar = MagicMock()
        mock_tar.fileobj = MagicMock()
        mock_tarinfo = MagicMock()
        mock_tarinfo.isfile.return_value = True
        mock_tar.tarinfo.fromtarfile.return_value = mock_tarinfo
        mock_extracted = MagicMock()
        mock_extracted.read.return_value = b""
        mock_tar.extractfile.return_value = mock_extracted
        mock_tarfile_open.return_value = mock_tar

        files = [FilesRow((1, "f.txt", 100, datetime.utcnow(), "a", "000001.tar", 0))]
        args = MagicMock()
        args.retries = 1
        args.error_on_duplicate_tar = False

        # Simulate multiprocessing by passing a worker
        mock_worker = MagicMock()
        mock_worker.print_queue = MagicMock()

        # Pass con=None for multiprocessing (no checkpoint support)
        extract.extractFiles(
            files, False, False, "/tmp", cur, args, mock_worker, None, "check", 1
        )

        # No checkpoint should be saved
        ckpt = checkpoint.load_latest_checkpoint(cur, "check")
        assert ckpt is None


class TestMultiprocessCheckpointWarning:
    """Tests for checkpoint behavior with multiprocessing."""

    @patch("zstash.extract.parallel.PrintMonitor")
    @patch("zstash.extract.parallel.ExtractWorker")
    @patch("zstash.extract.multiprocessing.Process")
    @patch("zstash.extract.logger")
    def test_warning_logged_for_check_with_multiprocessing(
        self, mock_logger, mock_process, mock_worker, mock_monitor, mock_extract_db
    ):
        """Test that warning is logged when using --workers with check."""
        db_path, cur, con = mock_extract_db

        mock_proc = MagicMock()
        mock_proc.is_alive.return_value = False
        mock_process.return_value = mock_proc

        files = [FilesRow((1, "f.txt", 100, datetime.utcnow(), "a", "000001.tar", 0))]
        args = MagicMock()

        extract.multiprocess_extract(
            num_workers=2,
            matches=files,
            keep_files=False,
            keep_tars=False,
            cache="/tmp",
            cur=cur,
            args=args,
            con=con,
            operation="check",
        )

        # Verify warning was logged
        mock_logger.info.assert_any_call(
            "Note: Checkpoint saving is disabled when using multiple workers. "
            "Use --workers=1 with --resume for checkpoint support."
        )

    @patch("zstash.extract.parallel.PrintMonitor")
    @patch("zstash.extract.parallel.ExtractWorker")
    @patch("zstash.extract.multiprocessing.Process")
    @patch("zstash.extract.logger")
    def test_no_warning_for_extract_with_multiprocessing(
        self, mock_logger, mock_process, mock_worker, mock_monitor, mock_extract_db
    ):
        """Test that no warning is logged for extract (only check needs warning)."""
        db_path, cur, con = mock_extract_db

        mock_proc = MagicMock()
        mock_proc.is_alive.return_value = False
        mock_process.return_value = mock_proc

        files = [FilesRow((1, "f.txt", 100, datetime.utcnow(), "a", "000001.tar", 0))]
        args = MagicMock()

        extract.multiprocess_extract(
            num_workers=2,
            matches=files,
            keep_files=True,
            keep_tars=False,
            cache="/tmp",
            cur=cur,
            args=args,
            con=con,
            operation="extract",  # extract, not check
        )

        # Warning should NOT be logged for extract
        checkpoint_warning_calls = [
            c
            for c in mock_logger.info.call_args_list
            if "Checkpoint saving is disabled" in str(c)
        ]
        assert len(checkpoint_warning_calls) == 0


class TestParseTarsOption:
    """Tests for parse_tars_option helper function."""

    def test_parse_single_tar(self):
        """Test parsing a single tar."""
        result = extract.parse_tars_option("000003", "000001", "000005")
        assert result == ["000003"]

    def test_parse_range(self):
        """Test parsing a tar range."""
        result = extract.parse_tars_option("000002-000004", "000001", "000005")
        assert result == ["000002", "000003", "000004"]

    def test_parse_open_start_range(self):
        """Test parsing range from beginning."""
        result = extract.parse_tars_option("-000003", "000001", "000005")
        assert result == ["000001", "000002", "000003"]

    def test_parse_open_end_range(self):
        """Test parsing range to end."""
        result = extract.parse_tars_option("000003-", "000001", "000005")
        assert result == ["000003", "000004", "000005"]

    def test_parse_with_tar_extension(self):
        """Test parsing with .tar extension is handled."""
        result = extract.parse_tars_option("000002.tar-000004.tar", "000001", "000005")
        assert result == ["000002", "000003", "000004"]

    def test_parse_multiple_specs(self):
        """Test parsing comma-separated tar specs."""
        result = extract.parse_tars_option("000001,000003,000005", "000001", "000005")
        assert result == ["000001", "000003", "000005"]

    def test_parse_deduplicates(self):
        """Test that duplicates are removed and sorted."""
        result = extract.parse_tars_option(
            "000003,000001,000003,000002", "000001", "000005"
        )
        assert result == ["000001", "000002", "000003"]

    def test_parse_hex_values(self):
        """Test parsing hex tar values."""
        result = extract.parse_tars_option("00000a-00000c", "000001", "000010")
        assert result == ["00000a", "00000b", "00000c"]


class TestCheckpointCompletion:
    """Tests for completing checkpoints after successful check."""

    def test_checkpoint_completed_on_success(self, mock_extract_db):
        """Test that checkpoint is marked completed when check succeeds."""
        db_path, cur, con = mock_extract_db

        # Create an in-progress checkpoint
        checkpoint.save_checkpoint(cur, con, "check", "000005.tar", 7, 7, "in_progress")

        # Simulate successful completion
        checkpoint.complete_checkpoint(cur, con, "check")

        ckpt = checkpoint.load_latest_checkpoint(cur, "check")
        assert ckpt["status"] == "completed"

    def test_checkpoint_not_completed_on_failure(self, mock_extract_db):
        """Test that checkpoint remains in_progress if there are failures."""
        db_path, cur, con = mock_extract_db

        checkpoint.save_checkpoint(cur, con, "check", "000003.tar", 4, 7, "in_progress")

        # Simulate failures (don't call complete_checkpoint)
        # In real code, this happens when failures list is not empty

        ckpt = checkpoint.load_latest_checkpoint(cur, "check")
        assert ckpt["status"] == "in_progress"


class TestResumeEdgeCases:
    """Tests for edge cases in checkpoint resume logic."""

    def test_resume_with_no_files_in_database(self, mock_extract_db):
        """Test resume when database has no files."""
        db_path, cur, con = mock_extract_db

        # Clear all files
        cur.execute("DELETE FROM files")
        con.commit()

        args = MagicMock()
        args.clear_checkpoint = False
        args.resume = True
        args.tars = None

        # Should not raise exception
        extract.handle_checkpoint_resume(args, cur, con, "check")

    def test_resume_with_checkpoint_but_no_tar_index(self, mock_extract_db):
        """Test resume when checkpoint has invalid tar index."""
        db_path, cur, con = mock_extract_db

        # Manually create checkpoint with null tar index
        checkpoint.create_checkpoint_table(cur, con)
        cur.execute(
            """
            INSERT INTO checkpoints
            (operation, last_tar, last_tar_index, timestamp, files_processed, total_files, status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("check", "invalid.tar", None, datetime.utcnow(), 1, 7, "in_progress"),
        )
        con.commit()

        args = MagicMock()
        args.clear_checkpoint = False
        args.resume = True
        args.tars = None

        # Should handle gracefully (args.tars stays None)
        extract.handle_checkpoint_resume(args, cur, con, "check")
        assert args.tars is None
