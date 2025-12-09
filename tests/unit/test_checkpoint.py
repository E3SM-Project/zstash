"""
Tests for checkpoint.py module
"""

import sqlite3
import tempfile

import pytest

from zstash import checkpoint


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    con = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
    cur = con.cursor()

    yield cur, con

    con.close()


class TestCheckpointTable:
    """Tests for checkpoint table creation and existence checks."""

    def test_checkpoint_table_does_not_exist_initially(self, temp_db):
        """Test that checkpoint table doesn't exist in a fresh database."""
        cur, con = temp_db
        assert not checkpoint.checkpoint_table_exists(cur)

    def test_create_checkpoint_table(self, temp_db):
        """Test creating checkpoint table."""
        cur, con = temp_db
        checkpoint.create_checkpoint_table(cur, con)
        assert checkpoint.checkpoint_table_exists(cur)

    def test_create_checkpoint_table_idempotent(self, temp_db):
        """Test that creating checkpoint table multiple times is safe."""
        cur, con = temp_db
        checkpoint.create_checkpoint_table(cur, con)
        checkpoint.create_checkpoint_table(cur, con)
        assert checkpoint.checkpoint_table_exists(cur)


class TestSaveCheckpoint:
    """Tests for saving checkpoints."""

    def test_save_checkpoint_creates_table(self, temp_db):
        """Test that save_checkpoint creates table if it doesn't exist."""
        cur, con = temp_db

        checkpoint.save_checkpoint(
            cur, con, "update", "000001.tar", 10, 100, "in_progress"
        )

        assert checkpoint.checkpoint_table_exists(cur)

    def test_save_checkpoint_stores_data(self, temp_db):
        """Test that checkpoint data is correctly stored."""
        cur, con = temp_db

        checkpoint.save_checkpoint(
            cur, con, "update", "00002a.tar", 50, 200, "in_progress"
        )

        # Verify data was stored
        cur.execute("SELECT * FROM checkpoints WHERE operation = 'update'")
        row = cur.fetchone()

        assert row is not None
        assert row[1] == "update"  # operation
        assert row[2] == "00002a.tar"  # last_tar
        assert row[3] == 42  # last_tar_index (hex 0x2a = 42)
        assert row[5] == 50  # files_processed
        assert row[6] == 200  # total_files
        assert row[7] == "in_progress"  # status

    def test_save_checkpoint_multiple_operations(self, temp_db):
        """Test saving checkpoints for different operations."""
        cur, con = temp_db

        checkpoint.save_checkpoint(
            cur, con, "update", "000001.tar", 10, 100, "in_progress"
        )
        checkpoint.save_checkpoint(
            cur, con, "check", "000005.tar", 25, 50, "in_progress"
        )

        # Verify both were stored
        cur.execute("SELECT COUNT(*) FROM checkpoints")
        count = cur.fetchone()[0]
        assert count == 2

    def test_save_checkpoint_invalid_tar_name(self, temp_db):
        """Test that invalid tar names are handled gracefully."""
        cur, con = temp_db

        # Should not raise exception
        checkpoint.save_checkpoint(
            cur, con, "update", "invalid.tar", 10, 100, "in_progress"
        )

        ckpt = checkpoint.load_latest_checkpoint(cur, "update")
        assert ckpt["last_tar_index"] is None


class TestLoadCheckpoint:
    """Tests for loading checkpoints."""

    def test_load_checkpoint_no_table(self, temp_db):
        """Test loading checkpoint when table doesn't exist."""
        cur, con = temp_db

        ckpt = checkpoint.load_latest_checkpoint(cur, "update")
        assert ckpt is None

    def test_load_checkpoint_no_data(self, temp_db):
        """Test loading checkpoint when table exists but is empty."""
        cur, con = temp_db
        checkpoint.create_checkpoint_table(cur, con)

        ckpt = checkpoint.load_latest_checkpoint(cur, "update")
        assert ckpt is None

    def test_load_checkpoint_success(self, temp_db):
        """Test successfully loading a checkpoint."""
        cur, con = temp_db

        checkpoint.save_checkpoint(
            cur, con, "update", "00002a.tar", 50, 200, "in_progress"
        )

        ckpt = checkpoint.load_latest_checkpoint(cur, "update")

        assert ckpt is not None
        assert ckpt["operation"] == "update"
        assert ckpt["last_tar"] == "00002a.tar"
        assert ckpt["last_tar_index"] == 42
        assert ckpt["files_processed"] == 50
        assert ckpt["total_files"] == 200
        assert ckpt["status"] == "in_progress"
        # Timestamp may be datetime or string depending on sqlite3 configuration
        assert ckpt["timestamp"] is not None

    def test_load_checkpoint_returns_latest(self, temp_db):
        """Test that load_checkpoint returns the most recent checkpoint."""
        cur, con = temp_db

        # Save multiple checkpoints
        checkpoint.save_checkpoint(
            cur, con, "update", "000001.tar", 10, 100, "in_progress"
        )
        checkpoint.save_checkpoint(
            cur, con, "update", "000002.tar", 20, 100, "in_progress"
        )
        checkpoint.save_checkpoint(
            cur, con, "update", "000003.tar", 30, 100, "in_progress"
        )

        ckpt = checkpoint.load_latest_checkpoint(cur, "update")

        # Should get the last one
        assert ckpt["last_tar"] == "000003.tar"
        assert ckpt["files_processed"] == 30

    def test_load_checkpoint_filters_by_operation(self, temp_db):
        """Test that checkpoints are filtered by operation type."""
        cur, con = temp_db

        checkpoint.save_checkpoint(
            cur, con, "update", "000001.tar", 10, 100, "in_progress"
        )
        checkpoint.save_checkpoint(
            cur, con, "check", "000005.tar", 50, 200, "in_progress"
        )

        update_ckpt = checkpoint.load_latest_checkpoint(cur, "update")
        check_ckpt = checkpoint.load_latest_checkpoint(cur, "check")

        assert update_ckpt["last_tar"] == "000001.tar"
        assert check_ckpt["last_tar"] == "000005.tar"


class TestCompleteCheckpoint:
    """Tests for completing checkpoints."""

    def test_complete_checkpoint_no_table(self, temp_db):
        """Test completing checkpoint when table doesn't exist."""
        cur, con = temp_db

        # Should not raise exception
        checkpoint.complete_checkpoint(cur, con, "update")

    def test_complete_checkpoint_success(self, temp_db):
        """Test successfully completing a checkpoint."""
        cur, con = temp_db

        checkpoint.save_checkpoint(
            cur, con, "update", "000001.tar", 100, 100, "in_progress"
        )

        checkpoint.complete_checkpoint(cur, con, "update")

        ckpt = checkpoint.load_latest_checkpoint(cur, "update")
        assert ckpt["status"] == "completed"

    def test_complete_checkpoint_updates_latest_only(self, temp_db):
        """Test that only the latest checkpoint is completed."""
        cur, con = temp_db

        # Save two checkpoints
        checkpoint.save_checkpoint(
            cur, con, "update", "000001.tar", 50, 100, "in_progress"
        )
        checkpoint.save_checkpoint(
            cur, con, "update", "000002.tar", 100, 100, "in_progress"
        )

        checkpoint.complete_checkpoint(cur, con, "update")

        # Check that latest is completed
        ckpt = checkpoint.load_latest_checkpoint(cur, "update")
        assert ckpt["status"] == "completed"
        assert ckpt["last_tar"] == "000002.tar"

        # Check that first checkpoint is still in_progress
        cur.execute("SELECT status FROM checkpoints WHERE last_tar = '000001.tar'")
        status = cur.fetchone()[0]
        assert status == "in_progress"


class TestClearCheckpoints:
    """Tests for clearing checkpoints."""

    def test_clear_checkpoints_no_table(self, temp_db):
        """Test clearing checkpoints when table doesn't exist."""
        cur, con = temp_db

        # Should not raise exception
        checkpoint.clear_checkpoints(cur, con, "update")

    def test_clear_checkpoints_success(self, temp_db):
        """Test successfully clearing checkpoints."""
        cur, con = temp_db

        # Save some checkpoints
        checkpoint.save_checkpoint(
            cur, con, "update", "000001.tar", 10, 100, "in_progress"
        )
        checkpoint.save_checkpoint(
            cur, con, "update", "000002.tar", 20, 100, "in_progress"
        )

        checkpoint.clear_checkpoints(cur, con, "update")

        # Verify they're gone
        ckpt = checkpoint.load_latest_checkpoint(cur, "update")
        assert ckpt is None

    def test_clear_checkpoints_filters_by_operation(self, temp_db):
        """Test that clear only removes checkpoints for specified operation."""
        cur, con = temp_db

        checkpoint.save_checkpoint(
            cur, con, "update", "000001.tar", 10, 100, "in_progress"
        )
        checkpoint.save_checkpoint(
            cur, con, "check", "000005.tar", 50, 200, "in_progress"
        )

        checkpoint.clear_checkpoints(cur, con, "update")

        # Update checkpoint should be gone
        update_ckpt = checkpoint.load_latest_checkpoint(cur, "update")
        assert update_ckpt is None

        # Check checkpoint should still exist
        check_ckpt = checkpoint.load_latest_checkpoint(cur, "check")
        assert check_ckpt is not None
        assert check_ckpt["last_tar"] == "000005.tar"


class TestGetCheckpointStatus:
    """Tests for getting checkpoint status."""

    def test_get_status_no_table(self, temp_db):
        """Test getting status when table doesn't exist."""
        cur, con = temp_db

        status = checkpoint.get_checkpoint_status(cur, "update")
        assert status is None

    def test_get_status_no_data(self, temp_db):
        """Test getting status when no checkpoints exist."""
        cur, con = temp_db
        checkpoint.create_checkpoint_table(cur, con)

        status = checkpoint.get_checkpoint_status(cur, "update")
        assert status is None

    def test_get_status_success(self, temp_db):
        """Test successfully getting checkpoint status."""
        cur, con = temp_db

        checkpoint.save_checkpoint(
            cur, con, "update", "000001.tar", 10, 100, "in_progress"
        )

        status = checkpoint.get_checkpoint_status(cur, "update")
        assert status == "in_progress"

    def test_get_status_after_completion(self, temp_db):
        """Test getting status after checkpoint is completed."""
        cur, con = temp_db

        checkpoint.save_checkpoint(
            cur, con, "update", "000001.tar", 100, 100, "in_progress"
        )
        checkpoint.complete_checkpoint(cur, con, "update")

        status = checkpoint.get_checkpoint_status(cur, "update")
        assert status == "completed"


class TestTarIndexParsing:
    """Tests for tar index parsing from tar names."""

    def test_parse_valid_hex_tar_names(self, temp_db):
        """Test parsing various valid hex tar names."""
        cur, con = temp_db

        test_cases = [
            ("000001.tar", 1),
            ("00000a.tar", 10),
            ("00002a.tar", 42),
            ("0000ff.tar", 255),
            ("001234.tar", 4660),
        ]

        for tar_name, expected_index in test_cases:
            checkpoint.save_checkpoint(cur, con, "test", tar_name, 1, 1, "in_progress")
            ckpt = checkpoint.load_latest_checkpoint(cur, "test")
            assert ckpt["last_tar_index"] == expected_index
            checkpoint.clear_checkpoints(cur, con, "test")
