"""
Checkpoint management for zstash operations.

This module provides functionality to save and load checkpoints during
zstash update and check operations, enabling efficient resume capabilities.
"""

from __future__ import absolute_import, print_function

import sqlite3
from datetime import datetime
from typing import Any, Dict, Optional

from .settings import logger

# Type alias for checkpoint data
CheckpointDict = Dict[str, Any]


def checkpoint_table_exists(cur: sqlite3.Cursor) -> bool:
    """
    Check if the checkpoints table exists in the database.
    This allows for backwards compatibility with older archives.
    """
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='checkpoints'"
    )
    return cur.fetchone() is not None


def create_checkpoint_table(cur: sqlite3.Cursor, con: sqlite3.Connection) -> None:
    """
    Create the checkpoints table if it doesn't exist.
    Safe to call multiple times.
    """
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS checkpoints (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            operation TEXT NOT NULL,
            last_tar TEXT,
            last_tar_index INTEGER,
            timestamp DATETIME NOT NULL,
            files_processed INTEGER,
            total_files INTEGER,
            status TEXT
        )
    """
    )
    con.commit()
    logger.debug("Checkpoints table created/verified")


def save_checkpoint(
    cur: sqlite3.Cursor,
    con: sqlite3.Connection,
    operation: str,
    last_tar: str,
    files_processed: int,
    total_files: int,
    status: str = "in_progress",
) -> None:
    """
    Save a checkpoint to the database.

    Args:
        cur: Database cursor
        con: Database connection
        operation: 'update' or 'check'
        last_tar: Name of the last tar processed (e.g., '00002a.tar')
        files_processed: Number of files processed so far
        total_files: Total number of files to process
        status: 'in_progress', 'completed', or 'failed'
    """
    # Ensure table exists
    if not checkpoint_table_exists(cur):
        create_checkpoint_table(cur, con)

    # Extract tar index from tar name (remove .tar and convert hex to int)
    last_tar_index: Optional[int] = None
    if last_tar:
        tar_name = last_tar.replace(".tar", "")
        try:
            last_tar_index = int(tar_name, 16)
        except ValueError:
            logger.warning(f"Could not parse tar index from: {last_tar}")

    timestamp = datetime.utcnow()

    cur.execute(
        """
        INSERT INTO checkpoints
        (operation, last_tar, last_tar_index, timestamp, files_processed, total_files, status)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """,
        (
            operation,
            last_tar,
            last_tar_index,
            timestamp,
            files_processed,
            total_files,
            status,
        ),
    )
    con.commit()

    logger.debug(
        f"Checkpoint saved: {operation} - {last_tar} ({files_processed}/{total_files}) - {status}"
    )


def load_latest_checkpoint(
    cur: sqlite3.Cursor, operation: str
) -> Optional[CheckpointDict]:
    """
    Load the most recent checkpoint for a given operation.
    Returns None if no checkpoint exists or table doesn't exist.

    Args:
        cur: Database cursor
        operation: 'update' or 'check'

    Returns:
        Dictionary with checkpoint data or None
    """
    # Check if table exists (backwards compatibility)
    if not checkpoint_table_exists(cur):
        logger.debug(
            "Checkpoints table does not exist. This is normal for older archives."
        )
        return None

    cur.execute(
        """
        SELECT id, operation, last_tar, last_tar_index, timestamp,
               files_processed, total_files, status
        FROM checkpoints
        WHERE operation = ?
        ORDER BY timestamp DESC
        LIMIT 1
    """,
        (operation,),
    )

    row = cur.fetchone()
    if row is None:
        logger.debug(f"No checkpoint found for operation: {operation}")
        return None

    checkpoint: CheckpointDict = {
        "id": row[0],
        "operation": row[1],
        "last_tar": row[2],
        "last_tar_index": row[3],
        "timestamp": row[4],
        "files_processed": row[5],
        "total_files": row[6],
        "status": row[7],
    }

    logger.info(
        f"Loaded checkpoint: {operation} from {checkpoint['timestamp']} - "
        f"last tar: {checkpoint['last_tar']}"
    )

    return checkpoint


def complete_checkpoint(
    cur: sqlite3.Cursor, con: sqlite3.Connection, operation: str
) -> None:
    """
    Mark the most recent checkpoint for an operation as completed.
    Safe to call even if checkpoints table doesn't exist.

    Args:
        cur: Database cursor
        con: Database connection
        operation: 'update' or 'check'
    """
    if not checkpoint_table_exists(cur):
        return

    cur.execute(
        """
        UPDATE checkpoints
        SET status = 'completed', timestamp = ?
        WHERE id = (
            SELECT id FROM checkpoints
            WHERE operation = ?
            ORDER BY timestamp DESC
            LIMIT 1
        )
    """,
        (datetime.utcnow(), operation),
    )
    con.commit()
    logger.info(f"Checkpoint completed for operation: {operation}")


def clear_checkpoints(
    cur: sqlite3.Cursor, con: sqlite3.Connection, operation: str
) -> None:
    """
    Clear all checkpoints for a given operation.
    Useful for starting fresh. Safe to call even if table doesn't exist.

    Args:
        cur: Database cursor
        con: Database connection
        operation: 'update' or 'check'
    """
    if not checkpoint_table_exists(cur):
        logger.debug("No checkpoints to clear (table doesn't exist)")
        return

    cur.execute("DELETE FROM checkpoints WHERE operation = ?", (operation,))
    con.commit()
    logger.info(f"Cleared all checkpoints for operation: {operation}")


def get_checkpoint_status(cur: sqlite3.Cursor, operation: str) -> Optional[str]:
    """
    Get the status of the most recent checkpoint.
    Returns None if no checkpoint exists or table doesn't exist.

    Args:
        cur: Database cursor
        operation: 'update' or 'check'

    Returns:
        Status string ('in_progress', 'completed', 'failed') or None
    """
    if not checkpoint_table_exists(cur):
        return None

    cur.execute(
        """
        SELECT status FROM checkpoints
        WHERE operation = ?
        ORDER BY timestamp DESC
        LIMIT 1
    """,
        (operation,),
    )

    row = cur.fetchone()
    return row[0] if row else None
