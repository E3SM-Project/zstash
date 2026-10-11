import os
from enum import Enum, auto
from typing import List, Optional

from globus_sdk import TransferClient, TransferData
from globus_sdk.response import GlobusHTTPResponse
from globus_sdk.services.transfer.response.iterable import IterableTransferResponse

from .settings import logger
from .utils import ts_utc


class GlobusConfig:
    """Globus connection configuration"""

    def __init__(self):
        self.remote_endpoint: Optional[str] = None
        self.local_endpoint: Optional[str] = None
        self.transfer_client: Optional[TransferClient] = None
        self.archive_directory_listing: Optional[IterableTransferResponse] = None


class TaskStatus(Enum):
    """Enum for Globus transfer task status"""

    # The first 4 values are defined by the Globus API:
    # https://docs.globus.org/api/transfer/task/#task_fields
    SUCCEEDED = auto()
    ACTIVE = auto()
    INACTIVE = auto()
    FAILED = auto()
    # The last 3 values are custom statuses we add on.
    UNKNOWN = auto()
    SUBMITTED = auto()
    EXHAUSTED_TIMEOUT_RETRIES = auto()

    @classmethod
    def convert_from_status_from_globus_sdk(cls, globus_task: GlobusHTTPResponse):
        """Convert a Globus API status string to a TaskStatus enum value"""
        status_from_globus_sdk: str = globus_task["status"]
        status_from_globus_sdk = status_from_globus_sdk.upper()
        if status_from_globus_sdk == "SUCCEEDED":
            return TaskStatus.SUCCEEDED
        elif status_from_globus_sdk == "ACTIVE":
            return TaskStatus.ACTIVE
        elif status_from_globus_sdk == "INACTIVE":
            return TaskStatus.INACTIVE
        elif status_from_globus_sdk == "FAILED":
            return TaskStatus.FAILED
        else:
            logger.warning(
                f"Received unrecognized Globus status: {status_from_globus_sdk}"
            )
            return TaskStatus.UNKNOWN

    def __str__(self) -> str:
        return self.name


class TransferBatch:
    """
    Represents one batch of files submitted (or to be submitted) as a single
    Globus transfer task.

    Lifecycle:
      1. Created in hpss_transfer() when a new batch is needed.
      2. Files are added to local_paths_to_delete (and to transfer_data) as
         hpss_put() is called for each tar.
      3. The batch is submitted to Globus (task_id is set).
      4. Once the Globus task succeeds, local_paths_to_delete are removed from disk.
    """

    def __init__(self):
        # Local tar files in this batch; deleted once the Globus transfer succeeds.
        self.local_paths_to_delete: List[str] = []
        self.task_id: Optional[str] = None
        self.task_status: Optional[TaskStatus] = None
        self.is_globus: bool = False
        self.transfer_data: Optional[TransferData] = None  # Only for Globus

    def delete_local_files(self):
        """Delete all local tar files tracked by this batch."""
        for path in self.local_paths_to_delete:
            try:
                os.remove(path)
            except FileNotFoundError:
                logger.warning(f"File already deleted: {path}")


class TransferManager:
    def __init__(self):
        # All transfer batches (Globus or HPSS), in submission order.
        self.batches: List[TransferBatch] = []
        self.cumulative_tarfiles_pushed: int = 0

        # Globus connection state; None when not using Globus.
        self.globus_config: Optional[GlobusConfig] = None

    def get_most_recent_batch(self) -> Optional[TransferBatch]:
        """Return the last batch, or None if no batches exist."""
        return self.batches[-1] if self.batches else None

    def delete_successfully_transferred_files(self):
        """
        Delete local tar files for every batch whose Globus transfer has
        succeeded (or for every non-Globus batch, which transfers synchronously).
        Batches whose files have already been deleted are skipped.
        """
        logger.info(
            f"{ts_utc()}: Checking for successfully transferred files to delete"
        )
        for batch in self.batches:
            if not batch.local_paths_to_delete:
                continue  # Already processed
            if batch.is_globus and batch.task_status != TaskStatus.SUCCEEDED:
                continue  # Globus transfer not yet confirmed successful
            logger.info(
                f"{ts_utc()}: Deleting {len(batch.local_paths_to_delete)} files "
                f"from successful transfer"
            )
            batch.delete_local_files()
            batch.local_paths_to_delete = []  # Mark as processed
