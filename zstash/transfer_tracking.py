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
    """Represents one batch of files being transferred"""

    def __init__(self):
        self.file_paths: List[str] = []
        self.task_id: Optional[str] = None
        self.task_status: Optional[TaskStatus] = None
        self.is_globus: bool = False
        self.transfer_data: Optional[TransferData] = None  # Only for Globus

    def delete_files(self):
        for src_path in self.file_paths:
            try:
                os.remove(src_path)
            except FileNotFoundError:
                logger.warning(f"File already deleted: {src_path}")


class TransferManager:
    def __init__(self):
        # All transfer batches (Globus or HPSS)
        self.batches: List[TransferBatch] = []
        self.cumulative_tarfiles_pushed: int = 0

        # Connection state (Globus-specific, None if not using Globus)
        self.globus_config: Optional[GlobusConfig] = None

    def get_most_recent_batch(self) -> Optional[TransferBatch]:
        """Get the last batch added to the manager, or None if no batches exist"""
        return self.batches[-1] if self.batches else None

    def delete_successfully_transferred_files(self):
        """Check transfer status and delete files from successful transfers"""
        logger.info(
            f"{ts_utc()}: Checking for successfully transferred files to delete"
        )
        # Clean up empty batches first
        self.batches = [batch for batch in self.batches if batch.file_paths]
        # Now delete files for successful transfers
        for batch in self.batches:
            if (not batch.is_globus) or (batch.task_status == TaskStatus.SUCCEEDED):
                # The files were transferred successfully, so delete them
                logger.info(
                    f"{ts_utc()}: Deleting {len(batch.file_paths)} files from successful transfer"
                )
                batch.delete_files()
                logger.debug("Deletion completed")
                batch.file_paths = []  # Mark as processed
