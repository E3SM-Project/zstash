import os
from typing import List, Optional

from globus_sdk import TransferClient, TransferData
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


class TransferBatch:
    """Represents one batch of files being transferred"""

    def __init__(self):
        self.file_paths: List[str] = []
        self.task_id: Optional[str] = None
        self.task_status: Optional[str] = None
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

    def get_most_recent_transfer(self) -> Optional[TransferBatch]:
        return self.batches[-1] if self.batches else None

    def delete_successfully_transferred_files(self):
        """Check transfer status and delete files from successful transfers"""
        logger.info(
            f"{ts_utc()}: Checking for successfully transferred files to delete"
        )
        # Clean up empty batches first
        self.batches = [batch for batch in self.batches if batch.file_paths]
        # Identify pending Globus batches (not yet succeeded)
        pending_globus_batches = [
            batch
            for batch in self.batches
            if batch.is_globus and batch.task_id and batch.task_status != "SUCCEEDED"
        ]
        # To avoid excessive Globus API calls, only poll a small subset of
        # pending batches on each invocation (here: at most one).
        if pending_globus_batches:
            batch_to_poll = pending_globus_batches[0]
            logger.debug(
                f"{ts_utc()}: batch is globus AND is not yet successful "
                f"(task_id={batch_to_poll.task_id})"
            )
            if self.globus_config and self.globus_config.transfer_client:
                # Non-blocking status check
                logger.debug(
                    f"{ts_utc()}: Checking status of task_id={batch_to_poll.task_id}"
                )
                task = self.globus_config.transfer_client.get_task(
                    batch_to_poll.task_id
                )
                batch_to_poll.task_status = task["status"]
                logger.debug(
                    f"{ts_utc()}: task_id={batch_to_poll.task_id} "
                    f"status={batch_to_poll.task_status}"
                )
            else:
                logger.debug(
                    f"{ts_utc()}: globus_config is not set up with a transfer client"
                )
        # Now delete files for successful transfers
        for batch in self.batches:
            if (not batch.is_globus) or (batch.task_status == "SUCCEEDED"):
                # The files were transferred successfully, so delete them
                logger.info(
                    f"{ts_utc()}: Deleting {len(batch.file_paths)} files from successful transfer"
                )
                batch.delete_files()
                logger.debug("Deletion completed")
                batch.file_paths = []  # Mark as processed
