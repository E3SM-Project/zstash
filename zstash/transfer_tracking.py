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
            if os.path.exists(src_path):
                os.remove(src_path)


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
        for batch in self.batches:
            # Skip if already processed
            if not batch.file_paths:
                logger.debug(f"{ts_utc()}: batch was already processed, skipping")
                continue

            # Check if this is a Globus transfer that needs status update
            if batch.is_globus and batch.task_id and (batch.task_status != "SUCCEEDED"):
                logger.debug(f"{ts_utc()}: batch is globus AND is not yet successful")
                if self.globus_config and self.globus_config.transfer_client:
                    # Non-blocking status check
                    logger.debug(
                        f"{ts_utc()}: Checking status of task_id={batch.task_id}"
                    )
                    task = self.globus_config.transfer_client.get_task(batch.task_id)
                    batch.task_status = task["status"]
                    logger.debug(
                        f"{ts_utc()}: task_id={batch.task_id} status={batch.task_status}"
                    )
                else:
                    logger.debug(
                        f"{ts_utc()}: globus_config is not set up with a transfer client"
                    )

            # Now delete if successful
            if (not batch.is_globus) or (batch.task_status == "SUCCEEDED"):
                # The files were transferred successfully, so delete them
                logger.info(
                    f"{ts_utc()}: Deleting {len(batch.file_paths)} files from successful transfer"
                )
                batch.delete_files()
                logger.debug("Deletion completed")
                batch.file_paths = []  # Mark as processed
