import os
from typing import List, Optional

from globus_sdk import TransferClient, TransferData
from globus_sdk.services.transfer.response.iterable import IterableTransferResponse

from .settings import logger
from .utils import ts_utc


class GlobusConfig:
    """Globus connection configuration"""

    def __init__(self):
        self.remote_endpoint: str
        self.local_endpoint: str
        self.transfer_client: TransferClient
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
        new_batch_list: List[TransferBatch] = []
        for batch in self.batches:
            # Check if this is a Globus transfer that needs status update
            if batch.is_globus and batch.task_id and (batch.task_status != "SUCCEEDED"):
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

            # Now delete if successful
            if (not batch.is_globus) or (batch.task_status == "SUCCEEDED"):
                # The files were transferred successfully.
                # So, we can delete them.
                logger.info(
                    f"{ts_utc()}: Deleting {len(batch.file_paths)} files from successful transfer"
                )
                batch.delete_files()
            else:
                # Keep tracking this batch - not yet successful
                new_batch_list.append(batch)
        # We don't need to keep tracking batches that have been both:
        # 1. transferred successfully
        # 2. been deleted
        self.batches = new_batch_list
