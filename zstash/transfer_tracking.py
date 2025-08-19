import os
from typing import List, Optional

from globus_sdk import TransferClient, TransferData
from globus_sdk.services.transfer.response.iterable import IterableTransferResponse

from .settings import logger
from .utils import ts_utc


# Globus specific #############################################################
class GlobusTransfer(object):
    def __init__(self):
        self.transfer_data: Optional[TransferData] = None
        self.task_id: Optional[str] = None
        # https://docs.globus.org/api/transfer/task/#task_fields
        # ACTIVE, SUCCEEDED, FAILED, INACTIVE
        self.task_status: Optional[str] = None
        logger.debug(f"{ts_utc()}: GlobusTransfer initialized")


class GlobusTransferCollection(object):
    def __init__(self):
        # Attributes common to all the transfers
        self.remote_endpoint: Optional[str] = None
        self.local_endpoint: Optional[str] = None
        self.transfer_client: Optional[TransferClient] = None
        self.archive_directory_listing: Optional[IterableTransferResponse] = None

        self.transfers: List[GlobusTransfer] = (
            []
        )  # TODO: Replace with collections.deque?
        self.cumulative_tarfiles_pushed: int = 0
        logger.debug(f"{ts_utc()}: GlobusTransferCollection initialized")

    def get_most_recent_transfer(self) -> Optional[GlobusTransfer]:
        return self.transfers[-1] if self.transfers else None


# All Transfers ###############################################################
class HPSSTransferCollection(object):
    def __init__(self):
        self.prev_transfers: List[str] = []  # Can remove
        self.curr_transfers: List[str] = []  # Still using!
        logger.debug(f"{ts_utc()}: HPSSTransferCollection initialized")


def delete_transferred_files(htc: HPSSTransferCollection):
    logger.debug(f"{ts_utc()}: deleting transfered files {htc.prev_transfers}")
    for src_path in htc.prev_transfers:
        os.remove(src_path)
    htc.prev_transfers = htc.curr_transfers
    htc.curr_transfers = []
    logger.info(f"{ts_utc()}: prev_transfers has been set to {htc.prev_transfers}")
