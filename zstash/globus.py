from __future__ import absolute_import, print_function

import sys
from typing import List, Optional

from globus_sdk import TransferAPIError, TransferClient
from globus_sdk.response import GlobusHTTPResponse
from six.moves.urllib.parse import urlparse

from .globus_utils import (
    HPSS_ENDPOINT_MAP,
    check_state_files,
    get_local_endpoint_id,
    get_transfer_client_with_auth,
    set_up_TransferData,
    submit_transfer_with_checks,
)
from .settings import logger
from .transfer_tracking import GlobusConfig, TransferBatch, TransferManager
from .utils import ts_utc


def globus_activate(
    hpss: str, globus_config: Optional[GlobusConfig] = None
) -> Optional[GlobusConfig]:
    """
    Read the local globus endpoint UUID from ~/.zstash.ini.
    If the ini file does not exist, create an ini file with empty values,
    and try to find the local endpoint UUID based on the FQDN
    """

    url = urlparse(hpss)
    if url.scheme != "globus":
        return None
    if globus_config is None:
        globus_config = GlobusConfig()
    check_state_files()
    globus_config.remote_endpoint = url.netloc
    globus_config.local_endpoint = get_local_endpoint_id(globus_config.local_endpoint)
    upper_remote_ep = globus_config.remote_endpoint.upper()
    if upper_remote_ep in HPSS_ENDPOINT_MAP.keys():
        globus_config.remote_endpoint = HPSS_ENDPOINT_MAP[upper_remote_ep]
    both_endpoints: List[Optional[str]] = [
        globus_config.local_endpoint,
        globus_config.remote_endpoint,
    ]
    globus_config.transfer_client = get_transfer_client_with_auth(both_endpoints)
    for ep_id in both_endpoints:
        r = globus_config.transfer_client.endpoint_autoactivate(
            ep_id, if_expires_in=600
        )
        if r.get("code") == "AutoActivationFailed":
            logger.error(
                f"The {ep_id} endpoint is not activated or the current activation expires soon. Please go to https://app.globus.org/file-manager/collections/{ep_id} and (re)activate the endpoint."
            )
            sys.exit(1)
    return globus_config


def file_exists(archive_directory_listing, name: str) -> bool:
    for entry in archive_directory_listing:
        if entry.get("name") == name:
            return True
    return False


# C901 'globus_transfer' is too complex (20)
def globus_transfer(  # noqa: C901
    transfer_manager: TransferManager,
    remote_ep: str,
    remote_path: str,
    name: str,
    transfer_type: str,
    non_blocking: bool,
):

    logger.info(f"{ts_utc()}: Entered globus_transfer() for name = {name}")
    logger.debug(f"{ts_utc()}: non_blocking = {non_blocking}")
    if (not transfer_manager.globus_config) or (
        not transfer_manager.globus_config.transfer_client
    ):
        transfer_manager.globus_config = globus_activate("globus://" + remote_ep)
    if (not transfer_manager.globus_config) or (
        not transfer_manager.globus_config.transfer_client
    ):
        sys.exit(1)

    if transfer_type == "get":
        if not transfer_manager.globus_config.archive_directory_listing:
            transfer_manager.globus_config.archive_directory_listing = (
                transfer_manager.globus_config.transfer_client.operation_ls(
                    transfer_manager.globus_config.remote_endpoint, remote_path
                )
            )
        if not file_exists(
            transfer_manager.globus_config.archive_directory_listing, name
        ):
            logger.error(
                "Remote file globus://{}{}/{} does not exist".format(
                    remote_ep, remote_path, name
                )
            )
            sys.exit(1)

    mrt: Optional[TransferBatch] = transfer_manager.get_most_recent_transfer()
    transfer_data = set_up_TransferData(
        transfer_type,
        transfer_manager.globus_config.local_endpoint,
        transfer_manager.globus_config.remote_endpoint,
        remote_path,
        name,
        transfer_manager.globus_config.transfer_client,
        mrt.transfer_data if mrt else None,
    )

    task: GlobusHTTPResponse
    try:
        if mrt and mrt.task_id:
            task = transfer_manager.globus_config.transfer_client.get_task(mrt.task_id)
            mrt.task_status = task["status"]
            # one  of {ACTIVE, SUCCEEDED, FAILED, CANCELED, PENDING, INACTIVE}
            # NOTE: How we behave here depends upon whether we want to support mutliple active transfers.
            # Presently, we do not, except inadvertantly (if status == PENDING)
            if mrt.task_status == "ACTIVE":
                logger.info(
                    f"{ts_utc()}: Previous task_id {mrt.task_id} Still Active. Returning ACTIVE."
                )
                # Don't return early - continue to submit the new transfer
                # The previous transfer will complete asynchronously
            elif mrt.task_status == "SUCCEEDED":
                logger.info(
                    f"{ts_utc()}: Previous task_id {mrt.task_id} status = SUCCEEDED."
                )
                src_ep = task["source_endpoint_id"]
                dst_ep = task["destination_endpoint_id"]
                label = task["label"]
                ts = ts_utc()
                logger.info(
                    f"{ts}:Globus transfer {mrt.task_id}, from {src_ep} to {dst_ep}: {label} succeeded"
                )
            else:
                logger.error(
                    f"{ts_utc()}: Previous task_id {mrt.task_id} status = {mrt.task_status}."
                )

        # DEBUG: review accumulated items in TransferData
        logger.info(f"{ts_utc()}: TransferData: accumulated items:")
        attribs = transfer_data.__dict__
        for item in attribs["data"]["DATA"]:
            if item["DATA_TYPE"] == "transfer_item":
                transfer_manager.cumulative_tarfiles_pushed += 1
                print(
                    f"   (routine)  PUSHING (#{transfer_manager.cumulative_tarfiles_pushed}) STORED source item: {item['source_path']}",
                    flush=True,
                )

        # Submit the current transfer_data
        logger.info(f"{ts_utc()}: DIVING: Submit Transfer for {transfer_data['label']}")
        task = submit_transfer_with_checks(
            transfer_manager.globus_config.transfer_client, transfer_data
        )
        task_id = task.get("task_id")
        # NOTE: This log message is misleading. If we have accumulated multiple tar files for transfer,
        # the "lable" given here refers only to the LAST tarfile in the TransferData list.
        logger.info(
            f"{ts_utc()}: SURFACE Submit Transfer returned new task_id = {task_id} for label {transfer_data['label']}"
        )

        # Create new transfer record with the task info
        new_transfer = TransferBatch()
        new_transfer.file_paths = (
            mrt.file_paths if mrt else []
        )  # Copy from the batch being submitted
        new_transfer.task_id = task_id
        new_transfer.task_status = "UNKNOWN"
        new_transfer.is_globus = True
        transfer_manager.batches.append(new_transfer)

        # Nullify the submitted transfer data structure so that a new one will be created on next call.
        # Reset for building the next batch
        transfer_data = None
    except TransferAPIError as e:
        if e.code == "NoCredException":
            logger.error(
                "{}. Please go to https://app.globus.org/endpoints and activate the endpoint.".format(
                    e.message
                )
            )
        else:
            logger.error(e)
        sys.exit(1)
    except Exception as e:
        logger.error("Exception: {}".format(e))
        sys.exit(1)

    new_mrt: Optional[TransferBatch] = transfer_manager.get_most_recent_transfer()
    # test for blocking on new task_id
    task_status = "UNKNOWN"
    if new_mrt and new_mrt.task_id:
        if not non_blocking:
            new_mrt.task_status = globus_block_wait(
                transfer_manager.globus_config.transfer_client,
                task_id=new_mrt.task_id,
                wait_timeout=7200,
                max_retries=5,
            )
        else:
            logger.info(
                f"{ts_utc()}: NO BLOCKING (task_wait) for task_id {new_mrt.task_id}"
            )

    if transfer_type == "put":
        return task_status

    if transfer_type == "get" and task_id:
        globus_wait(transfer_manager.globus_config.transfer_client, task_id)

    return task_status


def globus_block_wait(
    transfer_client: TransferClient,
    task_id: str,
    wait_timeout: int,
    max_retries: int,
):

    # poll every "polling_interval" seconds to speed up small transfers.  Report every 2 hours, stop waiting aftert 5*2 = 10 hours
    logger.info(
        f"{ts_utc()}: BLOCKING START: invoking task_wait for task_id = {task_id}"
    )
    task_status = "UNKNOWN"
    retry_count = 0
    while retry_count < max_retries:
        try:
            # Wait for the task to complete
            logger.info(
                f"{ts_utc()}: on task_wait try {retry_count + 1} out of {max_retries}"
            )
            transfer_client.task_wait(
                task_id, timeout=wait_timeout, polling_interval=10
            )
            logger.info(f"{ts_utc()}: done with wait")
        except Exception as e:
            logger.error(f"Unexpected Exception: {e}")
        else:
            curr_task = transfer_client.get_task(task_id)
            task_status = curr_task["status"]
            if task_status == "SUCCEEDED":
                break
        finally:
            retry_count += 1
            logger.info(
                f"{ts_utc()}: BLOCKING retry_count = {retry_count} of {max_retries} of timeout {wait_timeout} seconds"
            )

    if retry_count == max_retries:
        logger.info(
            f"{ts_utc()}: BLOCKING EXHAUSTED {max_retries} of timeout {wait_timeout} seconds"
        )
        task_status = "EXHAUSTED_TIMEOUT_RETRIES"

    logger.info(
        f"{ts_utc()}: BLOCKING ENDS: task_id {task_id} returned from task_wait with status {task_status}"
    )

    return task_status


def globus_wait(transfer_client: TransferClient, task_id: str):
    try:
        """
        A Globus transfer job (task) can be in one of the three states:
        ACTIVE, SUCCEEDED, FAILED. The script every 20 seconds polls a
        status of the transfer job (task) from the Globus Transfer service,
        with 20 second timeout limit. If the task is ACTIVE after time runs
        out 'task_wait' returns False, and True otherwise.
        """
        while not transfer_client.task_wait(task_id, timeout=300, polling_interval=20):
            pass
        """
        The Globus transfer job (task) has been finished (SUCCEEDED or FAILED).
        Check if the transfer SUCCEEDED or FAILED.
        """
        task = transfer_client.get_task(task_id)
        if task["status"] == "SUCCEEDED":
            src_ep = task["source_endpoint_id"]
            dst_ep = task["destination_endpoint_id"]
            label = task["label"]
            logger.info(
                "Globus transfer {}, from {} to {}: {} succeeded".format(
                    task_id, src_ep, dst_ep, label
                )
            )
        else:
            logger.error("Transfer FAILED")
    except TransferAPIError as e:
        if e.code == "NoCredException":
            logger.error(
                "{}. Please go to https://app.globus.org/endpoints and activate the endpoint.".format(
                    e.message
                )
            )
        else:
            logger.error(e)
        sys.exit(1)
    except Exception as e:
        logger.error("Exception: {}".format(e))
        sys.exit(1)


def globus_finalize(transfer_manager: TransferManager, non_blocking: bool = False):

    last_task_id = None

    if transfer_manager.globus_config is None:
        logger.warning("No GlobusConfig object provided for finalization")
        return

    transfer: Optional[TransferBatch] = transfer_manager.get_most_recent_transfer()
    if transfer:
        # Check if there's any pending transfer data that hasn't been submitted yet
        if transfer.transfer_data:
            # DEBUG: review accumulated items in TransferData
            logger.info(f"{ts_utc()}: FINAL TransferData: accumulated items:")
            attribs = transfer.transfer_data.__dict__
            for item in attribs["data"]["DATA"]:
                if item["DATA_TYPE"] == "transfer_item":
                    transfer_manager.cumulative_tarfiles_pushed += 1
                    print(
                        f"    (finalize) PUSHING ({transfer_manager.cumulative_tarfiles_pushed}) source item: {item['source_path']}",
                        flush=True,
                    )

            # SUBMIT new transfer here
            logger.info(
                f"{ts_utc()}: DIVING: Submit Transfer for {transfer.transfer_data['label']}"
            )
            try:
                last_task = submit_transfer_with_checks(
                    transfer_manager.globus_config.transfer_client,
                    transfer.transfer_data,
                )
                last_task_id = last_task.get("task_id")
            except TransferAPIError as e:
                if e.code == "NoCredException":
                    logger.error(
                        "{}. Please go to https://app.globus.org/endpoints and activate the endpoint.".format(
                            e.message
                        )
                    )
                else:
                    logger.error(e)
                sys.exit(1)
            except Exception as e:
                logger.error("Exception: {}".format(e))
                sys.exit(1)

        # Wait for any submitted transfers to complete
        # In non-blocking mode, this ensures index.db and any accumulated tar files complete
        # In blocking mode, this is redundant but harmless
        skip_last_wait: bool = False
        if transfer and transfer.task_id:
            if transfer.task_id == last_task_id:
                skip_last_wait = (
                    True  # No reason to call globus_wait twice on the same task_id
                )
            logger.info(
                f"{ts_utc()}: Waiting for transfer task_id={transfer.task_id} to complete"
            )
            globus_wait(
                transfer_manager.globus_config.transfer_client, transfer.task_id
            )
        if last_task_id and (not skip_last_wait):
            logger.info(
                f"{ts_utc()}: Waiting for last transfer task_id={last_task_id} to complete"
            )
            globus_wait(transfer_manager.globus_config.transfer_client, last_task_id)

        # Clean up tar files that were queued for deletion
        transfer_manager.delete_successfully_transferred_files()
