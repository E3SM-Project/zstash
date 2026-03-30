from __future__ import absolute_import, print_function

import sys
from typing import Dict, List, Optional, Set, Tuple

from globus_sdk import TransferAPIError, TransferClient, TransferData
from globus_sdk.response import GlobusHTTPResponse
from globus_sdk.services.transfer.response.iterable import IterableTransferResponse
from six.moves.urllib.parse import urlparse

from .globus_utils import (
    HPSS_ENDPOINT_MAP,
    add_file_to_TransferData,
    check_state_files,
    create_TransferData,
    get_label,
    get_local_endpoint_id,
    get_transfer_client_with_auth,
    submit_transfer_with_checks,
)
from .settings import logger
from .transfer_tracking import GlobusConfig, TaskStatus, TransferBatch, TransferManager
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


def file_exists(archive_directory_listing: IterableTransferResponse, name: str) -> bool:
    for entry in archive_directory_listing:
        if entry.get("name") == name:
            return True
    return False


def update_cumulative_tarfiles_pushed(
    transfer_manager: TransferManager, transfer_data: TransferData
) -> None:
    logger.info(f"{ts_utc()}: TransferData: accumulated items:")
    attribs = transfer_data.__dict__
    for item in attribs["data"]["DATA"]:
        if item["DATA_TYPE"] == "transfer_item":
            transfer_manager.cumulative_tarfiles_pushed += 1
            print(
                f"PUSHED (#{transfer_manager.cumulative_tarfiles_pushed}) tars, STORED source item: {item['source_path']}",
                flush=True,
            )


# C901 'globus_transfer' is too complex (20)
def globus_transfer(  # noqa: C901
    transfer_manager: TransferManager,
    remote_ep: str,
    remote_path: str,
    name: str,
    transfer_type: str,
    non_blocking: bool,
) -> TaskStatus:

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

    mrb: Optional[TransferBatch] = transfer_manager.get_most_recent_batch()
    if not mrb:
        raise RuntimeError(
            "The transfer manager should always have at least one batch by the time globus_transfer is called, however, the batch list is empty."
        )

    if transfer_manager.globus_config.local_endpoint:
        local_endpoint: str = transfer_manager.globus_config.local_endpoint
    else:
        raise ValueError("Local endpoint ID is not set.")
    if transfer_manager.globus_config.remote_endpoint:
        remote_endpoint: str = transfer_manager.globus_config.remote_endpoint
    else:
        raise ValueError("Remote endpoint ID is not set.")
    label: str = get_label(remote_path, name)
    transfer_data: TransferData
    if mrb.transfer_data:
        # We already have a TransferData for this batch.
        transfer_data = mrb.transfer_data
    else:
        # We need to create a new TransferData for this batch.
        transfer_data = create_TransferData(
            transfer_type,
            local_endpoint,
            remote_endpoint,
            transfer_manager.globus_config.transfer_client,
            label,
        )
    add_file_to_TransferData(
        transfer_type,
        local_endpoint,
        remote_endpoint,
        remote_path,
        name,
        transfer_data,
        label,
    )

    task: GlobusHTTPResponse
    try:
        if mrb.task_id:
            # This the current transfer task associated with the most recent batch.
            task = transfer_manager.globus_config.transfer_client.get_task(mrb.task_id)
            # Update the most recent batch's task_status based on the current status from Globus API.
            mrb.task_status = TaskStatus.convert_from_status_from_globus_sdk(task)
            if mrb.task_status == TaskStatus.ACTIVE:
                # The most recent transfer (mrb) is still active.
                logger.info(
                    f"{ts_utc()}: Previous task_id {mrb.task_id} Still Active. Returning ACTIVE."
                )
                if non_blocking:
                    # Globus allows up to 3 simulataneous transfers,
                    # but zstash is currently configured to only ever allow 1.
                    # If we're in this block, then we're already at 1 active transfer.
                    # We will therefore wait to submit a new transfer until it's done.
                    # So, we'll simply return and the next run of globus_transfer
                    # (i.e., on the next tar) will evaluate if the active transfer has finished.
                    return TaskStatus.ACTIVE
                else:
                    # If we're in this block, then the blocking wait
                    # for the previous transfer to finish was unsuccessful.
                    # This is an unexpected state and so we raise an error.
                    error_str: str = (
                        "task_status='ACTIVE', but in blocking mode, the previous transfer should have waited through globus_block_wait"
                    )
                    logger.error(error_str)
                    raise RuntimeError(error_str)
            elif mrb.task_status == TaskStatus.SUCCEEDED:
                logger.info(
                    f"{ts_utc()}: Previous task_id {mrb.task_id} status = SUCCEEDED."
                )
                src_ep = task["source_endpoint_id"]
                dst_ep = task["destination_endpoint_id"]
                label = task["label"]
                ts = ts_utc()
                logger.info(
                    f"{ts}:Globus transfer {mrb.task_id}, from {src_ep} to {dst_ep}: {label} succeeded"
                )
                # The previous transfer succeeded.
                # That means we can transfer the current batch now.
            else:
                # The previous transfer is in an unexpected state (i.e., "INACTIVE", "FAILED").
                # Either way, the previous transfer is effectively terminated,
                # so we will proceed with the current transfer attempt.
                # (I.e., we will not return yet).
                # Note: any status we manually set
                # (I.e., "UNKNOWN", "SUBMITTED", "EXHAUSTED_TIMEOUT_RETRIES") is NOT possible here,
                # because we're using `task["status"]` from the globus_sdk TransferClient.
                logger.error(
                    f"{ts_utc()}: Previous task_id {mrb.task_id} status = {mrb.task_status}."
                )

        update_cumulative_tarfiles_pushed(transfer_manager, transfer_data)

        logger.info(f"{ts_utc()}: DIVING: Submit Transfer for {transfer_data['label']}")
        # Submit the current transfer_data
        # ALWAYS submit. If we've gotten to this point, we're ready to submit.
        task = submit_transfer_with_checks(
            transfer_manager.globus_config.transfer_client, transfer_data
        )
        task_id = task.get("task_id")
        logger.info(
            f"{ts_utc()}: SURFACE Submit Transfer returned new task_id = {task_id}, with last tarfile having label: {transfer_data['label']}"
        )

        # Update the current batch with the task info
        # The batch was already created in hpss_transfer with files added to it
        # We just need to mark it as submitted
        if transfer_manager.batches:
            # Update these two fields of the most recent batch
            # (which is still available in this function as `mrb`).
            transfer_manager.batches[-1].task_id = task_id
            transfer_manager.batches[-1].task_status = TaskStatus.SUBMITTED
        else:
            # This block should be impossible to reach.
            # By now, we've ensured that `get_most_recent_batch()` returns a batch,
            # and we haven't removed any batches since then,
            # so there should always be at least one batch in `batches`.
            error_str = "transfer_manager has no batches"
            logger.error(error_str)
            raise RuntimeError(error_str)
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

    task_status: TaskStatus = TaskStatus.UNKNOWN
    if mrb.task_id:
        if not non_blocking:
            # If blocking, wait for the task to complete and get the final status,
            # before we proceed with any more transfers.
            mrb.task_status = globus_block_wait(
                transfer_manager.globus_config.transfer_client,
                task_id=mrb.task_id,
            )
            task_status = mrb.task_status
        else:
            logger.info(
                f"{ts_utc()}: NO BLOCKING (task_wait) for task_id {mrb.task_id}"
            )
    else:
        # This block should be impossible to reach.
        # By now, we've set `transfer_manager.batches[-1].task_id = task_id` or else raised an error, so `mrb.task_id` should always be set.
        error_str = "No task_id found for most recent batch after submission"
        logger.error(f"{ts_utc()}: {error_str}")
        raise RuntimeError(error_str)

    if transfer_type == "put":
        return task_status

    if transfer_type == "get" and task_id:
        globus_wait(transfer_manager.globus_config.transfer_client, task_id)

    return task_status


def globus_block_wait(
    transfer_client: TransferClient,
    task_id: str,
    wait_timeout: int = 7200,  # 7200/3600 = 2 hours
    max_retries: int = 5,
) -> TaskStatus:
    # Poll every "polling_interval" seconds to speed up small transfers.
    # Report every "wait_timeout" seconds, and stop waiting after "max_retries" reports.
    # By default: report every 2 hours, stop waiting after 5*2 = 10 hours
    logger.info(
        f"{ts_utc()}: BLOCKING START: invoking task_wait for task_id = {task_id}"
    )
    task_status: TaskStatus = TaskStatus.UNKNOWN
    retry_count: int = 0
    while retry_count < max_retries:
        try:
            logger.info(
                f"{ts_utc()}: on task_wait try {retry_count + 1} out of {max_retries}"
            )
            # Wait for the task to complete. This is what makes this function BLOCKING.
            # From https://globus-sdk-python.readthedocs.io/en/stable/services/transfer.html#globus_sdk.TransferClient.task_wait: Wait until a Task is complete or fails, with a time limit. If the task is “ACTIVE” after time runs out, returns False. Otherwise returns True.
            task_is_not_active: bool = transfer_client.task_wait(
                task_id, timeout=wait_timeout, polling_interval=10
            )
            if task_is_not_active:
                curr_task: GlobusHTTPResponse = transfer_client.get_task(task_id)
                task_status = TaskStatus.convert_from_status_from_globus_sdk(curr_task)
                if task_status == TaskStatus.SUCCEEDED:
                    break  # Break out of the while-loop. The transfer already succeeded, so no need to retry.
                elif task_status == TaskStatus.FAILED:
                    error_str = f"{ts_utc()}: task_wait returned True, but task_status={task_status} for task_id {task_id}. No reason to keep retrying now."
                    logger.warning(error_str)
                    # We still need to break, because no matter how long we wait now, nothing will change with the transfer status.
                    break
                else:
                    error_str = f"{ts_utc()}: task_wait returned True, but task_status={task_status} for task_id {task_id}. Will retry waiting until max_retries is reached."
                    logger.warning(error_str)
                    # Don't break -- continue retries
            logger.info(f"{ts_utc()}: done with wait")
        except Exception as e:
            logger.error(f"Unexpected Exception: {e}")
        finally:
            retry_count += 1
            logger.info(
                f"{ts_utc()}: BLOCKING retry_count = {retry_count} of {max_retries} of timeout {wait_timeout} seconds"
            )

    if retry_count == max_retries:
        logger.info(
            f"{ts_utc()}: BLOCKING EXHAUSTED {max_retries} of timeout {wait_timeout} seconds"
        )
        task_status = TaskStatus.EXHAUSTED_TIMEOUT_RETRIES

    logger.info(
        f"{ts_utc()}: BLOCKING ENDS: task_id {task_id} returned from task_wait with status {task_status}"
    )

    return task_status


def globus_wait(transfer_client: TransferClient, task_id: str):
    try:
        """
        A Globus transfer job (task) can be in one of the four states:
        {ACTIVE, SUCCEEDED, FAILED, INACTIVE}
        according to https://docs.globus.org/api/transfer/task/#task_fields.
        The script every 20 seconds polls a
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
        task: GlobusHTTPResponse = transfer_client.get_task(task_id)
        if TaskStatus.convert_from_status_from_globus_sdk(task) == TaskStatus.SUCCEEDED:
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


def _submit_pending_transfer_data(
    transfer_client: TransferClient,
    transfer_manager: TransferManager,
) -> Optional[str]:
    """
    If the most recent batch has unsubmitted TransferData, submit it and return task_id.
    Otherwise return None.
    """
    transfer: Optional[TransferBatch] = transfer_manager.get_most_recent_batch()
    if not transfer or not transfer.transfer_data:
        return None

    update_cumulative_tarfiles_pushed(transfer_manager, transfer.transfer_data)

    logger.info(
        f"{ts_utc()}: DIVING: Submit Transfer for {transfer.transfer_data['label']}"
    )
    try:
        last_task = submit_transfer_with_checks(transfer_client, transfer.transfer_data)
        task_id = last_task.get("task_id")

        # Best-effort: if this batch represents the submission, store the task_id.
        if task_id and transfer.is_globus and not transfer.task_id:
            transfer.task_id = task_id

        return task_id

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


def _collect_globus_task_ids(
    transfer_manager: TransferManager, extra_task_id: Optional[str], keep: bool
) -> Tuple[List[str], Dict[str, TransferBatch]]:
    """
    Return (ordered unique task_ids, task_id->batch mapping for first occurrence).
    """
    task_ids: List[str] = []
    seen: Set[str] = set()
    task_to_batch: Dict[str, TransferBatch] = {}

    for batch in transfer_manager.batches:
        if not keep:
            # NOTE: This is always true if `keep` is set,
            # since we never track files for deletion if `keep` is set.
            already_deleted: bool = not batch.file_paths
            if already_deleted:
                # This batch has already been processed and files deleted, so we can skip it.
                continue

        if (not batch.is_globus) or (not batch.task_id):
            continue

        # By this point, we know batch.task_id is not None
        tid: str = batch.task_id
        if tid in seen:
            continue

        seen.add(tid)
        task_ids.append(tid)
        task_to_batch[tid] = batch

    # Always include extra_task_id (e.g., just-submitted transfer),
    # even if not yet reflected in batches.
    if extra_task_id and (extra_task_id not in seen):
        task_ids.append(extra_task_id)

    return task_ids, task_to_batch


def _refresh_batch_status(
    transfer_client: TransferClient,
    task_id: str,
    task_to_batch: Dict[str, TransferBatch],
) -> Optional[TaskStatus]:
    """
    Fetch Globus task status and update corresponding batch.task_status if present.
    Returns status, or None if fetch fails.
    """
    try:
        task: GlobusHTTPResponse = transfer_client.get_task(task_id)
        status: TaskStatus = TaskStatus.convert_from_status_from_globus_sdk(task)
        batch: Optional[TransferBatch] = task_to_batch.get(task_id)
        if batch:
            batch.task_status = status
        return status
    except Exception as e:
        logger.warning(
            f"{ts_utc()}: Could not fetch status for task_id={task_id}; will wait anyway. ({e})"
        )
        return None


def _wait_for_all_tasks(
    transfer_client: TransferClient,
    task_ids: List[str],
    task_to_batch: Dict[str, TransferBatch],
) -> None:
    """
    For each task_id, refresh status; if not SUCCEEDED, block via globus_wait;
    then refresh status again for deletion logic.
    """
    for tid in task_ids:
        status = _refresh_batch_status(transfer_client, tid, task_to_batch)
        if status == TaskStatus.SUCCEEDED:
            logger.info(f"{ts_utc()}: task_id={tid} already SUCCEEDED; skipping wait")
            continue

        logger.info(
            f"{ts_utc()}: Waiting for transfer task_id={tid} to complete (status={status})"
        )
        globus_wait(transfer_client, tid)

        # After wait returns, task is terminal; refresh once more.
        _refresh_batch_status(transfer_client, tid, task_to_batch)


def _prune_empty_batches(transfer_manager: TransferManager) -> None:
    """
    Remove batches which have no remaining files to manage.

    Note: we only prune batches whose file_paths is empty, regardless of Globus/HPSS.
    That matches current semantics where file_paths=[] means "processed".
    """
    before = len(transfer_manager.batches)
    transfer_manager.batches = [b for b in transfer_manager.batches if b.file_paths]
    after = len(transfer_manager.batches)
    if after != before:
        logger.debug(f"{ts_utc()}: Pruned {before - after} empty transfer batches")


def globus_finalize(transfer_manager: TransferManager, keep: bool) -> None:
    if transfer_manager.globus_config is None:
        logger.debug("No GlobusConfig object provided for finalization")
        return
    if transfer_manager.globus_config.transfer_client is None:
        logger.debug("GlobusConfig provided but transfer_client is None")
        return

    # By this point, we know transfer_client is not None
    transfer_client: TransferClient = transfer_manager.globus_config.transfer_client

    last_task_id: Optional[str] = _submit_pending_transfer_data(
        transfer_client, transfer_manager
    )

    task_ids: List[str]
    task_to_batch: Dict[str, TransferBatch]
    task_ids, task_to_batch = _collect_globus_task_ids(
        transfer_manager, last_task_id, keep
    )

    _wait_for_all_tasks(transfer_client, task_ids, task_to_batch)

    transfer_manager.delete_successfully_transferred_files()

    _prune_empty_batches(transfer_manager)
