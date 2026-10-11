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


# ---------------------------------------------------------------------------
# globus_transfer helpers
# ---------------------------------------------------------------------------


def _ensure_globus_config(transfer_manager: TransferManager, remote_ep: str) -> None:
    """Initialise transfer_manager.globus_config if it is not already set."""
    if (
        not transfer_manager.globus_config
        or not transfer_manager.globus_config.transfer_client
    ):
        transfer_manager.globus_config = globus_activate("globus://" + remote_ep)
    if (
        not transfer_manager.globus_config
        or not transfer_manager.globus_config.transfer_client
    ):
        sys.exit(1)


def _get_globus_config(transfer_manager: TransferManager) -> GlobusConfig:
    """
    Return transfer_manager.globus_config, asserting it is not None.
    Call this inside helpers that run after _ensure_globus_config has already
    been called (i.e. anywhere inside globus_transfer and its callees).
    """
    gc = transfer_manager.globus_config
    assert gc is not None, "globus_config must be set before calling Globus helpers"
    assert (
        gc.transfer_client is not None
    ), "transfer_client must be set before calling Globus helpers"
    return gc


def _verify_remote_file_exists(
    transfer_manager: TransferManager,
    remote_ep: str,
    remote_path: str,
    name: str,
) -> None:
    """
    For 'get' transfers: populate the cached directory listing if needed, then
    exit if the requested file is not present on the remote endpoint.
    """
    gc = _get_globus_config(transfer_manager)
    assert gc.transfer_client is not None
    if not gc.archive_directory_listing:
        gc.archive_directory_listing = gc.transfer_client.operation_ls(
            gc.remote_endpoint, remote_path
        )
    if not file_exists(gc.archive_directory_listing, name):
        logger.error(
            "Remote file globus://{}{}/{} does not exist".format(
                remote_ep, remote_path, name
            )
        )
        sys.exit(1)


def _add_file_to_current_batch(
    transfer_manager: TransferManager,
    remote_ep: str,
    remote_path: str,
    name: str,
    transfer_type: str,
) -> None:
    """
    Add *name* to the TransferData on the most recent batch, creating a new
    TransferData object if the batch does not yet have one.
    """
    gc = _get_globus_config(transfer_manager)
    assert (
        gc.local_endpoint is not None
    ), "local_endpoint must be set after globus_activate"
    assert (
        gc.remote_endpoint is not None
    ), "remote_endpoint must be set after globus_activate"
    local_endpoint: str = gc.local_endpoint
    remote_endpoint: str = gc.remote_endpoint

    mrb = transfer_manager.get_most_recent_batch()
    if mrb is None:
        raise RuntimeError(
            "No batch exists; hpss_transfer() must create one before calling globus_transfer()."
        )

    label = get_label(remote_path, name)
    if mrb.transfer_data is None:
        mrb.transfer_data = create_TransferData(
            transfer_type,
            local_endpoint,
            remote_endpoint,
            gc.transfer_client,
            label,
        )

    add_file_to_TransferData(
        transfer_type,
        local_endpoint,
        remote_endpoint,
        remote_path,
        name,
        mrb.transfer_data,
        label,
    )


def _should_defer_submission(
    transfer_manager: TransferManager,
    non_blocking: bool,
) -> bool:
    """
    Check the status of the previously submitted Globus task (if any).

    Returns True if submission should be deferred because the previous task is
    still ACTIVE and we are in non-blocking mode.

    Raises RuntimeError if we are in blocking mode but the previous task is
    somehow still ACTIVE (that would indicate a bug in the blocking-wait logic).
    """
    mrb = transfer_manager.get_most_recent_batch()
    if mrb is None or not mrb.task_id:
        # No previous submission to worry about.
        return False

    gc = _get_globus_config(transfer_manager)
    assert gc.transfer_client is not None
    task = gc.transfer_client.get_task(mrb.task_id)
    mrb.task_status = TaskStatus.convert_from_status_from_globus_sdk(task)

    if mrb.task_status == TaskStatus.ACTIVE:
        if non_blocking:
            # The previous batch is still transferring; accumulate this file into
            # the pending TransferData and come back to it later.
            logger.info(
                f"{ts_utc()}: Previous task_id {mrb.task_id} still ACTIVE; "
                f"deferring submission (non-blocking mode)."
            )
            return True
        else:
            error_str = (
                "task_status='ACTIVE' in blocking mode — the previous transfer "
                "should have completed via globus_block_wait before reaching here."
            )
            logger.error(error_str)
            raise RuntimeError(error_str)

    if mrb.task_status == TaskStatus.SUCCEEDED:
        src_ep = task["source_endpoint_id"]
        dst_ep = task["destination_endpoint_id"]
        label = task["label"]
        logger.info(
            f"{ts_utc()}: Previous task_id {mrb.task_id} SUCCEEDED "
            f"(from {src_ep} to {dst_ep}: {label}). Proceeding with next submission."
        )
    else:
        # INACTIVE, FAILED, or an unexpected status.  The previous transfer is
        # effectively terminal; log a warning and proceed with the new submission.
        logger.warning(
            f"{ts_utc()}: Previous task_id {mrb.task_id} has unexpected "
            f"status={mrb.task_status}; proceeding anyway."
        )

    return False


def _submit_current_batch(transfer_manager: TransferManager) -> str:
    """
    Submit the TransferData that has been accumulated on the most recent batch.
    Returns the new Globus task_id.
    """
    mrb = transfer_manager.get_most_recent_batch()
    if mrb is None or mrb.transfer_data is None:
        raise RuntimeError("No pending TransferData to submit.")

    update_cumulative_tarfiles_pushed(transfer_manager, mrb.transfer_data)

    logger.info(f"{ts_utc()}: Submitting Globus transfer: {mrb.transfer_data['label']}")
    try:
        gc = _get_globus_config(transfer_manager)
        task = submit_transfer_with_checks(gc.transfer_client, mrb.transfer_data)
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

    task_id: str = task.get("task_id")
    logger.info(
        f"{ts_utc()}: Submitted transfer, new task_id={task_id} "
        f"(label: {mrb.transfer_data['label']})"
    )

    if not transfer_manager.batches:
        raise RuntimeError("transfer_manager has no batches after submission.")
    transfer_manager.batches[-1].task_id = task_id
    transfer_manager.batches[-1].task_status = TaskStatus.SUBMITTED

    return task_id


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def globus_transfer(
    transfer_manager: TransferManager,
    remote_ep: str,
    remote_path: str,
    name: str,
    transfer_type: str,
    non_blocking: bool,
) -> TaskStatus:
    """
    Transfer a single file to or from a Globus endpoint.

    For 'put' (non-blocking): the file is added to the pending TransferData.
      Submission is deferred while the previous Globus task is still ACTIVE.
      When the previous task finishes (or there is none), the accumulated batch
      is submitted as one Globus task.

    For 'put' (blocking): the batch is submitted immediately after each file and
      we wait for it to complete before returning.

    For 'get': the file is submitted immediately and we always wait for completion.
    """
    logger.info(f"{ts_utc()}: globus_transfer() called for name={name!r}")
    logger.debug(f"{ts_utc()}: non_blocking={non_blocking}")

    _ensure_globus_config(transfer_manager, remote_ep)

    if transfer_type == "get":
        _verify_remote_file_exists(transfer_manager, remote_ep, remote_path, name)

    _add_file_to_current_batch(
        transfer_manager, remote_ep, remote_path, name, transfer_type
    )

    if _should_defer_submission(transfer_manager, non_blocking):
        return TaskStatus.ACTIVE

    task_id = _submit_current_batch(transfer_manager)
    gc = _get_globus_config(transfer_manager)

    if transfer_type == "get":
        # 'get' transfers always block until complete.
        globus_wait(gc.transfer_client, task_id)
        return TaskStatus.SUCCEEDED

    if not non_blocking:
        # Blocking 'put': wait for this task before processing the next tar.
        status = globus_block_wait(gc.transfer_client, task_id=task_id)
        transfer_manager.batches[-1].task_status = status
        return status

    return TaskStatus.SUBMITTED


# ---------------------------------------------------------------------------
# Wait helpers
# ---------------------------------------------------------------------------


def globus_block_wait(
    transfer_client: TransferClient,
    task_id: str,
    wait_timeout: int = 7200,  # 7200/3600 = 2 hours
    max_retries: int = 5,
) -> TaskStatus:
    """
    Block until the given Globus task reaches a terminal state, or until
    max_retries * wait_timeout seconds have elapsed.

    Polls every 10 seconds; reports progress every wait_timeout seconds.
    Default limits: report every 2 hours, give up after 5 × 2 = 10 hours.
    """
    logger.info(f"{ts_utc()}: Blocking wait started for task_id={task_id}")
    task_status: TaskStatus = TaskStatus.UNKNOWN
    retry_count: int = 0
    while retry_count < max_retries:
        try:
            logger.info(
                f"{ts_utc()}: task_wait attempt {retry_count + 1} of {max_retries}"
            )
            # task_wait returns True when the task has reached a terminal state,
            # False if it is still ACTIVE after `timeout` seconds.
            task_is_terminal: bool = transfer_client.task_wait(
                task_id, timeout=wait_timeout, polling_interval=10
            )
            if task_is_terminal:
                curr_task: GlobusHTTPResponse = transfer_client.get_task(task_id)
                task_status = TaskStatus.convert_from_status_from_globus_sdk(curr_task)
                if task_status == TaskStatus.SUCCEEDED:
                    break
                elif task_status == TaskStatus.FAILED:
                    logger.warning(
                        f"{ts_utc()}: task_id={task_id} FAILED; no point retrying."
                    )
                    break
                else:
                    logger.warning(
                        f"{ts_utc()}: task_id={task_id} reached unexpected terminal "
                        f"status={task_status}; will retry up to max_retries."
                    )
            logger.info(f"{ts_utc()}: task_wait returned (not yet terminal)")
        except Exception as e:
            logger.error(f"Unexpected Exception: {e}")
        finally:
            retry_count += 1
            logger.info(
                f"{ts_utc()}: blocking wait retry_count={retry_count}/{max_retries}, "
                f"timeout={wait_timeout}s"
            )

    if retry_count == max_retries:
        logger.info(
            f"{ts_utc()}: Exhausted {max_retries} wait attempts of {wait_timeout}s each"
        )
        task_status = TaskStatus.EXHAUSTED_TIMEOUT_RETRIES

    logger.info(
        f"{ts_utc()}: Blocking wait ended for task_id={task_id}, status={task_status}"
    )
    return task_status


def globus_wait(transfer_client: TransferClient, task_id: str):
    """
    Poll until the given Globus task reaches a terminal state, then log the outcome.
    Exits the process on API errors.
    """
    try:
        # Poll every 20 seconds; re-poll indefinitely until terminal.
        while not transfer_client.task_wait(task_id, timeout=300, polling_interval=20):
            pass

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


# ---------------------------------------------------------------------------
# globus_finalize helpers
# ---------------------------------------------------------------------------


def _submit_pending_transfer_data(
    transfer_client: TransferClient,
    transfer_manager: TransferManager,
) -> Optional[str]:
    """
    If the most recent batch has unsubmitted TransferData, submit it and return
    the new task_id.  Returns None if there is nothing pending.
    """
    transfer: Optional[TransferBatch] = transfer_manager.get_most_recent_batch()
    if not transfer or not transfer.transfer_data:
        return None

    update_cumulative_tarfiles_pushed(transfer_manager, transfer.transfer_data)

    logger.info(
        f"{ts_utc()}: Submitting final pending transfer: {transfer.transfer_data['label']}"
    )
    try:
        last_task = submit_transfer_with_checks(transfer_client, transfer.transfer_data)
        task_id = last_task.get("task_id")

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
    Return (ordered unique task_ids, task_id -> first-seen batch mapping).
    Skips batches that have already had their files deleted (local_paths_to_delete
    is empty), unless keep=True in which case deletion is never tracked.
    """
    task_ids: List[str] = []
    seen: Set[str] = set()
    task_to_batch: Dict[str, TransferBatch] = {}

    for batch in transfer_manager.batches:
        if not keep:
            # NOTE: This is always true if `keep` is set,
            # since we never track files for deletion if `keep` is set.
            already_deleted: bool = not batch.local_paths_to_delete
            if already_deleted:
                continue

        if (not batch.is_globus) or (not batch.task_id):
            continue

        tid: str = batch.task_id
        if tid in seen:
            continue

        seen.add(tid)
        task_ids.append(tid)
        task_to_batch[tid] = batch

    # Always include extra_task_id (e.g., a just-submitted transfer that may not
    # yet be reflected in the batches list).
    if extra_task_id and (extra_task_id not in seen):
        task_ids.append(extra_task_id)

    return task_ids, task_to_batch


def _refresh_batch_status(
    transfer_client: TransferClient,
    task_id: str,
    task_to_batch: Dict[str, TransferBatch],
) -> Optional[TaskStatus]:
    """
    Fetch the current Globus status for task_id and update the corresponding
    batch.  Returns the status, or None if the fetch fails.
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
            f"{ts_utc()}: Could not fetch status for task_id={task_id}; "
            f"will wait anyway. ({e})"
        )
        return None


def _wait_for_all_tasks(
    transfer_client: TransferClient,
    task_ids: List[str],
    task_to_batch: Dict[str, TransferBatch],
) -> None:
    """
    For each outstanding Globus task: refresh its status; if it has not already
    succeeded, block until it reaches a terminal state; then refresh once more
    so the batch status is accurate for the subsequent deletion step.
    """
    for tid in task_ids:
        status = _refresh_batch_status(transfer_client, tid, task_to_batch)
        if status == TaskStatus.SUCCEEDED:
            logger.info(f"{ts_utc()}: task_id={tid} already SUCCEEDED; skipping wait")
            continue

        logger.info(f"{ts_utc()}: Waiting for transfer task_id={tid} (status={status})")
        globus_wait(transfer_client, tid)

        # Refresh once more so deletion logic sees the final status.
        _refresh_batch_status(transfer_client, tid, task_to_batch)


def _prune_empty_batches(transfer_manager: TransferManager) -> None:
    """Remove batches that have no remaining files to manage."""
    before = len(transfer_manager.batches)
    transfer_manager.batches = [
        b for b in transfer_manager.batches if b.local_paths_to_delete
    ]
    after = len(transfer_manager.batches)
    if after != before:
        logger.debug(f"{ts_utc()}: Pruned {before - after} empty transfer batches")


def globus_finalize(transfer_manager: TransferManager, keep: bool) -> None:
    """
    Called once at the end of a create/update run to flush any remaining
    pending transfers and wait for all outstanding Globus tasks to complete.

    Steps:
      1. Submit any TransferData that was accumulated but not yet sent.
      2. Collect the task_ids of all batches that still have files to delete.
      3. Wait for every outstanding task to reach a terminal state.
      4. Delete the local tar files for successfully completed transfers.
      5. Prune batches that no longer have any files to manage.
    """
    if transfer_manager.globus_config is None:
        logger.debug("No GlobusConfig object provided for finalization")
        return
    if transfer_manager.globus_config.transfer_client is None:
        logger.debug("GlobusConfig provided but transfer_client is None")
        return

    transfer_client: TransferClient = transfer_manager.globus_config.transfer_client

    # 1. Submit any pending (unsubmitted) TransferData.
    last_task_id: Optional[str] = _submit_pending_transfer_data(
        transfer_client, transfer_manager
    )

    # 2. Collect all task_ids that still have associated local files.
    task_ids: List[str]
    task_to_batch: Dict[str, TransferBatch]
    task_ids, task_to_batch = _collect_globus_task_ids(
        transfer_manager, last_task_id, keep
    )

    # 3. Wait for every outstanding task.
    _wait_for_all_tasks(transfer_client, task_ids, task_to_batch)

    # 4. Delete local tar files from succeeded transfers.
    transfer_manager.delete_successfully_transferred_files()

    # 5. Remove empty (fully-processed) batches.
    _prune_empty_batches(transfer_manager)
