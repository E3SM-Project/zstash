from __future__ import absolute_import, print_function

import sys
from typing import List, Optional

from globus_sdk import TransferAPIError, TransferData
from globus_sdk.response import GlobusHTTPResponse
from six.moves.urllib.parse import urlparse

from .globus_utils import (
    HPSS_ENDPOINT_MAP,
    check_state_files,
    file_exists,
    get_local_endpoint_id,
    get_transfer_client_with_auth,
    globus_block_wait,
    globus_wait,
    set_up_TransferData,
    submit_transfer_with_checks,
)
from .settings import logger
from .transfer_tracking import (
    GlobusTransfer,
    GlobusTransferCollection,
    HPSSTransferCollection,
    delete_transferred_files,
)
from .utils import ts_utc


def globus_activate(
    hpss: str, gtc: Optional[GlobusTransferCollection] = None
) -> Optional[GlobusTransferCollection]:
    url = urlparse(hpss)
    if url.scheme != "globus":
        return None
    if gtc is None:
        gtc = GlobusTransferCollection()
    check_state_files()
    gtc.remote_endpoint = url.netloc
    gtc.local_endpoint = get_local_endpoint_id(gtc.local_endpoint)
    upper_remote_ep = gtc.remote_endpoint.upper()
    if upper_remote_ep in HPSS_ENDPOINT_MAP.keys():
        gtc.remote_endpoint = HPSS_ENDPOINT_MAP.get(upper_remote_ep)
    both_endpoints: List[Optional[str]] = [gtc.local_endpoint, gtc.remote_endpoint]
    gtc.transfer_client = get_transfer_client_with_auth(both_endpoints)
    for ep_id in both_endpoints:
        r = gtc.transfer_client.endpoint_autoactivate(ep_id, if_expires_in=600)
        if r.get("code") == "AutoActivationFailed":
            logger.error(
                f"The {ep_id} endpoint is not activated or the current activation expires soon. Please go to https://app.globus.org/file-manager/collections/{ep_id} and (re)activate the endpoint."
            )
            sys.exit(1)
    return gtc


# C901 'globus_transfer' is too complex (20)
def globus_transfer(  # noqa: C901
    gtc: Optional[GlobusTransferCollection],
    remote_ep: str,
    remote_path: str,
    name: str,
    transfer_type: str,
    non_blocking: bool,
):
    logger.info(f"{ts_utc()}: Entered globus_transfer() for name = {name}")
    logger.debug(f"{ts_utc()}: non_blocking = {non_blocking}")
    if (not gtc) or (not gtc.transfer_client):
        gtc = globus_activate("globus://" + remote_ep)
    if (not gtc) or (not gtc.transfer_client):
        sys.exit(1)

    if transfer_type == "get":
        if not gtc.archive_directory_listing:
            gtc.archive_directory_listing = gtc.transfer_client.operation_ls(
                gtc.remote_endpoint, remote_path
            )
        if not file_exists(gtc.archive_directory_listing, name):
            logger.error(
                "Remote file globus://{}{}/{} does not exist".format(
                    remote_ep, remote_path, name
                )
            )
            sys.exit(1)

    mrt: Optional[GlobusTransfer] = gtc.get_most_recent_transfer()
    transfer_data: TransferData = set_up_TransferData(
        transfer_type,
        gtc.local_endpoint,
        gtc.remote_endpoint,
        remote_path,
        name,
        gtc.transfer_client,
        mrt.transfer_data if mrt else None,
    )

    task: GlobusHTTPResponse
    try:
        if mrt and mrt.task_id:
            task = gtc.transfer_client.get_task(mrt.task_id)
            mrt.task_status = task["status"]
            # one  of {ACTIVE, SUCCEEDED, FAILED, CANCELED, PENDING, INACTIVE}
            # NOTE: How we behave here depends upon whether we want to support mutliple active transfers.
            # Presently, we do not, except inadvertantly (if status == PENDING)
            if mrt.task_status == "ACTIVE":
                logger.info(
                    f"{ts_utc()}: Previous task_id {mrt.task_id} Still Active. Returning ACTIVE."
                )
                return "ACTIVE"
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
                gtc.cumulative_tarfiles_pushed += 1
                print(
                    f"   (routine)  PUSHING (#{gtc.cumulative_tarfiles_pushed}) STORED source item: {item['source_path']}",
                    flush=True,
                )

        # SUBMIT new transfer here
        logger.info(f"{ts_utc()}: DIVING: Submit Transfer for {transfer_data['label']}")
        task = submit_transfer_with_checks(gtc.transfer_client, transfer_data)
        task_id = task.get("task_id")
        # NOTE: This log message is misleading. If we have accumulated multiple tar files for transfer,
        # the "lable" given here refers only to the LAST tarfile in the TransferData list.
        logger.info(
            f"{ts_utc()}: SURFACE Submit Transfer returned new task_id = {task_id} for label {transfer_data['label']}"
        )
        # Nullify the submitted transfer data structure so that a new one will be created on next call.
        transfer_data = None

        new_transfer = GlobusTransfer()
        new_transfer.transfer_data = transfer_data
        new_transfer.task_id = task_id
        new_transfer.task_status = "UNKNOWN"
        gtc.transfers.append(new_transfer)
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

    new_mrt: Optional[GlobusTransfer] = gtc.get_most_recent_transfer()

    # test for blocking on new task_id
    if new_mrt and new_mrt.task_id:
        if not non_blocking:
            new_mrt.task_status = globus_block_wait(
                transfer_client=gtc.transfer_client,
                task_id=new_mrt.task_id,
                wait_timeout=7200,
                max_retries=5,
            )
        else:
            logger.info(
                f"{ts_utc()}: NO BLOCKING (task_wait) for task_id {new_mrt.task_id}"
            )

        if transfer_type == "get":
            globus_wait(gtc.transfer_client, new_mrt.task_id)


def globus_finalize(
    gtc: Optional[GlobusTransferCollection],
    htc: HPSSTransferCollection,
    non_blocking: bool = False,
):
    last_task_id = None

    if gtc is None:
        logger.warning("No GlobusTransferCollection object provided for finalization")
        return

    transfer: Optional[GlobusTransfer] = gtc.get_most_recent_transfer()
    if transfer:
        if transfer.transfer_data:
            # DEBUG: review accumulated items in TransferData
            logger.info(f"{ts_utc()}: FINAL TransferData: accumulated items:")
            attribs = transfer.transfer_data.__dict__
            for item in attribs["data"]["DATA"]:
                if item["DATA_TYPE"] == "transfer_item":
                    gtc.cumulative_tarfiles_pushed += 1
                    print(
                        f"    (finalize) PUSHING ({gtc.cumulative_tarfiles_pushed}) source item: {item['source_path']}",
                        flush=True,
                    )

            # SUBMIT new transfer here
            logger.info(
                f"{ts_utc()}: DIVING: Submit Transfer for {transfer.transfer_data['label']}"
            )
            try:
                last_task = submit_transfer_with_checks(
                    gtc.transfer_client, transfer.transfer_data
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

        if not non_blocking:
            if transfer and transfer.task_id:
                globus_wait(gtc.transfer_client, transfer.task_id)
        if last_task_id:
            globus_wait(gtc.transfer_client, last_task_id)

        # TODO: figure out how to end!
        # new_mrt: Optional[GlobusTransfer] = gtc.get_most_recent_transfer()
        # if new_mrt and new_mrt.task_id:
        #     new_mrt.task_status = globus_block_wait(
        #         transfer_client=gtc.transfer_client,
        #         task_id=new_mrt.task_id,
        #         wait_timeout=7200,
        #         max_retries=5,
        #     )
        delete_transferred_files(htc)
