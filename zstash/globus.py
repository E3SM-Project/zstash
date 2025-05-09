from __future__ import absolute_import, print_function

import configparser
import os
import os.path
import re
import socket
import sys

from fair_research_login.client import NativeClient
from globus_sdk import TransferAPIError, TransferClient, TransferData
from globus_sdk.services.transfer.response.iterable import IterableTransferResponse
from six.moves.urllib.parse import urlparse

from .settings import logger
from .utils import ts_utc

hpss_endpoint_map = {
    "ALCF": "de463ec4-6d04-11e5-ba46-22000b92c6ec",
    "NERSC": "9cd89cfd-6d04-11e5-ba46-22000b92c6ec",
}

# This is used if the `globus_endpoint_uuid` is not set in `~/.zstash.ini`
regex_endpoint_map = {
    r"theta.*\.alcf\.anl\.gov": "08925f04-569f-11e7-bef8-22000b9a448b",
    r"blueslogin.*\.lcrc\.anl\.gov": "15288284-7006-4041-ba1a-6b52501e49f1",
    r"chrlogin.*\.lcrc\.anl\.gov": "15288284-7006-4041-ba1a-6b52501e49f1",
    r"b\d+\.lcrc\.anl\.gov": "15288284-7006-4041-ba1a-6b52501e49f1",
    r"chr.*\.lcrc\.anl\.gov": "15288284-7006-4041-ba1a-6b52501e49f1",
    r"compy.*\.pnl\.gov": "68fbd2fa-83d7-11e9-8e63-029d279f7e24",
    r"perlmutter.*\.nersc\.gov": "6bdc7956-fc0f-4ad2-989c-7aa5ee643a79",
}

remote_endpoint = None
local_endpoint = None
transfer_client: TransferClient = None
transfer_data: TransferData = None
task_id = None
archive_directory_listing: IterableTransferResponse = None


def check_endpoint_version_5(ep_id):
    output = transfer_client.get_endpoint(ep_id)
    version = output.get("gcs_version", "0.0")
    if output["gcs_version"] is None:
        return False
    elif int(version.split(".")[0]) >= 5:
        return True
    return False


def submit_transfer_with_checks(transfer_data):
    try:
        task = transfer_client.submit_transfer(transfer_data)
    except TransferAPIError as err:
        if err.info.consent_required:
            scopes = "urn:globus:auth:scope:transfer.api.globus.org:all["
            for ep_id in [remote_endpoint, local_endpoint]:
                if check_endpoint_version_5(ep_id):
                    scopes += f" *https://auth.globus.org/scopes/{ep_id}/data_access"
            scopes += " ]"
            native_client = NativeClient(
                client_id="6c1629cf-446c-49e7-af95-323c6412397f", app_name="Zstash"
            )
            native_client.login(requested_scopes=scopes)
            # Quit here and tell user to re-try
            print(
                "Consents added, please re-run the previous command to start transfer"
            )
            sys.exit(0)
        else:
            raise err
    return task


def globus_activate(hpss: str):
    """
    Read the local globus endpoint UUID from ~/.zstash.ini.
    If the ini file does not exist, create an ini file with empty values,
    and try to find the local endpoint UUID based on the FQDN
    """
    global transfer_client
    global local_endpoint
    global remote_endpoint

    url = urlparse(hpss)
    if url.scheme != "globus":
        return
    remote_endpoint = url.netloc

    ini_path = os.path.expanduser("~/.zstash.ini")
    ini = configparser.ConfigParser()
    if ini.read(ini_path):
        if "local" in ini.sections():
            local_endpoint = ini["local"].get("globus_endpoint_uuid")
    else:
        ini["local"] = {"globus_endpoint_uuid": ""}
        try:
            with open(ini_path, "w") as f:
                ini.write(f)
        except Exception as e:
            logger.error(e)
            sys.exit(1)
    if not local_endpoint:
        fqdn = socket.getfqdn()
        if re.fullmatch(r"n.*\.local", fqdn) and os.getenv("HOSTNAME", "NA").startswith(
            "compy"
        ):
            fqdn = "compy.pnl.gov"
        for pattern in regex_endpoint_map.keys():
            if re.fullmatch(pattern, fqdn):
                local_endpoint = regex_endpoint_map.get(pattern)
                break
    # FQDN is not set on Perlmutter at NERSC
    if not local_endpoint:
        nersc_hostname = os.environ.get("NERSC_HOST")
        if nersc_hostname and (
            nersc_hostname == "perlmutter" or nersc_hostname == "unknown"
        ):
            local_endpoint = regex_endpoint_map.get(r"perlmutter.*\.nersc\.gov")
    if not local_endpoint:
        logger.error(
            "{} does not have the local Globus endpoint set nor could one be found in regex_endpoint_map.".format(
                ini_path
            )
        )
        sys.exit(1)

    if remote_endpoint.upper() in hpss_endpoint_map.keys():
        remote_endpoint = hpss_endpoint_map.get(remote_endpoint.upper())

    native_client = NativeClient(
        client_id="6c1629cf-446c-49e7-af95-323c6412397f",
        app_name="Zstash",
        default_scopes="openid urn:globus:auth:scope:transfer.api.globus.org:all",
    )
    native_client.login(no_local_server=True, refresh_tokens=True)
    transfer_authorizer = native_client.get_authorizers().get("transfer.api.globus.org")
    transfer_client = TransferClient(authorizer=transfer_authorizer)

    for ep_id in [local_endpoint, remote_endpoint]:
        r = transfer_client.endpoint_autoactivate(ep_id, if_expires_in=600)
        if r.get("code") == "AutoActivationFailed":
            logger.error(
                "The {} endpoint is not activated or the current activation expires soon. Please go to https://app.globus.org/file-manager/collections/{} and (re)activate the endpoint.".format(
                    ep_id, ep_id
                )
            )
            sys.exit(1)


def file_exists(name: str) -> bool:
    global archive_directory_listing

    for entry in archive_directory_listing:
        if entry.get("name") == name:
            return True
    return False


global_variable_tarfiles_pushed = 0


# C901 'globus_transfer' is too complex (20)
def globus_transfer(  # noqa: C901
    remote_ep: str, remote_path: str, name: str, transfer_type: str, non_blocking: bool
):
    global transfer_client
    global local_endpoint
    global remote_endpoint
    global transfer_data
    global task_id
    global archive_directory_listing
    global global_variable_tarfiles_pushed

    logger.info(f"{ts_utc()}: Entered globus_transfer() for name = {name}")
    logger.debug(f"{ts_utc()}: non_blocking = {non_blocking}")
    if not transfer_client:
        globus_activate("globus://" + remote_ep)
    if not transfer_client:
        sys.exit(1)

    if transfer_type == "get":
        if not archive_directory_listing:
            archive_directory_listing = transfer_client.operation_ls(
                remote_endpoint, remote_path
            )
        if not file_exists(name):
            logger.error(
                "Remote file globus://{}{}/{} does not exist".format(
                    remote_ep, remote_path, name
                )
            )
            sys.exit(1)

    if transfer_type == "get":
        src_ep = remote_endpoint
        src_path = os.path.join(remote_path, name)
        dst_ep = local_endpoint
        dst_path = os.path.join(os.getcwd(), name)
    else:
        src_ep = local_endpoint
        src_path = os.path.join(os.getcwd(), name)
        dst_ep = remote_endpoint
        dst_path = os.path.join(remote_path, name)

    subdir = os.path.basename(os.path.normpath(remote_path))
    subdir_label = re.sub("[^A-Za-z0-9_ -]", "", subdir)
    filename = name.split(".")[0]
    label = subdir_label + " " + filename

    if not transfer_data:
        transfer_data = TransferData(
            transfer_client,
            src_ep,
            dst_ep,
            label=label,
            verify_checksum=True,
            preserve_timestamp=True,
            fail_on_quota_errors=True,
        )
    transfer_data.add_item(src_path, dst_path)
    transfer_data["label"] = label
    try:
        if task_id:
            task = transfer_client.get_task(task_id)
            prev_task_status = task["status"]
            # one  of {ACTIVE, SUCCEEDED, FAILED, CANCELED, PENDING, INACTIVE}
            # NOTE: How we behave here depends upon whether we want to support mutliple active transfers.
            # Presently, we do not, except inadvertantly (if status == PENDING)
            if prev_task_status == "ACTIVE":
                logger.info(
                    f"{ts_utc()}: Previous task_id {task_id} Still Active. Returning ACTIVE."
                )
                return "ACTIVE"
            elif prev_task_status == "SUCCEEDED":
                logger.info(
                    f"{ts_utc()}: Previous task_id {task_id} status = SUCCEEDED."
                )
                src_ep = task["source_endpoint_id"]
                dst_ep = task["destination_endpoint_id"]
                label = task["label"]
                ts = ts_utc()
                logger.info(
                    "{}:Globus transfer {}, from {} to {}: {} succeeded".format(
                        ts, task_id, src_ep, dst_ep, label
                    )
                )
            else:
                logger.error(
                    f"{ts_utc()}: Previous task_id {task_id} status = {prev_task_status}."
                )

        # DEBUG: review accumulated items in TransferData
        logger.info(f"{ts_utc()}: TransferData: accumulated items:")
        attribs = transfer_data.__dict__
        for item in attribs["data"]["DATA"]:
            if item["DATA_TYPE"] == "transfer_item":
                global_variable_tarfiles_pushed += 1
                print(
                    f"   (routine)  PUSHING (#{global_variable_tarfiles_pushed}) STORED source item: {item['source_path']}",
                    flush=True,
                )

        # SUBMIT new transfer here
        logger.info(f"{ts_utc()}: DIVING: Submit Transfer for {transfer_data['label']}")
        task = submit_transfer_with_checks(transfer_data)
        task_id = task.get("task_id")
        # NOTE: This log message is misleading. If we have accumulated multiple tar files for transfer,
        # the "lable" given here refers only to the LAST tarfile in the TransferData list.
        logger.info(
            f"{ts_utc()}: SURFACE Submit Transfer returned new task_id = {task_id} for label {transfer_data['label']}"
        )

        # Nullify the submitted transfer data structure so that a new one will be created on next call.
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

    # test for blocking on new task_id
    task_status = "UNKNOWN"
    if not non_blocking:
        task_status = globus_block_wait(
            task_id=task_id, wait_timeout=7200, polling_interval=10, max_retries=5
        )
    else:
        logger.info(f"{ts_utc()}: NO BLOCKING (task_wait) for task_id {task_id}")

    if transfer_type == "put":
        return task_status

    if transfer_type == "get" and task_id:
        globus_wait(task_id)

    return task_status


def globus_block_wait(
    task_id: str, wait_timeout: int, polling_interval: int, max_retries: int
):
    global transfer_client

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
                f"{ts_utc()}: on task_wait try {retry_count+1} out of {max_retries}"
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


def globus_wait(task_id: str):
    global transfer_client

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


def globus_finalize(non_blocking: bool = False):
    global transfer_client
    global transfer_data
    global task_id
    global global_variable_tarfiles_pushed

    last_task_id = None

    if transfer_data:
        # DEBUG: review accumulated items in TransferData
        logger.info(f"{ts_utc()}: FINAL TransferData: accumulated items:")
        attribs = transfer_data.__dict__
        for item in attribs["data"]["DATA"]:
            if item["DATA_TYPE"] == "transfer_item":
                global_variable_tarfiles_pushed += 1
                print(
                    f"    (finalize) PUSHING ({global_variable_tarfiles_pushed}) source item: {item['source_path']}",
                    flush=True,
                )

        # SUBMIT new transfer here
        logger.info(f"{ts_utc()}: DIVING: Submit Transfer for {transfer_data['label']}")
        try:
            last_task = submit_transfer_with_checks(transfer_data)
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
        if task_id:
            globus_wait(task_id)
        if last_task_id:
            globus_wait(last_task_id)
