from __future__ import absolute_import, print_function

import configparser
import os
import os.path
import re
import socket
import sys
from typing import Dict, List
from urllib.parse import urlparse

from fair_research_login.client import NativeClient
from globus_sdk import TransferAPIError, TransferClient, TransferData

from .settings import logger
from .utils import GlobusInfo, ts_utc

# Constants ###################################################################

ZSTASH_CLIENT_ID: str = "6c1629cf-446c-49e7-af95-323c6412397f"

HPSS_ENDPOINT_MAP: Dict[str, str] = {
    "ALCF": "de463ec4-6d04-11e5-ba46-22000b92c6ec",
    "NERSC": "9cd89cfd-6d04-11e5-ba46-22000b92c6ec",
}

# This is used if the `globus_endpoint_uuid` is not set in `~/.zstash.ini`
REGEX_ENDPOINT_MAP: Dict[str, str] = {
    r"theta.*\.alcf\.anl\.gov": "08925f04-569f-11e7-bef8-22000b9a448b",
    r"blueslogin.*\.lcrc\.anl\.gov": "15288284-7006-4041-ba1a-6b52501e49f1",
    r"chrlogin.*\.lcrc\.anl\.gov": "15288284-7006-4041-ba1a-6b52501e49f1",
    r"b\d+\.lcrc\.anl\.gov": "15288284-7006-4041-ba1a-6b52501e49f1",
    r"chr.*\.lcrc\.anl\.gov": "15288284-7006-4041-ba1a-6b52501e49f1",
    r"compy.*\.pnl\.gov": "68fbd2fa-83d7-11e9-8e63-029d279f7e24",
    r"perlmutter.*\.nersc\.gov": "6bdc7956-fc0f-4ad2-989c-7aa5ee643a79",
}

# Last updated 2025-04-08
ENDPOINT_TO_NAME_MAP: Dict[str, str] = {
    "08925f04-569f-11e7-bef8-22000b9a448b": "Invalid, presumably Theta",
    "15288284-7006-4041-ba1a-6b52501e49f1": "LCRC Improv DTN",
    "68fbd2fa-83d7-11e9-8e63-029d279f7e24": "pic#compty-dtn",
    "6bdc7956-fc0f-4ad2-989c-7aa5ee643a79": "NERSC Perlmutter",
    "6c54cade-bde5-45c1-bdea-f4bd71dba2cc": "Globus Tutorial Collection 1",  # The Unit test endpoint
    "9cd89cfd-6d04-11e5-ba46-22000b92c6ec": "NERSC HPSS",
    "de463ec4-6d04-11e5-ba46-22000b92c6ec": "Invalid, presumably ALCF HPSS",
}

# Helper functions ############################################################


def ep_to_name(endpoint_id: str) -> str:
    if endpoint_id in ENDPOINT_TO_NAME_MAP:
        return ENDPOINT_TO_NAME_MAP[endpoint_id]
    else:
        return endpoint_id  # Just use the endpoint_id itself


def log_current_endpoints(globus_info: GlobusInfo):
    local: str
    remote: str
    if globus_info.local_endpoint:
        local = ep_to_name(globus_info.local_endpoint)
    else:
        local = "undefined"
    logger.debug(f"local endpoint={local}")
    if globus_info.remote_endpoint:
        remote = ep_to_name(globus_info.remote_endpoint)
    else:
        remote = "undefined"
    logger.debug(f"remote endpoint={remote}")


def get_all_endpoint_scopes(endpoints: List[str]) -> str:
    inner = " ".join(
        [f"*https://auth.globus.org/scopes/{ep}/data_access" for ep in endpoints]
    )
    return f"urn:globus:auth:scope:transfer.api.globus.org:all[{inner}]"


def set_clients(globus_info: GlobusInfo):
    native_client = NativeClient(
        client_id=ZSTASH_CLIENT_ID,
        app_name="Zstash",
        default_scopes="openid urn:globus:auth:scope:transfer.api.globus.org:all",
    )
    log_current_endpoints(globus_info)
    logger.debug(
        "set_clients. Calling login, which may print 'Please Paste your Auth Code Below:'"
    )
    if globus_info.local_endpoint and globus_info.remote_endpoint:
        all_scopes: str = get_all_endpoint_scopes(
            [globus_info.local_endpoint, globus_info.remote_endpoint]
        )
        native_client.login(
            requested_scopes=all_scopes, no_local_server=True, refresh_tokens=True
        )
    else:
        native_client.login(no_local_server=True, refresh_tokens=True)
    transfer_authorizer = native_client.get_authorizers().get("transfer.api.globus.org")
    globus_info.transfer_client = TransferClient(authorizer=transfer_authorizer)


# Used exclusively by check_consents
def check_endpoint_version_5(globus_info: GlobusInfo, ep_id: str) -> bool:
    if not globus_info.transfer_client:
        raise ValueError("transfer_client is undefined")
    log_current_endpoints(globus_info)
    logger.debug(f"check_endpoint_version_5. endpoint={ep_to_name(ep_id)}")
    output = globus_info.transfer_client.get_endpoint(ep_id)
    version = output.get("gcs_version", "0.0")
    if output["gcs_version"] is None:
        return False
    elif int(version.split(".")[0]) >= 5:
        return True
    return False


# Used exclusively by submit_transfer_with_checks, exclusively when there is a TransferAPIError
# This function is really to diagnose an error: are the consents ok?
# That is, we don't *need* to check consents or endpoint versions if everything worked out fine.
def check_consents(globus_info: GlobusInfo):
    scopes = "urn:globus:auth:scope:transfer.api.globus.org:all["
    for ep_id in [globus_info.remote_endpoint, globus_info.local_endpoint]:
        if ep_id and check_endpoint_version_5(globus_info, ep_id):
            scopes += f" *https://auth.globus.org/scopes/{ep_id}/data_access"
    scopes += " ]"
    native_client = NativeClient(client_id=ZSTASH_CLIENT_ID, app_name="Zstash")
    log_current_endpoints(globus_info)
    logger.debug(
        "check_consents. Calling login, which may print 'Please Paste your Auth Code Below:'"
    )
    native_client.login(requested_scopes=scopes)


# Used exclusively in globus_activate
def set_local_endpoint(globus_info: GlobusInfo):
    ini_path = os.path.expanduser("~/.zstash.ini")
    ini = configparser.ConfigParser()
    if ini.read(ini_path):
        if "local" in ini.sections():
            globus_info.local_endpoint = ini["local"].get("globus_endpoint_uuid")
            logger.debug(
                f"globus endpoint in ~/.zstash.ini: {ep_to_name(globus_info.local_endpoint)}"
            )
    else:
        ini["local"] = {"globus_endpoint_uuid": ""}
        try:
            with open(ini_path, "w") as f:
                ini.write(f)
        except Exception as e:
            logger.error(e)
            sys.exit(1)
    if not globus_info.local_endpoint:
        fqdn = socket.getfqdn()
        if re.fullmatch(r"n.*\.local", fqdn) and os.getenv("HOSTNAME", "NA").startswith(
            "compy"
        ):
            fqdn = "compy.pnl.gov"
        for pattern in REGEX_ENDPOINT_MAP.keys():
            if re.fullmatch(pattern, fqdn):
                globus_info.local_endpoint = REGEX_ENDPOINT_MAP.get(pattern)
                break
    # FQDN is not set on Perlmutter at NERSC
    if not globus_info.local_endpoint:
        nersc_hostname = os.environ.get("NERSC_HOST")
        if nersc_hostname and (
            nersc_hostname == "perlmutter" or nersc_hostname == "unknown"
        ):
            globus_info.local_endpoint = REGEX_ENDPOINT_MAP.get(
                r"perlmutter.*\.nersc\.gov"
            )
    if not globus_info.local_endpoint:
        logger.error(
            f"{ini_path} does not have the local Globus endpoint set nor could one be found in REGEX_ENDPOINT_MAP."
        )
        sys.exit(1)


# Used exclusively in globus_transfer
def file_exists(globus_info: GlobusInfo, name: str) -> bool:
    if not globus_info.archive_directory_listing:
        raise ValueError("archive_directory_listing is undefined")
    for entry in globus_info.archive_directory_listing:
        if entry.get("name") == name:
            return True
    return False


# Used exclusively in globus_transfer
def globus_block_wait(
    globus_info: GlobusInfo, wait_timeout: int, polling_interval: int, max_retries: int
):

    # poll every "polling_interval" seconds to speed up small transfers.  Report every 2 hours, stop waiting aftert 5*2 = 10 hours
    logger.info(
        f"{ts_utc()}: BLOCKING START: invoking task_wait for task_id = {globus_info.task_id}"
    )
    task_status = "UNKNOWN"
    retry_count = 0
    while retry_count < max_retries:
        try:
            # Wait for the task to complete
            logger.info(
                f"{ts_utc()}: on task_wait try {retry_count+1} out of {max_retries}"
            )
            if not globus_info.transfer_client:
                raise ValueError("transfer_client is undefined")
            globus_info.transfer_client.task_wait(
                globus_info.task_id, timeout=wait_timeout, polling_interval=10
            )
            logger.info(f"{ts_utc()}: done with wait")
        except Exception as e:
            logger.error(f"Unexpected Exception: {e}")
        else:
            if not globus_info.transfer_client:
                raise ValueError("transfer_client is undefined")
            curr_task = globus_info.transfer_client.get_task(globus_info.task_id)
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
        f"{ts_utc()}: BLOCKING ENDS: task_id {globus_info.task_id} returned from task_wait with status {task_status}"
    )

    return task_status


# Used exclusively in globus_transfer, globus_finalize
def submit_transfer_with_checks(globus_info: GlobusInfo):
    if not globus_info.transfer_client:
        raise ValueError("transfer_client is undefined")
    try:
        task = globus_info.transfer_client.submit_transfer(globus_info.transfer_data)
    except TransferAPIError as err:
        if err.info.consent_required:
            check_consents(globus_info)
            # Quit here and tell user to re-try
            print(
                "Consents added, please re-run the previous command to start transfer"
            )
            sys.exit(0)
        else:
            if err.info.authorization_parameters:
                print("Error is in authorization parameters")
            raise err
    return task


# Used exclusively in globus_transfer, globus_finalize
def globus_wait(globus_info: GlobusInfo, alternative_task_id=None):
    if alternative_task_id:
        task_id = alternative_task_id
    else:
        task_id = globus_info.task_id
    try:
        """
        A Globus transfer job (task) can be in one of the three states:
        ACTIVE, SUCCEEDED, FAILED. The script every 20 seconds polls a
        status of the transfer job (task) from the Globus Transfer service,
        with 20 second timeout limit. If the task is ACTIVE after time runs
        out 'task_wait' returns False, and True otherwise.
        """
        if not globus_info.transfer_client:
            raise ValueError("transfer_client is undefined")
        while not globus_info.transfer_client.task_wait(
            task_id, timeout=300, polling_interval=20
        ):
            pass
        """
        The Globus transfer job (task) has been finished (SUCCEEDED or FAILED).
        Check if the transfer SUCCEEDED or FAILED.
        """
        if not globus_info.transfer_client:
            raise ValueError("transfer_client is undefined")
        task = globus_info.transfer_client.get_task(task_id)
        if task["status"] == "SUCCEEDED":
            src_ep = task["source_endpoint_id"]
            dst_ep = task["destination_endpoint_id"]
            label = task["label"]
            logger.info(
                f"Globus transfer {task_id}, from {src_ep} to {dst_ep}: {label} succeeded"
            )
        else:
            logger.error("Transfer FAILED")
    except TransferAPIError as e:
        if e.code == "NoCredException":
            logger.error(
                f"{e.message}. Please go to https://app.globus.org/endpoints and activate the endpoint."
            )
        else:
            logger.error(e)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Exception: {e}")
        sys.exit(1)


# Primary functions ###########################################################


# C901 'globus_activate' is too complex (19)
def globus_activate(globus_info: GlobusInfo, alt_hpss: str = ""):  # noqa: C901
    """
    Read the local globus endpoint UUID from ~/.zstash.ini.
    If the ini file does not exist, create an ini file with empty values,
    and try to find the local endpoint UUID based on the FQDN
    """
    if alt_hpss != "":
        globus_info.hpss_path = alt_hpss
        globus_info.url = urlparse(alt_hpss)
        if globus_info.url.scheme != "globus":
            raise ValueError(f"Invalid url.scheme={globus_info.url.scheme}")
        globus_info.remote_endpoint = globus_info.url.netloc
    else:
        globus_info.remote_endpoint = globus_info.url.netloc
    set_local_endpoint(globus_info)
    if globus_info.remote_endpoint.upper() in HPSS_ENDPOINT_MAP.keys():
        globus_info.remote_endpoint = HPSS_ENDPOINT_MAP.get(
            globus_info.remote_endpoint.upper()
        )
    log_current_endpoints(globus_info)
    set_clients(globus_info)
    log_current_endpoints(globus_info)
    for ep_id in [globus_info.local_endpoint, globus_info.remote_endpoint]:
        if ep_id:
            ep_name = ep_to_name(ep_id)
        else:
            ep_name = "undefined"
        logger.debug(f"globus_activate. endpoint={ep_name}")
        if not globus_info.transfer_client:
            raise ValueError("Was unable to instantiate transfer_client")
        r = globus_info.transfer_client.endpoint_autoactivate(ep_id, if_expires_in=600)
        if r.get("code") == "AutoActivationFailed":
            logger.error(
                f"The {ep_id} endpoint is not activated or the current activation expires soon. Please go to https://app.globus.org/file-manager/collections/{ep_id} and (re)activate the endpoint."
            )
            sys.exit(1)


# C901 'globus_transfer' is too complex (20)
def globus_transfer(  # noqa: C901
    globus_info: GlobusInfo,
    remote_ep: str,
    remote_path: str,
    name: str,
    transfer_type: str,
    non_blocking: bool,
):

    logger.info(f"{ts_utc()}: Entered globus_transfer() for name = {name}")
    logger.debug(f"{ts_utc()}: non_blocking = {non_blocking}")
    if not globus_info.transfer_client:
        globus_activate(globus_info, "globus://" + remote_ep)
    # Try again:
    if not globus_info.transfer_client:
        logger.info(f"{ts_utc()}: Could not instantiate transfer client.")
        sys.exit(1)

    if transfer_type == "get":
        if not globus_info.archive_directory_listing:
            globus_info.archive_directory_listing = (
                globus_info.transfer_client.operation_ls(
                    globus_info.remote_endpoint, remote_path
                )
            )
        if not file_exists(globus_info, name):
            logger.error(
                f"Remote file globus://{remote_ep}{remote_path}/{name} does not exist"
            )
            sys.exit(1)

    if transfer_type == "get":
        src_ep = globus_info.remote_endpoint
        src_path = os.path.join(remote_path, name)
        dst_ep = globus_info.local_endpoint
        dst_path = os.path.join(os.getcwd(), name)
    else:
        src_ep = globus_info.local_endpoint
        src_path = os.path.join(os.getcwd(), name)
        dst_ep = globus_info.remote_endpoint
        dst_path = os.path.join(remote_path, name)

    subdir = os.path.basename(os.path.normpath(remote_path))
    subdir_label = re.sub("[^A-Za-z0-9_ -]", "", subdir)
    filename = name.split(".")[0]
    label = subdir_label + " " + filename

    if not globus_info.transfer_data:
        globus_info.transfer_data = TransferData(
            globus_info.transfer_client,
            src_ep,
            dst_ep,
            label=label,
            verify_checksum=True,
            preserve_timestamp=True,
            fail_on_quota_errors=True,
        )
    globus_info.transfer_data.add_item(src_path, dst_path)
    globus_info.transfer_data["label"] = label
    try:
        if globus_info.task_id:
            task = globus_info.transfer_client.get_task(globus_info.task_id)
            prev_task_status = task["status"]
            # one  of {ACTIVE, SUCCEEDED, FAILED, CANCELED, PENDING, INACTIVE}
            # NOTE: How we behave here depends upon whether we want to support mutliple active transfers.
            # Presently, we do not, except inadvertantly (if status == PENDING)
            if prev_task_status == "ACTIVE":
                logger.info(
                    f"{ts_utc()}: Previous task_id {globus_info.task_id} Still Active. Returning ACTIVE."
                )
                return "ACTIVE"
            elif prev_task_status == "SUCCEEDED":
                logger.info(
                    f"{ts_utc()}: Previous task_id {globus_info.task_id} status = SUCCEEDED."
                )
                src_ep = task["source_endpoint_id"]
                dst_ep = task["destination_endpoint_id"]
                label = task["label"]
                ts = ts_utc()
                logger.info(
                    f"{ts}:Globus transfer {globus_info.task_id}, from {src_ep} to {dst_ep}: {label} succeeded"
                )
            else:
                logger.error(
                    f"{ts_utc()}: Previous task_id {globus_info.task_id} status = {prev_task_status}."
                )

        # DEBUG: review accumulated items in TransferData
        logger.info(f"{ts_utc()}: TransferData: accumulated items:")
        attribs = globus_info.transfer_data.__dict__
        for item in attribs["data"]["DATA"]:
            if item["DATA_TYPE"] == "transfer_item":
                globus_info.tarfiles_pushed += 1
                print(
                    f"   (routine)  PUSHING (#{globus_info.tarfiles_pushed}) STORED source item: {item['source_path']}",
                    flush=True,
                )

        # SUBMIT new transfer here
        logger.info(
            f"{ts_utc()}: DIVING: Submit Transfer for {globus_info.transfer_data['label']}"
        )
        task = submit_transfer_with_checks(globus_info)
        globus_info.task_id = task.get("task_id")
        # NOTE: This log message is misleading. If we have accumulated multiple tar files for transfer,
        # the "label" given here refers only to the LAST tarfile in the TransferData list.
        logger.info(
            f"{ts_utc()}: SURFACE Submit Transfer returned new task_id = {globus_info.task_id} for label {globus_info.transfer_data['label']}"
        )

        # Nullify the submitted transfer data structure so that a new one will be created on next call.
        globus_info.transfer_data = None
    except TransferAPIError as e:
        if e.code == "NoCredException":
            logger.error(
                f"{e.message}. Please go to https://app.globus.org/endpoints and activate the endpoint."
            )
        else:
            logger.error(e)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Exception: {e}")
        sys.exit(1)

    # test for blocking on new task_id
    task_status = "UNKNOWN"
    if not non_blocking:
        task_status = globus_block_wait(
            globus_info, wait_timeout=7200, polling_interval=10, max_retries=5
        )
    else:
        logger.info(
            f"{ts_utc()}: NO BLOCKING (task_wait) for task_id {globus_info.task_id}"
        )

    if transfer_type == "put":
        return task_status

    if transfer_type == "get" and globus_info.task_id:
        globus_wait(globus_info)

    return task_status


def globus_finalize(globus_info: GlobusInfo, non_blocking: bool = False):
    last_task_id = None

    if globus_info.transfer_data:
        # DEBUG: review accumulated items in TransferData
        logger.info(f"{ts_utc()}: FINAL TransferData: accumulated items:")
        attribs = globus_info.transfer_data.__dict__
        for item in attribs["data"]["DATA"]:
            if item["DATA_TYPE"] == "transfer_item":
                globus_info.tarfiles_pushed += 1
                print(
                    f"    (finalize) PUSHING ({globus_info.tarfiles_pushed}) source item: {item['source_path']}",
                    flush=True,
                )

        # SUBMIT new transfer here
        logger.info(
            f"{ts_utc()}: DIVING: Submit Transfer for {globus_info.transfer_data['label']}"
        )
        try:
            last_task = submit_transfer_with_checks(globus_info)
            last_task_id = last_task.get("task_id")
        except TransferAPIError as e:
            if e.code == "NoCredException":
                logger.error(
                    f"{e.message}. Please go to https://app.globus.org/endpoints and activate the endpoint."
                )
            else:
                logger.error(e)
            sys.exit(1)
        except Exception as e:
            logger.error(f"Exception: {e}")
            sys.exit(1)

    if not non_blocking:
        if globus_info.task_id:
            globus_wait(globus_info)
        if last_task_id:
            globus_wait(globus_info, last_task_id)
