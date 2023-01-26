from __future__ import absolute_import, print_function

import configparser
import os.path
import re
import socket
import sys

from fair_research_login.client import NativeClient
from globus_sdk import TransferAPIError, TransferClient, TransferData
from globus_sdk.services.transfer.response.iterable import IterableTransferResponse
from six.moves.urllib.parse import urlparse

from .settings import logger

hpss_endpoint_map = {
    "ALCF": "de463ec4-6d04-11e5-ba46-22000b92c6ec",
    "NERSC": "9cd89cfd-6d04-11e5-ba46-22000b92c6ec",
}

# This is used if the `globus_endpoint_uuid` is not set in `~/.zstash.ini`
regex_endpoint_map = {
    r"theta.*\.alcf\.anl\.gov": "08925f04-569f-11e7-bef8-22000b9a448b",
    r"blueslogin.*\.lcrc\.anl\.gov": "61f9954c-a4fa-11ea-8f07-0a21f750d19b",
    r"b\d+\.lcrc\.anl\.gov": "61f9954c-a4fa-11ea-8f07-0a21f750d19b",
    r"chr.*\.lcrc\.anl\.gov": "61f9954c-a4fa-11ea-8f07-0a21f750d19b",
    r"cori.*\.nersc\.gov": "9d6d99eb-6d04-11e5-ba46-22000b92c6ec",
    r"perlmutter.*\.nersc\.gov": "6bdc7956-fc0f-4ad2-989c-7aa5ee643a79",  # If this doesn't work, use cori
}

remote_endpoint = None
local_endpoint = None
transfer_client: TransferClient = None
transfer_data: TransferData = None
task_id = None
archive_directory_listing: IterableTransferResponse = None


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


def globus_transfer(
    remote_ep: str, remote_path: str, name: str, transfer_type: str
):  # noqa: C901
    global transfer_client
    global local_endpoint
    global remote_endpoint
    global transfer_data
    global task_id
    global archive_directory_listing

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
    transfer_data["label"] = subdir_label + " " + filename
    try:
        if task_id:
            task = transfer_client.get_task(task_id)
            if task["status"] == "ACTIVE":
                return
            elif task["status"] == "SUCCEEDED":
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
        task = transfer_client.submit_transfer(transfer_data)
        task_id = task.get("task_id")
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

    if transfer_type == "get" and task_id:
        globus_wait(task_id)


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
        while not transfer_client.task_wait(task_id, timeout=20, polling_interval=20):
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

    last_task_id = None

    if transfer_data:
        try:
            last_task = transfer_client.submit_transfer(transfer_data)
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
