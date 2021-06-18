from __future__ import absolute_import, print_function

import sys
import os.path
import re
import socket
import configparser
from fair_research_login.client import NativeClient
from globus_sdk import TransferClient, TransferData
from globus_sdk.exc import TransferAPIError

from .settings import logger


hpss_endpoint_map = {
    "ALCF": "de463ec4-6d04-11e5-ba46-22000b92c6ec",
    "NERSC": "9cd89cfd-6d04-11e5-ba46-22000b92c6ec",
}

regex_endpoint_map = {
    "theta.*\.alcf\.anl\.gov": "08925f04-569f-11e7-bef8-22000b9a448b",
    "blueslogin.*\.lcrc\.anl\.gov": "61f9954c-a4fa-11ea-8f07-0a21f750d19b",
    "chr.*\.lcrc\.anl\.gov": "61f9954c-a4fa-11ea-8f07-0a21f750d19b",
    "cori.*\.nersc\.gov": "9d6d99eb-6d04-11e5-ba46-22000b92c6ec",
}

def globus_transfer(remote_endpoint, remote_path, name, transfer_type, local_endpoint=None):
    """
    Read the local globus endpoint UUID from ~/.zstash.ini.
    If the ini file does not exist, create an ini file with empty values,
    and try to find the local endpoint UUID based on the FQDN 
    """
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
    if not local_endpoint:
        logger.error("{} does not have the local Globus endpoint set".format(ini_path))
        sys.exit(1)

    if remote_endpoint.upper() in hpss_endpoint_map.keys():
        remote_endpoint = hpss_endpoint_map.get(remote_endpoint.upper())

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

    native_client = NativeClient(
            client_id = "6c1629cf-446c-49e7-af95-323c6412397f",
            app_name="Zstash",
            default_scopes = "openid urn:globus:auth:scope:transfer.api.globus.org:all")
    native_client.login(no_local_server=True, refresh_tokens=True)
    transfer_authorizer = native_client.get_authorizers().get("transfer.api.globus.org")
    tc = TransferClient(transfer_authorizer)

    td = TransferData(tc, src_ep, dst_ep, sync_level=checksum, verify_checksum=True, fail_on_quota_errors=True)
    td.add_item(src_path, dst_path)
    try:
        task = tc.submit_transfer(td)
        task_id = task.get("task_id")
        """
        A Globus transfer job (task) can be in one of the three states:
        ACTIVE, SUCCEEDED, FAILED. The script every 20 seconds polls a
        status of the transfer job (task) from the Globus Transfer service,
        with 60 second timeout limit. If the task is ACTIVE after time runs
        out 'task_wait' returns False, and True otherwise.
        """
        while not tc.task_wait(task_id, 20, 20):
            task = tc.get_task(task_id)
            if task.get("is_paused"):
                break
        """
        The Globus transfer job (task) has been finished (SUCCEEDED or FAILED),
        or is still active (ACTIVE). Check if the transfer SUCCEEDED or FAILED.
        """
        task = tc.get_task(task_id)
        if task["status"] == "SUCCEEDED":
            bps = task.get("effective_bytes_per_second")
            logger.info("Globus transfer {}, from {}{} to {}{} succeeded".format(
                    task_id, src_ep, src_path, dst_ep, dst_path))
            faults = task.get("faults")
        elif task.get("status") == "ACTIVE":
            if task.get("is_paused"):
                pause_info = tc.task_pause_info(task_id)
                paused_rules = pause_info.get("pause_rules")
                reason = paused_rules[0].get("message")
                message = "The task was paused. Reason: {}".format(reason)
                status = PAUSED
                logger.info("{} - {}".format(self, message))
            else:
                message = "The task reached a {} second deadline\n".format(GlobusTransfer.deadline)
                events = tc.task_event_list(task_id, num_results=5, filter="is_error:1")
                message += self.get_error_events(tc, task_id)
                status = DEADLINE
                logger.warning("{} - faults: {}, error: {}".format(self, task.get("faults"), message))
            tc.cancel_task(task_id)
        else:
            logger.error("Transfer FAILED")
    except TransferAPIError as e:
        if e.code == "NoCredException":
            logger.error("{}. Please go to https://app.globus.org/ and activate the endpoint.".format(e.message))
        else:
            logger.error(e)
        sys.exit(1)
    except Exception as e:
        logger.error("{} - exception: {}".format(self, e))
        sys.exit(1)
