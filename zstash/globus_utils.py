from __future__ import absolute_import, print_function

import configparser
import json
import os
import os.path
import re
import socket
import sys
from typing import Dict, List, Optional

from globus_sdk import (
    NativeAppAuthClient,
    RefreshTokenAuthorizer,
    TransferAPIError,
    TransferClient,
    TransferData,
)
from globus_sdk.response import GlobusHTTPResponse

from .settings import logger

# Global constants ############################################################
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

ZSTASH_CLIENT_ID: str = "6c1629cf-446c-49e7-af95-323c6412397f"

# State files
GLOBUS_CFG: str = os.path.expanduser("~/.globus-native-apps.cfg")
INI_PATH: str = os.path.expanduser("~/.zstash.ini")
TOKEN_FILE = os.path.expanduser("~/.zstash_globus_tokens.json")

# Independent functions #######################################################
# The functions here don't rely on the global variables defined in globus.py.


# Primarily used by globus_activate ###########################################
def check_state_files():
    if os.path.exists(GLOBUS_CFG):
        logger.warning(
            f"Globus CFG {GLOBUS_CFG} exists. This may be left over from earlier versions of zstash, and may cause issues. Consider deleting."
        )

    if os.path.exists(INI_PATH):
        logger.info(
            f"{INI_PATH} exists. We can try to read the local endpoint ID from it."
        )
    else:
        logger.info(
            f"{INI_PATH} does NOT exist. This means we won't be able to read the local endpoint ID from it."
        )

    if os.path.exists(TOKEN_FILE):
        logger.info(
            f"Token file {TOKEN_FILE} exists. We can try to load tokens from it."
        )
    else:
        logger.info(
            f"Token file {TOKEN_FILE} does NOT exist. This means we won't be able to load tokens from it."
        )


def get_local_endpoint_id(local_endpoint_id: Optional[str]) -> str:
    ini = configparser.ConfigParser()
    if ini.read(INI_PATH):
        if "local" in ini.sections():
            local_endpoint_id = ini["local"].get("globus_endpoint_uuid")
            logger.info(
                f"Setting local_endpoint_id based on {INI_PATH}: {local_endpoint_id}"
            )
    else:
        ini["local"] = {"globus_endpoint_uuid": ""}
        try:
            with open(INI_PATH, "w") as f:
                ini.write(f)
            logger.info(f"Writing to empty {INI_PATH}")
        except Exception as e:
            logger.error(e)
            sys.exit(1)
    if not local_endpoint_id:
        fqdn = socket.getfqdn()
        if re.fullmatch(r"n.*\.local", fqdn) and os.getenv("HOSTNAME", "NA").startswith(
            "compy"
        ):
            fqdn = "compy.pnl.gov"
        for pattern in REGEX_ENDPOINT_MAP.keys():
            if re.fullmatch(pattern, fqdn):
                local_endpoint_id = REGEX_ENDPOINT_MAP.get(pattern)
                logger.info(
                    f"Setting local_endpoint_id based on FQDN {fqdn}: {local_endpoint_id}"
                )
                break
    # FQDN is not set on Perlmutter at NERSC
    if not local_endpoint_id:
        nersc_hostname = os.environ.get("NERSC_HOST")
        if nersc_hostname and (
            nersc_hostname == "perlmutter" or nersc_hostname == "unknown"
        ):
            local_endpoint_id = REGEX_ENDPOINT_MAP.get(r"perlmutter.*\.nersc\.gov")
            logger.info(
                f"Setting local_endpoint_id based on NERSC_HOST {nersc_hostname}: {local_endpoint_id}"
            )
    if not local_endpoint_id:
        logger.error(
            f"{INI_PATH} does not have the local Globus endpoint set nor could one be found in REGEX_ENDPOINT_MAP."
        )
        sys.exit(1)
    return local_endpoint_id


def get_transfer_client_with_auth(
    both_endpoints: List[Optional[str]],
) -> TransferClient:
    tokens = load_tokens()

    # Check if we have stored refresh tokens
    if "transfer.api.globus.org" in tokens:
        token_data = tokens["transfer.api.globus.org"]
        if "refresh_token" in token_data:
            logger.info("Found stored refresh token - using it")
            # Create a simple auth client for the RefreshTokenAuthorizer
            auth_client = NativeAppAuthClient(ZSTASH_CLIENT_ID)
            transfer_authorizer = RefreshTokenAuthorizer(
                refresh_token=token_data["refresh_token"], auth_client=auth_client
            )
            transfer_client = TransferClient(authorizer=transfer_authorizer)
            return transfer_client

    # No stored tokens, need to authenticate
    logger.info("No stored tokens found - starting authentication")

    # Get the required scopes
    all_scopes = get_all_endpoint_scopes(both_endpoints)

    # Use the NativeAppAuthClient pattern from the documentation
    client = NativeAppAuthClient(ZSTASH_CLIENT_ID)
    client.oauth2_start_flow(
        requested_scopes=all_scopes,
        refresh_tokens=True,  # This is the key to persistent auth!
    )

    authorize_url = client.oauth2_get_authorize_url()
    print(f"Please go to this URL and login:\n{authorize_url}")

    auth_code = input("Please enter the code you get after login here: ").strip()
    token_response = client.oauth2_exchange_code_for_tokens(auth_code)

    # Save tokens for next time
    save_tokens(token_response)

    # Get the transfer token and create authorizer
    globus_transfer_data = token_response.by_resource_server["transfer.api.globus.org"]
    transfer_authorizer = RefreshTokenAuthorizer(
        refresh_token=globus_transfer_data["refresh_token"], auth_client=client
    )

    transfer_client = TransferClient(authorizer=transfer_authorizer)
    return transfer_client


def load_tokens():
    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


def get_all_endpoint_scopes(endpoints: List[Optional[str]]) -> str:
    inner = " ".join(
        [
            f"*https://auth.globus.org/scopes/{ep}/data_access"
            for ep in endpoints
            if ep is not None
        ]
    )
    return f"urn:globus:auth:scope:transfer.api.globus.org:all[{inner}]"


def save_tokens(token_response):
    tokens_to_save = {}
    for resource_server, token_data in token_response.by_resource_server.items():
        tokens_to_save[resource_server] = {
            "access_token": token_data["access_token"],
            "refresh_token": token_data.get("refresh_token"),
            "expires_at": token_data.get("expires_at_seconds"),
        }

    with open(TOKEN_FILE, "w") as f:
        json.dump(tokens_to_save, f, indent=2)
    logger.info("Tokens saved successfully")


# Primarily used by globus_transfer ###########################################
def set_up_TransferData(
    transfer_type: str,
    local_endpoint: Optional[str],
    remote_endpoint: Optional[str],
    remote_path: str,
    name: str,
    transfer_client: TransferClient,
    transfer_data: Optional[TransferData] = None,
) -> TransferData:
    if not local_endpoint:
        raise ValueError("Local endpoint ID is not set.")
    if not remote_endpoint:
        raise ValueError("Remote endpoint ID is not set.")
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
    return transfer_data


def submit_transfer_with_checks(transfer_client, transfer_data) -> GlobusHTTPResponse:
    task: GlobusHTTPResponse
    try:
        task = transfer_client.submit_transfer(transfer_data)
    except TransferAPIError as err:
        if err.info.consent_required:
            logger.error("Consent required - this suggests scope issues.")
            logger.error(
                "With proper scope handling, this block should not be reached."
            )
            logger.error(
                "Please report this bug at https://github.com/E3SM-Project/zstash/issues, with details of what you were trying to do."
            )
            raise RuntimeError(
                "Insufficient Globus consents - please report this bug"
            ) from err
        else:
            raise err
    return task
