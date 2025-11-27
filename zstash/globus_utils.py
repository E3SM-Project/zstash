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
from globus_sdk.services.auth.errors import AuthAPIError

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
# Default token file - can be overridden via environment variable
DEFAULT_TOKEN_FILE = os.path.expanduser("~/.zstash_globus_tokens.json")


# Helper functions for token file management #################################
def get_token_file_path() -> str:
    """
    Get the token file path, checking environment variable first,
    then falling back to default.
    """
    return os.environ.get("ZSTASH_GLOBUS_TOKEN_FILE", DEFAULT_TOKEN_FILE)


def get_endpoint_key(endpoints: List[Optional[str]]) -> str:
    """
    Generate a unique key for a pair of endpoints.
    Sorts endpoints to ensure consistency regardless of order.
    """
    # Filter out None values and sort to ensure consistent key
    sorted_eps = sorted([ep for ep in endpoints if ep is not None])
    return ":".join(sorted_eps)


# Independent functions #######################################################
# The functions here don't rely on the global variables defined in globus.py.


# Primarily used by globus_activate ###########################################
def check_state_files():
    token_file = get_token_file_path()

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

    if os.path.exists(token_file):
        logger.info(
            f"Token file {token_file} exists. We can try to load tokens from it."
        )
    else:
        logger.info(
            f"Token file {token_file} does NOT exist. This means we won't be able to load tokens from it."
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
    endpoint_key = get_endpoint_key(both_endpoints)

    tokens = load_tokens()

    # Check if we have stored refresh tokens for this endpoint pair
    if endpoint_key in tokens:
        endpoint_tokens = tokens[endpoint_key]
        if "transfer.api.globus.org" in endpoint_tokens:
            token_data = endpoint_tokens["transfer.api.globus.org"]
            if "refresh_token" in token_data:
                logger.info(
                    f"Found stored refresh token for endpoints {endpoint_key} - using it"
                )
                # Create a simple auth client for the RefreshTokenAuthorizer
                auth_client = NativeAppAuthClient(ZSTASH_CLIENT_ID)
                try:
                    transfer_authorizer = RefreshTokenAuthorizer(
                        refresh_token=token_data["refresh_token"],
                        auth_client=auth_client,
                    )
                    transfer_client = TransferClient(authorizer=transfer_authorizer)
                    return transfer_client
                except AuthAPIError:
                    logger.warning(
                        f"Stored refresh token for {endpoint_key} is invalid, will re-authenticate."
                    )
                    # Remove invalid token entry
                    del tokens[endpoint_key]
                    save_tokens_to_file(tokens)

    # No stored tokens for this endpoint pair, need to authenticate
    logger.info(
        f"No stored tokens found for endpoints {endpoint_key} - starting authentication"
    )

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
    save_tokens(token_response, both_endpoints)

    # Get the transfer token and create authorizer
    globus_transfer_data = token_response.by_resource_server["transfer.api.globus.org"]
    transfer_authorizer = RefreshTokenAuthorizer(
        refresh_token=globus_transfer_data["refresh_token"], auth_client=client
    )

    transfer_client = TransferClient(authorizer=transfer_authorizer)
    return transfer_client


def load_tokens() -> Dict:
    """
    Load all tokens from the token file.
    Returns a dict with structure:
    {
        "endpoint1:endpoint2": {
            "transfer.api.globus.org": {
                "access_token": "...",
                "refresh_token": "...",
                "expires_at": ...
            }
        },
        ...
    }

    Also handles legacy single-token format for backward compatibility.
    """
    token_file = get_token_file_path()

    if os.path.exists(token_file):
        try:
            with open(token_file, "r") as f:
                data = json.load(f)

            # Check if this is the old single-token format
            if "transfer.api.globus.org" in data:
                # Legacy format detected - migrate it
                logger.info("Detected legacy token format, migrating to new format")
                # We can't determine the original endpoints, so we'll just
                # return empty dict and let user re-authenticate
                # Optionally, we could try to keep the old token with a generic key
                return {}

            return data
        except (json.JSONDecodeError, IOError):
            logger.warning("Error reading token file")
            return {}
    return {}


def save_tokens_to_file(tokens: Dict):
    """
    Save the complete token dictionary to file.
    """
    token_file = get_token_file_path()

    try:
        # Create directory if it doesn't exist
        token_dir = os.path.dirname(token_file)
        if token_dir and not os.path.exists(token_dir):
            os.makedirs(token_dir)

        with open(token_file, "w") as f:
            json.dump(tokens, f, indent=2)
        logger.info(f"Tokens saved successfully to {token_file}")
    except IOError as e:
        logger.error(f"Failed to save tokens: {e}")


def get_all_endpoint_scopes(endpoints: List[Optional[str]]) -> str:
    inner = " ".join(
        [
            f"*https://auth.globus.org/scopes/{ep}/data_access"
            for ep in endpoints
            if ep is not None
        ]
    )
    return f"urn:globus:auth:scope:transfer.api.globus.org:all[{inner}]"


def save_tokens(token_response, endpoints: List[Optional[str]]):
    """
    Save tokens for a specific endpoint pair.
    """
    endpoint_key = get_endpoint_key(endpoints)

    # Load existing tokens
    all_tokens = load_tokens()

    # Prepare tokens for this endpoint pair
    tokens_to_save = {}
    for resource_server, token_data in token_response.by_resource_server.items():
        tokens_to_save[resource_server] = {
            "access_token": token_data["access_token"],
            "refresh_token": token_data.get("refresh_token"),
            "expires_at": token_data.get("expires_at_seconds"),
        }

    # Store under the endpoint key
    all_tokens[endpoint_key] = tokens_to_save

    # Save everything back to file
    save_tokens_to_file(all_tokens)


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
    token_file = get_token_file_path()

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
                f"One possible cause: {token_file} may be configured for a different Globus endpoint. For example, you may have previously set a different destination endpoint for `--hpss=globus://`."
            )
            logger.error(f"Try deleting {token_file} and re-running.")

            logger.error(
                "Another possible cause: insufficient Globus consents. It's possible the consent on https://auth.globus.org/v2/web/consents is for a different destination endpoint."
            )
            logger.error(
                "If you don't need any other Globus consents at the moment, try revoking consents before re-running: https://auth.globus.org/v2/web/consents > Manage Your Consents > Globus Endpoint Performance Monitoring > rescind all"
            )
            logger.error(
                "If neither of those work, please report this bug at https://github.com/E3SM-Project/zstash/issues, with details of what you were trying to do."
            )
            raise RuntimeError("Insufficient Globus consents") from err
        else:
            raise err
    return task
