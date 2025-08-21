import configparser
import json
import os
import re
import shutil
from typing import List, Optional, Tuple
from urllib.parse import ParseResult, urlparse

from globus_sdk import (
    NativeAppAuthClient,
    RefreshTokenAuthorizer,
    TransferAPIError,
    TransferClient,
    TransferData,
)
from globus_sdk.response import GlobusHTTPResponse

"""
Minimal example of how Globus is used in zstash

To start fresh with Globus:
1. Log into endpoints at globus.org: File Manager > Add the endpoints in the "Collection" fields
2. To start fresh, with no consents: https://auth.globus.org/v2/web/consents > Manage Your Consents > Globus Endpoint Performance Monitoring > rescind all"

To run on Chrysalis:

# Set up environment
lcrc_conda # Function to set up conda
rm -rf build/
conda clean --all --y
conda env create -f conda/dev.yml -n zstash-simple-globus-script-20250804
conda activate zstash-simple-globus-script-20250804
pre-commit run --all-files
python -m pip install .
cd examples

# Run

# Reset Globus state, as described above

# Case 1: REQUEST_SCOPES_EARLY=False
python simple_globus.py

# Reset Globus state, as described above

# Case 2: REQUEST_SCOPES_EARLY=True
python simple_globus.py
"""

# Settings ####################################################################
REQUEST_SCOPES_EARLY: bool = True
REMOTE_DIR_PREFIX: str = "zstash_simple_globus_try6"

LOCAL_ENDPOINT: str = "LCRC Improv DTN"
REMOTE_ENDPOINT: str = "NERSC Perlmutter"

# Constants ###################################################################
GLOBUS_CFG: str = os.path.expanduser("~/.globus-native-apps.cfg")
INI_PATH: str = os.path.expanduser("~/.zstash.ini")
TOKEN_FILE = os.path.expanduser("~/.zstash_globus_tokens.json")
ZSTASH_CLIENT_ID: str = "6c1629cf-446c-49e7-af95-323c6412397f"
NAME_TO_ENDPOINT_MAP = {
    "NERSC HPSS": "9cd89cfd-6d04-11e5-ba46-22000b92c6ec",
    "NERSC Perlmutter": "6bdc7956-fc0f-4ad2-989c-7aa5ee643a79",
    "LCRC Improv DTN": "15288284-7006-4041-ba1a-6b52501e49f1",
}


# Functions ###################################################################
def main():
    base_dir = os.getcwd()
    print(f"Starting in {base_dir}")
    reset_state_files()
    skipped_second_auth: bool = False
    try:
        skipped_second_auth = simple_transfer("toy_run")
    except RuntimeError:
        print("Now that we have the authentications, let's re-run.")
        skipped_second_auth = simple_transfer("toy_run")
    print(f"For toy_run, skipped_second_auth={skipped_second_auth}")
    if skipped_second_auth:
        print("We didn't need to authenticate a second time on the toy run!")
    os.chdir(base_dir)
    print(f"Now in {os.getcwd()}")
    skipped_second_auth = simple_transfer("real_run")
    print(f"For real_run, skipped_second_auth={skipped_second_auth}")
    assert skipped_second_auth


def reset_state_files():
    files_to_remove = [INI_PATH, GLOBUS_CFG, TOKEN_FILE]
    for file_path in files_to_remove:
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"Removed {file_path}")


def simple_transfer(run_dir: str) -> bool:
    remote_path = f"globus://{NAME_TO_ENDPOINT_MAP[REMOTE_ENDPOINT]}/~/{REMOTE_DIR_PREFIX}_{run_dir}"
    config_path: str
    txt_file: str
    config_path, txt_file = get_dir_and_file_to_archive(run_dir)
    url: ParseResult = urlparse(remote_path)
    assert url.scheme == "globus"
    check_state_files()
    remote_endpoint: str = url.netloc
    print(f"url.scheme={url.scheme}, url.netloc={url.netloc}")
    local_endpoint: str = get_local_endpoint_id()
    both_endpoints: List[str] = [local_endpoint, remote_endpoint]

    # Get the transfer client with proper authentication
    transfer_client, needed_fresh_auth = get_transfer_client_with_auth(both_endpoints)

    for ep_id in both_endpoints:
        r = transfer_client.endpoint_autoactivate(ep_id, if_expires_in=600)
        assert r.get("code") != "AutoActivationFailed"

    os.chdir(config_path)
    print(f"Now in {os.getcwd()}")
    transfer_data: TransferData = construct_TransferData(
        url, txt_file, transfer_client, local_endpoint, remote_endpoint
    )

    task: GlobusHTTPResponse
    skipped_second_auth: bool = not needed_fresh_auth

    try:
        task = transfer_client.submit_transfer(transfer_data)
        if not needed_fresh_auth:
            print("Bypassed authentication entirely - used stored tokens!")
        else:
            print("Used fresh authentication - tokens now stored for next time")
        skipped_second_auth = True
    except TransferAPIError as err:
        if err.info.consent_required:
            # This should be much less likely now with proper scope handling
            print("Consent required - this suggests scope issues")
            skipped_second_auth = False

            scopes = "urn:globus:auth:scope:transfer.api.globus.org:all["
            for ep_id in both_endpoints:
                scopes += f" *https://auth.globus.org/scopes/{ep_id}/data_access"
            scopes += " ]"

            print("Getting additional consents...")
            client = NativeAppAuthClient(ZSTASH_CLIENT_ID)
            client.oauth2_start_flow(requested_scopes=scopes, refresh_tokens=True)
            authorize_url = client.oauth2_get_authorize_url()
            print("Please go to this URL and login: {0}".format(authorize_url))
            auth_code = input(
                "Please enter the code you get after login here: "
            ).strip()
            token_response = client.oauth2_exchange_code_for_tokens(auth_code)
            save_tokens(token_response)

            print(
                "Consents added, please re-run the previous command to start transfer"
            )
            raise RuntimeError("Re-run now that authentications are set up!")
        else:
            if err.info.authorization_parameters:
                print("Error is in authorization parameters")
            raise err

    task_id = task.get("task_id")
    wait_timeout = 300  # 300 sec = 5 min
    print(f"Wait for task to complete, wait_timeout={wait_timeout}")
    transfer_client.task_wait(task_id, timeout=wait_timeout, polling_interval=10)
    curr_task: GlobusHTTPResponse = transfer_client.get_task(task_id)
    assert curr_task["status"] == "SUCCEEDED"
    return skipped_second_auth


def get_dir_and_file_to_archive(run_dir: str) -> Tuple[str, str]:
    if os.path.exists(run_dir):
        shutil.rmtree(run_dir)
    os.mkdir(run_dir)
    os.chdir(run_dir)
    print(f"Now in {os.getcwd()}")
    dir_to_archive: str = "dir_to_archive"
    txt_file: str = "file0.txt"
    os.mkdir(dir_to_archive)
    with open(f"{dir_to_archive}/{txt_file}", "w") as f:
        f.write("file contents")
    config_path: str = os.path.abspath(dir_to_archive)
    assert os.path.isdir(config_path)
    return config_path, txt_file


def check_state_files():
    files_to_check = [
        (INI_PATH, "INI_PATH"),
        (GLOBUS_CFG, "GLOBUS_CFG"),
        (TOKEN_FILE, "TOKEN_FILE"),
    ]
    for file_path, name in files_to_check:
        if os.path.exists(file_path):
            print(f"{name}: {file_path} exists.")
        else:
            print(f"{name}: {file_path} does NOT exist.")


def get_local_endpoint_id() -> str:
    ini = configparser.ConfigParser()
    local_endpoint: Optional[str] = None
    if ini.read(INI_PATH):
        if "local" in ini.sections():
            local_endpoint = ini["local"].get("globus_endpoint_uuid")
            print("Got local_endpoint from ini file")
    else:
        ini["local"] = {"globus_endpoint_uuid": ""}
        with open(INI_PATH, "w") as f:
            ini.write(f)
        print("Added local_endpoint to ini file")
    if not local_endpoint:
        local_endpoint = NAME_TO_ENDPOINT_MAP[LOCAL_ENDPOINT]
        print("Got local endpoint from NAME_TO_ENDPOINT_MAP")
    return local_endpoint


def get_transfer_client_with_auth(
    both_endpoints: List[str],
) -> Tuple[TransferClient, bool]:
    """
    Get a TransferClient, handling authentication properly.
    Returns (transfer_client, needed_fresh_auth)
    """
    tokens = load_tokens()

    # Check if we have stored refresh tokens
    if "transfer.api.globus.org" in tokens:
        token_data = tokens["transfer.api.globus.org"]
        if "refresh_token" in token_data:
            print("Found stored refresh token - using it")
            # Create a simple auth client for the RefreshTokenAuthorizer
            auth_client = NativeAppAuthClient(ZSTASH_CLIENT_ID)
            transfer_authorizer = RefreshTokenAuthorizer(
                refresh_token=token_data["refresh_token"], auth_client=auth_client
            )
            transfer_client = TransferClient(authorizer=transfer_authorizer)
            return transfer_client, False  # No fresh auth needed

    # No stored tokens, need to authenticate
    print("No stored tokens found - starting authentication")

    # Get the required scopes
    if REQUEST_SCOPES_EARLY:
        all_scopes = get_all_endpoint_scopes(both_endpoints)
        print(f"Requesting scopes early: {all_scopes}")
    else:
        all_scopes = "urn:globus:auth:scope:transfer.api.globus.org:all"

    # Use the NativeAppAuthClient pattern from the documentation
    client = NativeAppAuthClient(ZSTASH_CLIENT_ID)
    client.oauth2_start_flow(
        requested_scopes=all_scopes,
        refresh_tokens=True,  # This is the key to persistent auth!
    )

    authorize_url = client.oauth2_get_authorize_url()
    print("Please go to this URL and login: {0}".format(authorize_url))

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
    return transfer_client, True  # Fresh auth was needed


def get_all_endpoint_scopes(endpoints: List[str]) -> str:
    inner = " ".join(
        [f"*https://auth.globus.org/scopes/{ep}/data_access" for ep in endpoints]
    )
    return f"urn:globus:auth:scope:transfer.api.globus.org:all[{inner}]"


def save_tokens(token_response):
    """Save tokens from a token response."""
    tokens_to_save = {}
    for resource_server, token_data in token_response.by_resource_server.items():
        tokens_to_save[resource_server] = {
            "access_token": token_data["access_token"],
            "refresh_token": token_data.get("refresh_token"),
            "expires_at": token_data.get("expires_at_seconds"),
        }

    with open(TOKEN_FILE, "w") as f:
        json.dump(tokens_to_save, f, indent=2)
    print("Tokens saved successfully")


def construct_TransferData(
    url: ParseResult,
    txt_file: str,
    transfer_client: TransferClient,
    local_endpoint: str,
    remote_endpoint: str,
) -> TransferData:
    url_path: str = str(url.path)
    src_path: str = os.path.join(os.getcwd(), txt_file)
    dst_path: str = os.path.join(url_path, txt_file)
    subdir = os.path.basename(os.path.normpath(url_path))
    subdir_label: str = re.sub("[^A-Za-z0-9_ -]", "", subdir)
    filename: str = txt_file.split(".")[0]
    label: str = subdir_label + " " + filename
    transfer_data: TransferData = TransferData(
        transfer_client,
        local_endpoint,  # src_ep
        remote_endpoint,  # dst_ep
        label=label,
        verify_checksum=True,
        preserve_timestamp=True,
        fail_on_quota_errors=True,
    )
    transfer_data.add_item(src_path, dst_path)
    transfer_data["label"] = label
    return transfer_data


def load_tokens():
    """Load stored tokens if they exist."""
    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


# Run #########################################################################
if __name__ == "__main__":
    main()
