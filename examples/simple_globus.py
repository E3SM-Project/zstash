import os
import re
import shutil
from typing import List, Tuple
from urllib.parse import ParseResult, urlparse

from fair_research_login.client import NativeClient
from globus_sdk import TransferAPIError, TransferClient, TransferData
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

Case 1: authenticate 2x on toy run, 0x on real run
python simple_globus.py # REQUEST_SCOPES_EARLY=False
# TOY RUN:
# Prompts 1st time for auth code, no login requested
# Prompts 2nd time for auth code:
# > Argonne prompt > enter Argonne credentials
# > NERSC prompt > no login requested
# "Consents added, please re-run the previous command to start transfer"
# "Now that we have the authentications, let's re-run."
# REAL RUN:
# "Might ask for 1st authentication prompt:"
# No prompt at all!
# "Authenticated for the 1st time!"

# Reset Globus state, as described above

# Case 2: authenticate 1x on toy run, 1x on real run
python simple_globus.py # REQUEST_SCOPES_EARLY=True
# TOY RUN:
# Prompts 1st time for auth code:
# > Argonne prompt > no login requested
# > NERSC prompt > no login requested
# "Bypassed 2nd authentication."
# "We didn't need to authenticate a second time on the toy run!"
# REAL RUN:
# Prompts 1st time for auth code, no login requested
# "Bypassed 2nd authentication."
"""

# Settings ####################################################################
REQUEST_SCOPES_EARLY: bool = True
REMOTE_DIR_PREFIX: str = "zstash_simple_globus_try2"

LOCAL_ENDPOINT: str = "LCRC Improv DTN"
REMOTE_ENDPOINT: str = "NERSC Perlmutter"

# Constants ###################################################################
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
    skipped_second_auth: bool = False
    try:
        skipped_second_auth = simple_transfer("toy_run")
    except RuntimeError:
        print("Now that we have the authentications, let's re-run.")
    print(f"For toy_run, skipped_second_auth={skipped_second_auth}")
    if skipped_second_auth:
        print("We didn't need to authenticate a second time on the toy run!")
    os.chdir(base_dir)
    print(f"Now in {os.getcwd()}")
    skipped_second_auth = simple_transfer("real_run")
    print(f"For real_run, skipped_second_auth={skipped_second_auth}")
    assert skipped_second_auth


def simple_transfer(run_dir: str) -> bool:
    remote_path = f"globus://{NAME_TO_ENDPOINT_MAP[REMOTE_ENDPOINT]}/~/{REMOTE_DIR_PREFIX}_{run_dir}"
    config_path: str
    txt_file: str
    config_path, txt_file = get_dir_and_file_to_archive(run_dir)
    url: ParseResult = urlparse(remote_path)
    assert url.scheme == "globus"
    remote_endpoint: str = url.netloc
    print(f"url.scheme={url.scheme}, url.netloc={url.netloc}")
    local_endpoint: str = NAME_TO_ENDPOINT_MAP[LOCAL_ENDPOINT]
    both_endpoints: List[str] = [local_endpoint, remote_endpoint]
    native_client = NativeClient(
        client_id=ZSTASH_CLIENT_ID,
        app_name="Zstash",
        default_scopes="openid urn:globus:auth:scope:transfer.api.globus.org:all",
    )
    # May print 'Please Paste your Auth Code Below:'
    # This is the 1st authentication prompt!
    print("Might ask for 1st authentication prompt:")
    if REQUEST_SCOPES_EARLY:
        all_scopes: str = get_all_endpoint_scopes(both_endpoints)
        native_client.login(
            requested_scopes=all_scopes, no_local_server=True, refresh_tokens=True
        )
    else:
        native_client.login(no_local_server=True, refresh_tokens=True)
    print("Authenticated for the 1st time!")
    transfer_authorizer = native_client.get_authorizers().get("transfer.api.globus.org")
    transfer_client: TransferClient = TransferClient(authorizer=transfer_authorizer)
    for ep_id in both_endpoints:
        r = transfer_client.endpoint_autoactivate(ep_id, if_expires_in=600)
        assert r.get("code") != "AutoActivationFailed"
    os.chdir(config_path)
    print(f"Now in {os.getcwd()}")
    transfer_data: TransferData = construct_TransferData(
        url, txt_file, transfer_client, local_endpoint, remote_endpoint
    )
    task: GlobusHTTPResponse
    skipped_second_auth: bool = False
    try:
        task = transfer_client.submit_transfer(transfer_data)
        print("Bypassed 2nd authentication.")
        skipped_second_auth = True
    except TransferAPIError as err:
        if err.info.consent_required:
            scopes = "urn:globus:auth:scope:transfer.api.globus.org:all["
            for ep_id in both_endpoints:
                scopes += f" *https://auth.globus.org/scopes/{ep_id}/data_access"
            scopes += " ]"
            native_client = NativeClient(client_id=ZSTASH_CLIENT_ID, app_name="Zstash")
            # May print 'Please Paste your Auth Code Below:'
            # This is the 2nd authentication prompt!
            print("Might ask for 2nd authentication prompt:")
            native_client.login(requested_scopes=scopes)
            print("Authenticated for the 2nd time!")
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


def get_all_endpoint_scopes(endpoints: List[str]) -> str:
    inner = " ".join(
        [f"*https://auth.globus.org/scopes/{ep}/data_access" for ep in endpoints]
    )
    return f"urn:globus:auth:scope:transfer.api.globus.org:all[{inner}]"


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


# Run #########################################################################
if __name__ == "__main__":
    main()
