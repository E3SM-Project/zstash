import configparser
import os
import re
import shutil
from typing import Optional
from urllib.parse import ParseResult, urlparse

from fair_research_login.client import NativeClient
from globus_sdk import TransferAPIError, TransferClient, TransferData
from globus_sdk.response import GlobusHTTPResponse

# Minimal example of how Globus is used in zstash
# 1. Log into endpoints at globus.org
# 2. To start fresh, with no consents:
# https://app.globus.org/settings/consents > Manage Your Consents > Globus Endpoint Performance Monitoring > rescind all"

HSI_DIR = "zstash_debugging_20250415_v2"

# Globus-specific settings ####################################################
GLOBUS_CFG: str = os.path.expanduser("~/.globus-native-apps.cfg")
INI_PATH: str = os.path.expanduser("~/.zstash.ini")
ZSTASH_CLIENT_ID: str = "6c1629cf-446c-49e7-af95-323c6412397f"
NAME_TO_ENDPOINT_MAP = {
    # "Globus Tutorial Collection 1": "6c54cade-bde5-45c1-bdea-f4bd71dba2cc",  # The Unit test endpoint
    "NERSC HPSS": "9cd89cfd-6d04-11e5-ba46-22000b92c6ec",
    "NERSC Perlmutter": "6bdc7956-fc0f-4ad2-989c-7aa5ee643a79",
}


# Functions ###################################################################
def main():
    base_dir = os.getcwd()
    print(f"Starting in {base_dir}")
    if os.path.exists(INI_PATH):
        os.remove(INI_PATH)
    if os.path.exists(GLOBUS_CFG):
        os.remove(GLOBUS_CFG)
    try:
        simple_transfer("toy_run")
    except RuntimeError:
        print("Now that we have the authentications, let's re-run.")
    # /global/homes/f/forsyth/.globus-native-apps.cfg does not exist. zstash will need to prompt for authentications twice, and then you will need to re-run.
    #
    # Might ask for 1st authentication prompt:
    # Please paste the following URL in a browser:
    # Authenticated for the 1st time!
    #
    # Might ask for 2nd authentication prompt:
    # Please paste the following URL in a browser:
    # Authenticated for the 2nd time!
    # Consents added, please re-run the previous command to start transfer
    # Now that we have the authentications, let's re-run.
    os.chdir(base_dir)
    print(f"Now in {os.getcwd()}")
    assert os.path.exists(INI_PATH)
    assert os.path.exists(GLOBUS_CFG)
    simple_transfer("real_run")
    # /global/homes/f/forsyth/.globus-native-apps.cfg exists. If this file does not have the proper settings, it may cause a TransferAPIError (e.g., 'Token is not active', 'No credentials supplied')
    #
    # Might ask for 1st authentication prompt:
    # Authenticated for the 1st time!
    #
    # Bypassed 2nd authentication.
    #
    # Wait for task to complete, wait_timeout=300
    print(f"To see transferred files, run: hsi ls {HSI_DIR}")
    # To see transferred files, run: hsi ls zstash_debugging_20250415_v2
    # Shows file0.txt


def simple_transfer(run_dir: str):
    hpss_path = f"globus://{NAME_TO_ENDPOINT_MAP['NERSC HPSS']}/~/{HSI_DIR}"
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
    url: ParseResult = urlparse(hpss_path)
    assert url.scheme == "globus"
    if os.path.exists(GLOBUS_CFG):
        print(
            f"{GLOBUS_CFG} exists. If this file does not have the proper settings, it may cause a TransferAPIError (e.g., 'Token is not active', 'No credentials supplied')"
        )
    else:
        print(
            f"{GLOBUS_CFG} does not exist. zstash will need to prompt for authentications twice, and then you will need to re-run."
        )
    config_path: str = os.path.abspath(dir_to_archive)
    assert os.path.isdir(config_path)
    remote_endpoint: str = url.netloc
    # Simulate globus_activate > set_local_endpoint
    ini = configparser.ConfigParser()
    local_endpoint: Optional[str] = None
    if ini.read(INI_PATH):
        if "local" in ini.sections():
            local_endpoint = ini["local"].get("globus_endpoint_uuid")
    else:
        ini["local"] = {"globus_endpoint_uuid": ""}
        with open(INI_PATH, "w") as f:
            ini.write(f)
    if not local_endpoint:
        nersc_hostname = os.environ.get("NERSC_HOST")
        assert nersc_hostname == "perlmutter"
        local_endpoint = NAME_TO_ENDPOINT_MAP["NERSC Perlmutter"]
    native_client = NativeClient(
        client_id=ZSTASH_CLIENT_ID,
        app_name="Zstash",
        default_scopes="openid urn:globus:auth:scope:transfer.api.globus.org:all",
    )
    # May print 'Please Paste your Auth Code Below:'
    # This is the 1st authentication prompt!
    print("Might ask for 1st authentication prompt:")
    native_client.login(no_local_server=True, refresh_tokens=True)
    print("Authenticated for the 1st time!")
    transfer_authorizer = native_client.get_authorizers().get("transfer.api.globus.org")
    transfer_client: TransferClient = TransferClient(authorizer=transfer_authorizer)
    for ep_id in [
        local_endpoint,
        remote_endpoint,
    ]:
        r = transfer_client.endpoint_autoactivate(ep_id, if_expires_in=600)
        assert r.get("code") != "AutoActivationFailed"
    os.chdir(config_path)
    print(f"Now in {os.getcwd()}")
    url_path: str = str(url.path)
    assert local_endpoint is not None
    src_path: str = os.path.join(os.getcwd(), txt_file)
    dst_path: str = os.path.join(url_path, txt_file)
    subdir = os.path.basename(os.path.normpath(url_path))
    subdir_label = re.sub("[^A-Za-z0-9_ -]", "", subdir)
    filename = txt_file.split(".")[0]
    label = subdir_label + " " + filename
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
    task: GlobusHTTPResponse
    try:
        task = transfer_client.submit_transfer(transfer_data)
        print("Bypassed 2nd authentication.")
    except TransferAPIError as err:
        if err.info.consent_required:
            scopes = "urn:globus:auth:scope:transfer.api.globus.org:all["
            for ep_id in [remote_endpoint, local_endpoint]:
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
    task_status = curr_task["status"]
    assert task_status == "SUCCEEDED"


# Run #########################################################################
if __name__ == "__main__":
    main()
