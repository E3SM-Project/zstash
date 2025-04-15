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

HSI_DIR = "zstash_debugging_20250414_v4"

# Globus-specific settings ####################################################
GLOBUS_CFG: str = os.path.expanduser("~/.globus-native-apps.cfg")
INI_PATH: str = os.path.expanduser("~/.zstash.ini")
ZSTASH_CLIENT_ID: str = "6c1629cf-446c-49e7-af95-323c6412397f"
NAME_TO_ENDPOINT_MAP = {
    "Globus Tutorial Collection 1": "6c54cade-bde5-45c1-bdea-f4bd71dba2cc",  # The Unit test endpoint
    "NERSC HPSS": "9cd89cfd-6d04-11e5-ba46-22000b92c6ec",
    "NERSC Perlmutter": "6bdc7956-fc0f-4ad2-989c-7aa5ee643a79",
}


class GlobusInfo(object):
    def __init__(self, hpss_path: str):
        url: ParseResult = urlparse(hpss_path)
        assert url.scheme == "globus"
        self.hpss_path: str = hpss_path
        self.url: ParseResult = url
        self.remote_endpoint: Optional[str] = None
        self.local_endpoint: Optional[str] = None
        self.transfer_client: Optional[TransferClient] = None
        self.transfer_data: Optional[TransferData] = None
        self.task_id = None


# zstash general settings #####################################################
class Config(object):
    def __init__(self):
        self.path: Optional[str] = None
        self.hpss: Optional[str] = None
        self.maxsize: int = int(1024 * 1024 * 1024 * 256)


class CommandInfo(object):
    def __init__(self, dir_to_archive: str, hpss_path: str):
        self.config: Config = Config()
        # Simulate CommandInfo.set_dir_to_archive
        self.config.path = os.path.abspath(dir_to_archive)
        # Simulate CommandInfo.set_hpss_parameters
        self.config.hpss = hpss_path
        url: ParseResult = urlparse(hpss_path)
        assert url.scheme == "globus"
        self.globus_info: GlobusInfo = GlobusInfo(hpss_path)
        if os.path.exists(GLOBUS_CFG):
            print(
                f"{GLOBUS_CFG} exists. If this file does not have the proper settings, it may cause a TransferAPIError (e.g., 'Token is not active', 'No credentials supplied')"
            )
        else:
            print(
                f"{GLOBUS_CFG} does not exist. zstash will need to prompt for authentications twice, and then you will need to re-run."
            )


# Functions ###################################################################
def main():
    hpss_path = f"globus://{NAME_TO_ENDPOINT_MAP['NERSC HPSS']}/~/{HSI_DIR}"
    dir_to_archive: str = "dir_to_archive"
    base_dir = os.getcwd()
    toy_run(hpss_path, dir_to_archive)
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
    real_run(hpss_path, dir_to_archive)
    # /global/homes/f/forsyth/.globus-native-apps.cfg exists. If this file does not have the proper settings, it may cause a TransferAPIError (e.g., 'Token is not active', 'No credentials supplied')
    #
    # Might ask for 1st authentication prompt:
    # Authenticated for the 1st time!
    #
    # distilled_globus_block_wait. task_wait, retry_count=0
    # To see transferred files, run: hsi ls zstash_debugging_20250414_v2
    # Shows file0.txt
    print(f"To see transferred files, run: hsi ls {HSI_DIR}")


# Toy run to get everything set up correctly.
def toy_run(
    hpss_path: str,
    dir_to_archive: str,
):
    # Start fresh
    if os.path.exists(INI_PATH):
        os.remove(INI_PATH)
    if os.path.exists(GLOBUS_CFG):
        os.remove(GLOBUS_CFG)
    set_up_dirs("toy_run", dir_to_archive)
    try:
        distilled_create(
            hpss_path,
            dir_to_archive,
        )
    except RuntimeError:
        print("Now that we have the authentications, let's re-run.")


def real_run(
    hpss_path: str,
    dir_to_archive: str,
):
    # Start fresh
    assert os.path.exists(INI_PATH)
    assert os.path.exists(GLOBUS_CFG)
    set_up_dirs("real_run", dir_to_archive)
    distilled_create(
        hpss_path,
        dir_to_archive,
    )


def set_up_dirs(run_dir: str, dir_to_archive: str):
    if os.path.exists(run_dir):
        shutil.rmtree(run_dir)
    os.mkdir(run_dir)
    os.chdir(run_dir)
    os.mkdir(dir_to_archive)
    with open(f"{dir_to_archive}/file0.txt", "w") as f:
        f.write("file0 stuff")


# Distilled versions of zstash functions ######################################


def distilled_create(hpss_path: str, dir_to_archive: str):
    command_info = CommandInfo(dir_to_archive, hpss_path)
    print(command_info.config.path)
    assert command_info.config.path is not None
    assert os.path.isdir(command_info.config.path)

    # Begin simulating globus_activate ########################################
    command_info.globus_info.remote_endpoint = command_info.globus_info.url.netloc
    # Simulate globus_activate > set_local_endpoint
    ini = configparser.ConfigParser()
    if ini.read(INI_PATH):
        if "local" in ini.sections():
            command_info.globus_info.local_endpoint = ini["local"].get(
                "globus_endpoint_uuid"
            )
    else:
        ini["local"] = {"globus_endpoint_uuid": ""}
        with open(INI_PATH, "w") as f:
            ini.write(f)
    if not command_info.globus_info.local_endpoint:
        nersc_hostname = os.environ.get("NERSC_HOST")
        assert nersc_hostname == "perlmutter"
        command_info.globus_info.local_endpoint = NAME_TO_ENDPOINT_MAP[
            "NERSC Perlmutter"
        ]
    # Simulate globus_activate > set_clients
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
    command_info.globus_info.transfer_client = TransferClient(
        authorizer=transfer_authorizer
    )
    # Continue globus_activate
    for ep_id in [
        command_info.globus_info.local_endpoint,
        command_info.globus_info.remote_endpoint,
    ]:
        r = command_info.globus_info.transfer_client.endpoint_autoactivate(
            ep_id, if_expires_in=600
        )
        assert r.get("code") != "AutoActivationFailed"
    # End simulating globus_activate ##########################################

    os.chdir(command_info.config.path)
    file_path = os.path.join(command_info.config.path, "file0.txt")

    # Begin simulating hpss_put ###############################################
    url = urlparse(command_info.config.hpss)
    url_path: str = str(url.path)
    path: str
    name: str
    path, name = os.path.split(file_path)
    cwd: str = os.getcwd()
    if path != "":
        # This directory contains the file we want to transfer to HPSS.
        os.chdir(path)
    _ = distilled_globus_transfer(command_info.globus_info, url_path, name)
    if path != "":
        os.chdir(cwd)
    # End simulating hpss_put #################################################

    assert command_info.globus_info.transfer_data is None


def distilled_globus_transfer(
    globus_info: GlobusInfo, remote_path: str, name: str
) -> str:
    assert globus_info.local_endpoint is not None
    src_ep: str = globus_info.local_endpoint
    src_path: str = os.path.join(os.getcwd(), name)
    assert globus_info.remote_endpoint is not None
    dst_ep: str = globus_info.remote_endpoint
    dst_path: str = os.path.join(remote_path, name)
    subdir = os.path.basename(os.path.normpath(remote_path))
    subdir_label = re.sub("[^A-Za-z0-9_ -]", "", subdir)
    filename = name.split(".")[0]
    label = subdir_label + " " + filename
    assert globus_info.transfer_data is None
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
    task: GlobusHTTPResponse
    if globus_info.task_id:
        task = globus_info.transfer_client.get_task(globus_info.task_id)
        prev_task_status = task["status"]
        if prev_task_status == "ACTIVE":
            return "ACTIVE"

    # Begin simulating submit_transfer_with_checks ############################
    try:
        assert globus_info.transfer_client is not None
        task = globus_info.transfer_client.submit_transfer(globus_info.transfer_data)
    except TransferAPIError as err:
        if err.info.consent_required:
            scopes = "urn:globus:auth:scope:transfer.api.globus.org:all["
            for ep_id in [globus_info.remote_endpoint, globus_info.local_endpoint]:
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
    # End simulating submit_transfer_with_checks ##############################

    globus_info.task_id = task.get("task_id")
    # Nullify the submitted transfer data structure so that a new one will be created on next call.
    globus_info.transfer_data = None
    # wait_timeout = 7200 sec = 120 min = 2h
    # wait_timeout = 300 sec = 5 min
    wait_timeout = 300
    max_retries = 2

    # Begin simulating globus_block_wait ######################################
    task_status = "UNKNOWN"
    retry_count = 0
    while retry_count < max_retries:
        try:
            # Wait for the task to complete
            assert globus_info.transfer_client is not None
            print(f"task_wait, retry_count={retry_count}")
            globus_info.transfer_client.task_wait(
                globus_info.task_id, timeout=wait_timeout, polling_interval=10
            )
        except Exception as e:
            print(f"Unexpected Exception: {e}")
        else:
            assert globus_info.transfer_client is not None
            curr_task: GlobusHTTPResponse = globus_info.transfer_client.get_task(
                globus_info.task_id
            )
            task_status = curr_task["status"]
            if task_status == "SUCCEEDED":
                break
        finally:
            retry_count += 1
    if retry_count == max_retries:
        task_status = "EXHAUSTED_TIMEOUT_RETRIES"
    # End simulating globus_block_wait ######################################

    return task_status


# Run #########################################################################
if __name__ == "__main__":
    main()
