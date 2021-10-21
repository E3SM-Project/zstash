import os
import shutil
import configparser
import socket
import re
import unittest
from fair_research_login.client import NativeClient
from globus_sdk import TransferClient, DeleteData
from globus_sdk.exc import TransferAPIError


from tests.base import (
    HPSS_ARCHIVE,
    TOP_LEVEL,
    ZSTASH_PATH,
    TestZstash,
    print_starred,
    run_cmd,
)

# Use 'Globus Tutorial Endpoint 1' to simulate an HPSS Globus endpoint
hpss_globus_endpoint = "ddb59aef-6d04-11e5-ba46-22000b92c6ec"

regex_endpoint_map = {
    "theta.*\.alcf\.anl\.gov": "08925f04-569f-11e7-bef8-22000b9a448b",
    "blueslogin.*\.lcrc\.anl\.gov": "61f9954c-a4fa-11ea-8f07-0a21f750d19b",
    "chr.*\.lcrc\.anl\.gov": "61f9954c-a4fa-11ea-8f07-0a21f750d19b",
    "cori.*\.nersc\.gov": "9d6d99eb-6d04-11e5-ba46-22000b92c6ec",
}


class TestGlobus(TestZstash):

    def preactivate_globus(self):
        """
        Read the local globus endpoint UUID from ~/.zstash.ini.
        If the ini file does not exist, create an ini file with empty values,
        and try to find the local endpoint UUID based on the FQDN
        """
        local_endpoint = None
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
                self.fail(e)
        if not local_endpoint:
            fqdn = socket.getfqdn()
            for pattern in regex_endpoint_map.keys():
                if re.fullmatch(pattern, fqdn):
                    local_endpoint = regex_endpoint_map.get(pattern)
                    break
        if not local_endpoint:
            self.fail("{} does not have the local Globus endpoint set".format(ini_path))

        native_client = NativeClient(
                client_id = "6c1629cf-446c-49e7-af95-323c6412397f",
                app_name="Zstash",
                default_scopes = "openid urn:globus:auth:scope:transfer.api.globus.org:all")
        native_client.login(no_local_server=True, refresh_tokens=True)
        transfer_authorizer = native_client.get_authorizers().get("transfer.api.globus.org")
        self.transfer_client = TransferClient(transfer_authorizer)

        for ep_id in [hpss_globus_endpoint, local_endpoint]:
            ep = self.transfer_client.get_endpoint(ep_id)
            if not ep.get("activated"):
                self.fail("The {} endpoint is not activated. Please go to https://app.globus.org/file-manager/collections/{} and activate the endpoint.".format(ep_id, ep_id))


    def delete_files_globus(self):
        ep = self.transfer_client.get_endpoint(hpss_globus_endpoint)
        if not ep.get("activated"):
            self.fail("The {} endpoint is not activated. Please go to https://app.globus.org/file-manager/collections/{} and activate the endpoint.".format(ep_id, ep_id))

        ddata = DeleteData(self.transfer_client, hpss_globus_endpoint, recursive=True)
        ddata.add_item("/~/zstash_test/")
        try:
            task = self.transfer_client.submit_delete(ddata)
            task_id = task.get("task_id")
            """
            A Globus transfer job (task) can be in one of the three states:
            ACTIVE, SUCCEEDED, FAILED. The script every 5 seconds polls a
            status of the transfer job (task) from the Globus Transfer service,
            with 5 second timeout limit. If the task is ACTIVE after time runs
            out 'task_wait' returns False, and True otherwise.
            """
            while not self.transfer_client.task_wait(task_id, 5, 5):
                task = self.transfer_client.get_task(task_id)
                if task.get("is_paused"):
                    break
            """
            The Globus transfer job (task) has been finished (SUCCEEDED or FAILED),
            or is still active (ACTIVE). Check if the transfer SUCCEEDED or FAILED.
            """
            task = self.transfer_client.get_task(task_id)
            if task["status"] == "SUCCEEDED":
                print("G")
            elif task.get("status") == "ACTIVE":
                if task.get("is_paused"):
                    pause_info = self.transfer_client.task_pause_info(task_id)
                    paused_rules = pause_info.get("pause_rules")
                    reason = paused_rules[0].get("message")
                    message = "The task was paused. Reason: {}".format(reason)
                    status = PAUSED
                    print(message)
                else:
                    message = "The task reached a {} second deadline\n".format(24*3600)
                    events = self.transfer_client.task_event_list(task_id, num_results=5, filter="is_error:1")
                    message += self.get_error_events(tc, task_id)
                    status = DEADLINE
                    print(message)
                self.tranasfer_client.cancel_task(task_id)
            else:
                print("Globus delete FAILED")
        except TransferAPIError as e:
            if e.code == "NoCredException":
                self.fail("{}. Please go to https://app.globus.org/endpoints and activate the endpoint.".format(e.message))
            else:
                self.fail(e)
        except Exception as e:
            self.fail("{} - exception: {}".format(self, e))

    def tearDown(self):
        """
        Tear down a test. This is run after every test method.

        After the script has failed or completed, remove all created files, even those on the HPSS repo.
        """
        os.chdir(TOP_LEVEL)
        print("Removing test files, both locally and at the HPSS repo")
        # self.cache may appear in any of these directories,
        # but should not appear at the same level as these.
        # Therefore, there is no need to explicitly remove it.
        for d in [self.test_dir, self.backup_dir]:
            if os.path.exists(d):
                shutil.rmtree(d)

        if self.hpss_path and self.hpss_path.lower().startswith("globus:"):
            self.delete_files_globus()

    def helperLsGlobus(self, test_name, hpss_path, cache=None, zstash_path=ZSTASH_PATH):
        """
        Test `zstash ls --hpss=globus://...`.
        """
        self.preactivate_globus()
        self.hpss_path = hpss_path
        if cache:
            # Override default cache
            self.cache = cache
            cache_option = " --cache={}".format(self.cache)
        else:
            cache_option = ""
        use_hpss = self.setupDirs(test_name)
        self.create(use_hpss, zstash_path, cache=self.cache)
        self.assertWorkspace()
        os.chdir(self.test_dir)
        for option in ["", "-v", "-l"]:
            print_starred("Testing zstash ls {}".format(option))
            cmd = "{}zstash ls{} {} --hpss={}".format(
                zstash_path, cache_option, option, self.hpss_path
            )
            output, err = run_cmd(cmd)
            self.check_strings(cmd, output + err, ["file0.txt"], ["ERROR"])
        os.chdir(TOP_LEVEL)

    def testLs(self):
        self.helperLsGlobus("testLsGlobus", f"globus://{hpss_globus_endpoint}/~/zstash_test/")


if __name__ == "__main__":
    unittest.main()