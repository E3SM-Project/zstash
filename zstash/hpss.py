from __future__ import absolute_import, print_function

import os.path
import subprocess
from typing import List

from six.moves.urllib.parse import urlparse

from .globus import globus_transfer
from .settings import logger
from .utils import CommandInfo, HPSSType, run_command, ts_utc


def hpss_transfer(
    command_info: CommandInfo,
    file_path: str,
    transfer_type: str,
    non_blocking: bool = False,
):

    logger.info(
        f"{ts_utc()}: in hpss_transfer, prev_transfers is starting as {command_info.prev_transfers}"
    )
    # TODO: Expected output for tests needs to be changed if we uncomment this:
    # logger.debug(
    #     f"{ts_utc()}: in hpss_transfer, curr_transfers is starting as {command_info.curr_transfers}"
    # )

    if command_info.hpss_type == HPSSType.NO_HPSS:
        logger.info(f"{transfer_type}: HPSS is unavailable")
        if transfer_type == "put" and file_path != command_info.get_db_name():
            # We are adding a file (that is NOT the database) to the local non-HPSS archive
            logger.info(
                f"{transfer_type}: Keeping tar files locally and removing write permissions"
            )
            # https://unix.stackexchange.com/questions/46915/get-the-chmod-numerical-value-for-a-file
            display_mode_command: List[str] = "stat --format '%a' {}".format(
                file_path
            ).split()
            display_mode_output: bytes = subprocess.check_output(
                display_mode_command
            ).strip()
            logger.info(
                "{!r} original mode={!r}".format(file_path, display_mode_output)
            )
            # https://www.washington.edu/doit/technology-tips-chmod-overview
            # Remove write-permission from user, group, and others,
            # without changing read or execute permissions for any.
            change_mode_command: List[str] = "chmod ugo-w {}".format(file_path).split()
            # An error will be raised if this line fails.
            subprocess.check_output(change_mode_command)
            new_display_mode_output: bytes = subprocess.check_output(
                display_mode_command
            ).strip()
            logger.info("{!r} new mode={!r}".format(file_path, new_display_mode_output))
        # else: no action needed
    else:
        transfer_word: str
        transfer_command: str
        if transfer_type == "put":
            transfer_word = "to"
            transfer_command = "put"
        elif transfer_type == "get":
            transfer_word = "from"
            transfer_command = "get"
        else:
            raise ValueError("Invalid transfer_type={}".format(transfer_type))
        logger.info(f"Transferring file {transfer_word} HPSS: {file_path}")

        url = urlparse(command_info.config.hpss)
        endpoint: str = str(url.netloc)
        url_path: str = str(url.path)

        command_info.curr_transfers.append(file_path)
        # TODO: Expected output for tests needs to be changed if we uncomment this:
        # logger.debug(
        #     f"{ts_utc()}: curr_transfers has been appended to, is now {command_info.curr_transfers}"
        # )
        path: str
        name: str
        path, name = os.path.split(file_path)

        # Need to be in local directory for `hsi` to work
        cwd = os.getcwd()
        if path != "":
            if (transfer_type == "get") and (not os.path.isdir(path)):
                # We are getting a file from HPSS.
                # The directory the file is in doesn't exist locally.
                # So, make the path locally
                os.makedirs(path)
            # Enter the path (directory)
            # For `put`, this directory contains the file we want to transfer to HPSS.
            # For `get`, this directory is where the file we get from HPSS will go.
            os.chdir(path)

        globus_status = None
        if command_info.hpss_type == HPSSType.GLOBUS:
            globus_status = "UNKNOWN"
            # Transfer file using the Globus Transfer Service
            logger.info(f"{ts_utc()}: DIVING: hpss calls globus_transfer(name={name})")
            if not command_info.globus_info:
                raise ValueError("globus_info is undefined")
            globus_status = globus_transfer(
                command_info.globus_info,
                endpoint,
                url_path,
                name,
                transfer_type,
                non_blocking,
            )
            logger.info(
                f"{ts_utc()}: SURFACE hpss globus_transfer(name={name}) returns {globus_status}"
            )
            # NOTE: Here, the status could be "EXHAUSTED_TIMEOUT_RETRIES", meaning a very long transfer
            # or perhaps transfer is hanging. We should decide whether to ignore it, or cancel it, but
            # we'd need the task_id to issue a cancellation.  Perhaps we should have globus_transfer
            # return a tuple (task_id, status).
        else:
            # Transfer file using `hsi`
            command: str = (
                f'hsi -q "cd {command_info.config.hpss}; {transfer_command} {name}"'
            )
            error_str: str = f"Transferring file {transfer_word} HPSS: {name}"
            run_command(command, error_str)

        # Return to original working directory
        if path != "":
            os.chdir(cwd)

        if transfer_type == "put":
            if not command_info.keep:
                if (command_info.hpss_type != HPSSType.GLOBUS) or (
                    globus_status == "SUCCEEDED"
                ):
                    # Note: This is intended to fulfill the default removal of successfully-transfered
                    # tar files when keep=False, irrespective of non-blocking status
                    logger.debug(
                        f"{ts_utc()}: deleting transfered files {command_info.prev_transfers}"
                    )
                    for src_path in command_info.prev_transfers:
                        os.remove(src_path)
                    command_info.prev_transfers = command_info.curr_transfers
                    command_info.curr_transfers = list()
                    logger.info(
                        f"{ts_utc()}: prev_transfers has been set to {command_info.prev_transfers}"
                    )


def hpss_put(
    command_info: CommandInfo,
    file_path: str,
    non_blocking: bool = False,
):
    """
    Put a file to the HPSS archive.
    """
    hpss_transfer(command_info, file_path, "put", non_blocking)


def hpss_get(command_info: CommandInfo, file_path: str):
    """
    Get a file from the HPSS archive.
    """
    hpss_transfer(command_info, file_path, "get")


def hpss_chgrp(hpss: str, group: str, recurse: bool = False):
    """
    Change the group of the HPSS archive.
    """
    if hpss == "none":
        logger.info("chgrp: HPSS is unavailable")
    else:
        recurse_str: str
        if recurse:
            recurse_str = "-R "
        else:
            recurse_str = ""
        command: str = "hsi chgrp {}{} {}".format(recurse_str, group, hpss)
        error_str: str = "Changing group of HPSS archive {} to {}".format(hpss, group)
        run_command(command, error_str)
