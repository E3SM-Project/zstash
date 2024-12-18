from __future__ import absolute_import, print_function

import os.path
import subprocess
from typing import List

from six.moves.urllib.parse import urlparse

from .globus import globus_transfer
from .settings import get_db_filename, logger
from .utils import run_command, ts_utc


def hpss_transfer(
    hpss: str,
    file_path: str,
    transfer_type: str,
    cache: str,
    keep: bool = False,
    non_blocking: bool = False,
):
    if hpss == "none":
        logger.info("{}: HPSS is unavailable".format(transfer_type))
        if transfer_type == "put" and file_path != get_db_filename(cache):
            # We are adding a file (that is not the cache) to the local non-HPSS archive
            logger.info(
                "{}: Keeping tar files locally and removing write permissions".format(
                    transfer_type
                )
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
        logger.info("Transferring file {} HPSS: {}".format(transfer_word, file_path))
        scheme: str
        endpoint: str
        path: str
        name: str

        url = urlparse(hpss)
        scheme = url.scheme
        endpoint = url.netloc
        url_path = url.path

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

        if scheme == "globus":
            globus_status = "UNKNOWN"
            # Transfer file using the Globus Transfer Service
            logger.info(f"{ts_utc()}: DIVING: hpss calls globus_transfer(name={name})")
            globus_status = globus_transfer(
                endpoint, url_path, name, transfer_type, non_blocking
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
            command: str = 'hsi -q "cd {}; {} {}"'.format(hpss, transfer_command, name)
            error_str: str = "Transferring file {} HPSS: {}".format(transfer_word, name)
            run_command(command, error_str)

        # Return to original working directory
        if path != "":
            os.chdir(cwd)

        if transfer_type == "put":
            if not keep:
              if (scheme != "globus") or (globus_status == "SUCCEEDED" and not non_blocking):
                os.remove(file_path)


def hpss_put(
    hpss: str, file_path: str, cache: str, keep: bool = True, non_blocking: bool = False
):
    """
    Put a file to the HPSS archive.
    """
    hpss_transfer(hpss, file_path, "put", cache, keep, non_blocking)


def hpss_get(hpss: str, file_path: str, cache: str):
    """
    Get a file from the HPSS archive.
    """
    hpss_transfer(hpss, file_path, "get", cache, False)


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
