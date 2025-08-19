from __future__ import absolute_import, print_function

import os.path
import subprocess
from typing import List, Optional

from six.moves.urllib.parse import urlparse

from .globus import globus_transfer
from .settings import get_db_filename, logger
from .transfer_tracking import (
    GlobusTransferCollection,
    HPSSTransferCollection,
    delete_transferred_files,
)
from .utils import run_command, ts_utc


# C901 'hpss_transfer' is too complex (19)
def hpss_transfer(  # noqa: C901
    hpss: str,
    file_path: str,
    transfer_type: str,
    cache: str,
    keep: bool = False,
    non_blocking: bool = False,
    is_index: bool = False,
    gtc: Optional[GlobusTransferCollection] = None,
    htc: Optional[HPSSTransferCollection] = None,
):
    if not htc:
        htc = HPSSTransferCollection()
    logger.info(
        f"{ts_utc()}: in hpss_transfer, prev_transfers is starting as {htc.prev_transfers}"
    )
    # logger.debug(
    #     f"{ts_utc()}: in hpss_transfer, curr_transfers is starting as {htc.curr_transfers}"
    # )

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

        htc.curr_transfers.append(file_path)
        # logger.debug(
        #     f"{ts_utc()}: curr_transfers has been appended to, is now {htc.curr_transfers}"
        # )
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

        globus_status: Optional[str] = "UNKNOWN"
        if scheme == "globus":
            if not gtc:
                raise RuntimeError(
                    "Scheme is 'globus' but no GlobusTransferCollection provided"
                )
            # Transfer file using the Globus Transfer Service
            logger.info(f"{ts_utc()}: DIVING: hpss calls globus_transfer(name={name})")
            globus_transfer(gtc, endpoint, url_path, name, transfer_type, non_blocking)
            mrt = gtc.get_most_recent_transfer()
            if mrt:
                globus_status = mrt.task_status
                logger.info(
                    f"{ts_utc()}: SURFACE hpss globus_transfer(name={name}), task_id={mrt.task_id}, globus_status={globus_status}"
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
                if (scheme != "globus") or (globus_status == "SUCCEEDED"):
                    # Note: This is intended to fulfill the default removal of successfully-transfered
                    # tar files when keep=False, irrespective of non-blocking status
                    delete_transferred_files(htc)


def hpss_put(
    hpss: str,
    file_path: str,
    cache: str,
    keep: bool = True,
    non_blocking: bool = False,
    is_index=False,
    gtc: Optional[GlobusTransferCollection] = None,
    htc: Optional[HPSSTransferCollection] = None,
):
    """
    Put a file to the HPSS archive.
    """
    hpss_transfer(
        hpss, file_path, "put", cache, keep, non_blocking, is_index, gtc=gtc, htc=htc
    )


def hpss_get(hpss: str, file_path: str, cache: str):
    """
    Get a file from the HPSS archive.
    """
    # gtc will get set as part of globus_transfer
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
