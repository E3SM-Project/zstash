from __future__ import absolute_import, print_function

import os.path
import subprocess

from .settings import get_db_filename, logger
from .utils import run_command


def hpss_transfer(hpss, file_path, transfer_type, cache, keep=None):
    if hpss == "none":
        logger.info("{}: HPSS is unavailable".format(transfer_type))
        if transfer_type == "put" and file_path != get_db_filename(cache):
            logger.info(
                "{}: Keeping tar files locally and removing write permissions".format(
                    transfer_type
                )
            )
            # https://unix.stackexchange.com/questions/46915/get-the-chmod-numerical-value-for-a-file
            display_mode = "stat --format '%a' {}".format(file_path).split()
            output = subprocess.check_output(display_mode).strip()
            # FIXME: On Python 3 '{}'.format(b'abc') produces "b'abc'", not 'abc'; use '{!r}'.format(b'abc') if this is desired behavior mypy(error)
            logger.info("{} original mode={}".format(file_path, output))  # type: ignore
            # https://www.washington.edu/doit/technology-tips-chmod-overview
            # Remove write-permission from user, group, and others,
            # without changing read or execute permissions for any.
            change_mode = "chmod ugo-w {}".format(file_path).split()
            subprocess.check_output(change_mode)
            output = subprocess.check_output(display_mode).strip()
            # FIXME: On Python 3 '{}'.format(b'abc') produces "b'abc'", not 'abc'; use '{!r}'.format(b'abc') if this is desired behavior mypy(error)
            logger.info("{} new mode={}".format(file_path, output))  # type: ignore
        return
    if transfer_type == "put":
        transfer_word = "to"
        transfer_command = "put"
    elif transfer_type == "get":
        transfer_word = "from"
        transfer_command = "get"
    else:
        raise Exception("Invalid transfer_type={}".format(transfer_type))
    logger.info("Transferring file {} HPSS: {}".format(transfer_word, file_path))
    path, name = os.path.split(file_path)

    # Need to be in local directory for hsi put to work
    cwd = os.getcwd()
    if path != "":
        if (transfer_type == "get") and (not os.path.isdir(path)):
            os.makedirs(path)
        os.chdir(path)

    # Transfer file using hsi put
    command = 'hsi -q "cd {}; {} {}"'.format(hpss, transfer_command, name)
    error_str = "Transferring file {} HPSS: {}".format(transfer_word, name)
    run_command(command, error_str)

    # Back to original working directory
    if path != "":
        os.chdir(cwd)

    if transfer_type == "put":
        # Remove local file if requested
        if not keep:
            os.remove(file_path)


def hpss_put(hpss, file_path, cache, keep=True):
    """
    Put a file to the HPSS archive.
    """
    hpss_transfer(hpss, file_path, "put", cache, keep)


def hpss_get(hpss, file_path, cache):
    """
    Get a file from the HPSS archive.
    """
    hpss_transfer(hpss, file_path, "get", cache, False)


def hpss_chgrp(hpss, group, recurse=False):
    """
    Change the group of the HPSS archive.
    """
    if hpss == "none":
        logger.info("chgrp: HPSS is unavailable")
        return
    if recurse:
        recurse_str = "-R "
    else:
        recurse_str = ""
    command = "hsi chgrp {}{} {}".format(recurse_str, group, hpss)
    error_str = "Changing group of HPSS archive {} to {}".format(hpss, group)
    run_command(command, error_str)
