from __future__ import print_function, absolute_import

import os.path
import shlex
import subprocess
from .settings import logger


def run_command(command, error_str):
    p1 = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (stdout, stderr) = p1.communicate()
    status = p1.returncode
    if status != 0:
        logger.error(error_str)
        logger.debug('stdout:\n%s', stdout)
        logger.debug('stderr:\n%s', stderr)
        raise Exception


def hpss_transfer(hpss, file_path, transfer_type, keep=None):
    if transfer_type == 'put':
        transfer_word = 'to'
        transfer_command = 'put'
    elif transfer_type == 'get':
        transfer_word = 'from'
        transfer_command = 'get'
    else:
        raise Exception('Invalid transfer_type={}'.format(transfer_type))
    logger.info('Transferring file {} HPSS: {}'.format(transfer_word, file_path))
    path, name = os.path.split(file_path)

    # Need to be in local directory for hsi put to work
    cwd = os.getcwd()
    if path != '':
        if (transfer_type == 'get') and (not os.path.isdir(path)):
            os.makedirs(path)
        os.chdir(path)

    # Transfer file using hsi put
    command = 'hsi -q "cd {}; {} {}"'.format(hpss, transfer_command, name)
    error_str = 'Transferring file {} HPSS: {}'.format(transfer_word, name)
    run_command(command, error_str)

    # Back to original working directory
    if path != '':
        os.chdir(cwd)

    if transfer_type == 'put':
        # Remove local file if requested
        if not keep:
            os.remove(file_path)


def hpss_put(hpss, file_path, keep=True):
    """
    Put a file to the HPSS archive.
    """
    hpss_transfer(hpss, file_path, 'put', keep)


def hpss_get(hpss, file_path):
    """
    Get a file from the HPSS archive.
    """
    hpss_transfer(hpss, file_path, 'get', False)


def hpss_chgrp(hpss, group, recurse=False):
    """
    Change the group of the HPSS archive.
    """
    if recurse:
        recurse_str = '-R '
    else:
        recurse_str = ''
    command = 'hsi chgrp {}{} {}'.format(recurse_str, group, hpss)
    error_str = 'Changing group of HPSS archive {} to {}'.format(hpss, group)
    run_command(command, error_str)
