from __future__ import print_function, absolute_import

import os.path
import shlex
from subprocess import Popen, PIPE
from .settings import logger


def hpss_put(hpss, file, keep=True):
    """
    Put a file to the HPSS archive.
    """

    logger.info('Transferring file to HPSS: %s' % (file))
    path, name = os.path.split(file)

    # Need to be in local directory for hsi put to work
    cwd = os.getcwd()
    if path != '':
        os.chdir(path)

    # Transfer file using hsi put
    cmd = 'hsi -q "cd %s; put %s"' % (hpss, name)
    p1 = Popen(shlex.split(cmd), stdout=PIPE, stderr=PIPE)
    (stdout, stderr) = p1.communicate()
    status = p1.returncode
    if status != 0:
        logger.error('Transferring file to HPSS: %s' % (name))
        logger.debug('stdout:\n%s', stdout)
        logger.debug('stderr:\n%s', stderr)
        raise Exception

    # Back to original working directory
    if path != '':
        os.chdir(cwd)

    # Remove local file if requested
    if not keep:
        os.remove(file)


def hpss_get(hpss, file):
    """
    Get ia file from the HPSS archive.
    """

    logger.info('Transferring from HPSS: %s' % (file))
    path, name = os.path.split(file)

    # Need to be in local directory for hsi get to work
    cwd = os.getcwd()
    if path != '':
        if not os.path.isdir(path):
            os.makedirs(path)
        os.chdir(path)

    # Transfer file using hsi get
    cmd = 'hsi -q "cd %s; get %s"' % (hpss, name)
    p1 = Popen(shlex.split(cmd), stdout=PIPE, stderr=PIPE)
    (stdout, stderr) = p1.communicate()
    status = p1.returncode
    if status != 0:
        logger.error('Transferring file from HPSS: %s' % (name))
        logger.debug('stdout:\n%s', stdout)
        logger.debug('stderr:\n%s', stderr)
        raise Exception

    # Back to original working directory
    if path != '':
        os.chdir(cwd)

def hpss_chgrp(hpss, group, recurse=False):
    """
    Change the group of the HPSS archive.
    """
    if recurse:
        cmd = 'hsi chgrp -R {} {}'.format(group, hpss)
    else:
        cmd = 'hsi chgrp {} {}'.format(group, hpss)

    p1 = Popen(shlex.split(cmd), stdout=PIPE, stderr=PIPE)
    (stdout, stderr) = p1.communicate()
    status = p1.returncode
    if status != 0:
        logger.error('Changing group of HPSS archive {} to {}'.format(hpss, group))
        logger.debug('stdout:\n%s', stdout)
        logger.debug('stderr:\n%s', stderr)
        raise Exception

