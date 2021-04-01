from __future__ import absolute_import, print_function

import shlex
import subprocess
from fnmatch import fnmatch

from .settings import logger


def exclude_files(exclude, files):

    # Construct lits of files to exclude, based on
    #  https://codereview.stackexchange.com/questions/33624/
    #  filtering-a-long-list-of-files-through-a-set-of-ignore-patterns-using-iterators
    exclude_patterns = exclude.split(",")

    # If exclude pattern ends with a trailing '/', the user intends to exclude
    # the entire subdirectory content, therefore replace '/' with '/*'
    for i in range(len(exclude_patterns)):
        if exclude_patterns[i][-1] == "/":
            exclude_patterns[i] += "*"

    # Actual files to exclude
    exclude_files = []
    for file_name in files:
        if any(fnmatch(file_name, pattern) for pattern in exclude_patterns):
            exclude_files.append(file_name)
            continue

    # Now, remove them
    new_files = [f for f in files if f not in exclude_files]

    return new_files


def run_command(command, error_str):
    p1 = subprocess.Popen(
        shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    (stdout, stderr) = p1.communicate()
    status = p1.returncode
    if status != 0:
        error_str = "Error={}, Command was `{}`".format(error_str, command)
        if "hsi" in command:
            error_str = "{}. This command includes `hsi`. Be sure that you have logged into `hsi`.".format(
                error_str
            )
        logger.error(error_str)
        logger.debug("stdout:\n%s", stdout)
        logger.debug("stderr:\n%s", stderr)
        raise Exception(error_str)
