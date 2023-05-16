from __future__ import absolute_import, print_function

import os
import shlex
import sqlite3
import subprocess
from fnmatch import fnmatch
from typing import Any, List, Tuple

from .settings import TupleTarsRow, config, logger


def filter_files(subset: str, files: List[str], include: bool) -> List[str]:

    # Construct list of files to filter, based on
    #  https://codereview.stackexchange.com/questions/33624/
    #  filtering-a-long-list-of-files-through-a-set-of-ignore-patterns-using-iterators
    subset_patterns: List[str] = subset.split(",")

    # If subset pattern ends with a trailing '/', the user intends to filter
    # the entire subdirectory content, therefore replace '/' with '/*'
    for i in range(len(subset_patterns)):
        if subset_patterns[i][-1] == "/":
            subset_patterns[i] += "*"

    # Actual files to filter
    subset_files: List[str] = []
    for file_name in files:
        if any(fnmatch(file_name, pattern) for pattern in subset_patterns):
            subset_files.append(file_name)

    # Now, filter those files
    if include:
        new_files = [f for f in files if f in subset_files]
    else:
        new_files = [f for f in files if f not in subset_files]

    return new_files


def exclude_files(exclude: str, files: List[str]) -> List[str]:
    return filter_files(exclude, files, include=False)


def include_files(include: str, files: List[str]) -> List[str]:
    return filter_files(include, files, include=True)


def run_command(command: str, error_str: str):
    p1: subprocess.Popen = subprocess.Popen(
        shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    stdout: bytes
    stderr: bytes
    (stdout, stderr) = p1.communicate()
    status: int = p1.returncode
    if status != 0:
        error_str = "Error={}, Command was `{}`".format(error_str, command)
        if "hsi" in command:
            error_str = f"{error_str}. This command includes `hsi`. Be sure that you have logged into `hsi`"
        if "cd" in command:
            error_str = f"{error_str}. This command includes `cd`. Check that this directory exists and contains the needed files"
        logger.error(error_str)
        logger.debug("stdout:\n{!r}".format(stdout))
        logger.debug("stderr:\n{!r}".format(stderr))
        raise RuntimeError(error_str)


def get_files_to_archive(cache: str, include: str, exclude: str) -> List[str]:
    # List of files
    logger.info("Gathering list of files to archive")
    # Tuples of the form (path, filename)
    file_tuples: List[Tuple[str, str]] = []
    # Walk the current directory
    for root, dirnames, filenames in os.walk("."):
        if not dirnames and not filenames:
            # There are no subdirectories nor are there files.
            # This directory is empty.
            file_tuples.append((root, ""))
        for filename in filenames:
            # Loop over files
            # filenames is a list, so if it is empty, no looping will occur.
            file_tuples.append((root, filename))

    # Sort first on directories (x[0])
    # Further sort on filenames (x[1])
    file_tuples = sorted(file_tuples, key=lambda x: (x[0], x[1]))

    # Relative file paths, excluding the cache
    files: List[str] = [
        os.path.normpath(os.path.join(x[0], x[1]))
        for x in file_tuples
        if x[0] != os.path.join(".", cache)
    ]

    # First, add files based on include pattern
    if include is not None:
        files = include_files(include, files)

    # Then, eliminate files based on exclude pattern
    if exclude is not None:
        files = exclude_files(exclude, files)

    return files


def update_config(cur: sqlite3.Cursor):
    # Retrieve some configuration settings from database
    # Loop through all attributes of config.
    for attr in dir(config):
        value: Any = getattr(config, attr)
        if not callable(value) and not attr.startswith("__"):
            # config.{attr} is not a function.
            # The attribute name does not start with "__"
            # Get the value (column 2) for attribute `attr` (column 1)
            # i.e., for the row where column 1 is the attribute, get the value from column 2
            cur.execute("select value from config where arg=?", (attr,))
            value = cur.fetchone()[0]
            # Update config with the new attribute-value pair
            setattr(config, attr, value)


def create_tars_table(cur: sqlite3.Cursor, con: sqlite3.Connection):
    # Create 'tars' table
    cur.execute(
        """
create table tars (
id integer primary key,
name text,
size integer,
md5 text
);
    """
    )
    con.commit()


def tars_table_exists(cur: sqlite3.Cursor) -> bool:
    # https://stackoverflow.com/questions/1601151/how-do-i-check-in-sqlite-whether-a-table-exists
    cur.execute("PRAGMA table_info(tars);")
    table_info_list: List[TupleTarsRow] = cur.fetchall()
    return True if table_info_list != [] else False
