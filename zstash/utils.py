from __future__ import absolute_import, print_function

import os
import shlex
import sqlite3
import subprocess
from datetime import datetime, timezone
from enum import Enum
from fnmatch import fnmatch
from typing import Any, List, Optional, Tuple
from urllib.parse import urlparse

from .settings import DEFAULT_CACHE, TupleTarsRow, config, logger


class HPSSType(Enum):
    NO_HPSS = 1
    SAME_MACHINE_HPSS = 2
    GLOBUS = 3


class GlobusInfo(object):
    def __init__(self, hpss_path: str):
        url = urlparse(hpss_path)
        if url.scheme != "globus":
            raise ValueError(f"Invalid Globus hpss_path={hpss_path}")
        self.hpss_path = hpss_path
        self.url = url
        
        # Set in globus.globus_activate
        self.remote_endpoint = None
        self.local_endpoint = None
        self.transfer_client = None

        # transfer_data = None
        # task_id = None
        # archive_directory_listing = None

# Class to hold configuration, as it appears in the database
class Config(object):
    path: Optional[str] = None
    hpss: Optional[str] = None
    maxsize: Optional[int] = None

class CommandInfo(object):
    def __init__(self, command_name: str):
        self.command_name = command_name
        self.dir_called_from = os.getcwd()
        self.cache_dir = DEFAULT_CACHE
        self.config = Config()
        self.keep = False # Defaults to False
        self.prev_transfers = []
        self.curr_transfers = []

        # Use set_dir_to_archive
        self.dir_to_archive_absolute = None
        self.dir_to_archive_relative = None

        # Use set_maxsize
        self.maxsize = None

        # Use set_hpss_parameters
        self.hpss_path = None
        self.hpss_type = None
        self.globus_info = None

    def set_dir_to_archive(self, path: str):
        abs_path = os.path.abspath(path)
        if abs_path is not None:
            self.dir_to_archive_absolute = abs_path
            self.dir_to_archive_relative = path
        else:
            raise ValueError(f"Invalid path={path}")

    def set_maxsize(self, maxsize):
        self.maxsize = int(1024 * 1024 * 1024 * maxsize)

    def set_hpss_parameters(self, hpss_path: str):
        self.hpss_path = hpss_path
        if hpss_path == "none":
            self.hpss_type = HPSSType.NO_HPSS
        elif hpss_path is not None:
            url = urlparse(hpss_path)
            if url.scheme == "globus":
                self.hpss_type = HPSSType.GLOBUS
                self.globus_info = GlobusInfo(hpss_path)
            else:
                self.hpss_type = HPSSType.SAME_MACHINE_HPSS
        else:
            raise ValueError(f"Invalid hpss_path={hpss_path}")
        
    def update_config(self):
        self.config.path = self.dir_to_archive_absolute
        self.config.hpss = self.hpss_path
        self.config.maxsize = self.maxsize

    def get_db_name(self) -> str:
        return os.path.join(self.cache_dir, "index.db")

    def list_cache_dir(self):
        logger.info(
            f"Contents of cache {self.cache_dir} = {os.listdir(self.cache_dir)}"
        )

    def list_hpss_path(self):
        if self.hpss_type == HPSSType.SAME_MACHINE_HPSS:
            command = "hsi ls -l {}".format(self.hpss_path)
            error_str = "Attempted to list contents at hpss_path={hpss_path}"
            run_command(command, error_str)
        else:
            logger.info("No HPSS path to list")


def ts_utc():
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")


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
