from __future__ import absolute_import, print_function

import os
import shlex
import sqlite3
import subprocess
from datetime import datetime, timezone
from enum import Enum
from fnmatch import fnmatch
from typing import Any, List, Optional, Tuple
from urllib.parse import ParseResult, urlparse

from globus_sdk import TransferClient, TransferData
from globus_sdk.services.transfer.response.iterable import IterableTransferResponse

from .settings import TupleTarsRow, logger


class HPSSType(Enum):
    NO_HPSS = 1
    SAME_MACHINE_HPSS = 2
    GLOBUS = 3
    UNDEFINED = 4


class GlobusInfo(object):
    def __init__(self, hpss_path: str):
        url: ParseResult = urlparse(hpss_path)
        if url.scheme != "globus":
            raise ValueError(f"Invalid Globus hpss_path={hpss_path}")
        self.hpss_path: str = hpss_path
        self.url: ParseResult = url

        # Set in globus.globus_activate
        self.remote_endpoint: Optional[str] = None
        self.local_endpoint: Optional[str] = None
        self.transfer_client: Optional[TransferClient] = None

        # Set in globus.globus_transfer
        self.archive_directory_listing: Optional[IterableTransferResponse] = None
        self.transfer_data: Optional[TransferData] = None
        self.task_id = None
        self.tarfiles_pushed: int = 0


# Class to hold configuration, as it appears in the database
class Config(object):
    path: Optional[str] = None
    hpss: Optional[str] = None
    maxsize: Optional[int] = None


class CommandInfo(object):

    def __init__(self, command_name: str):
        self.command_name: str = command_name
        self.dir_called_from: str = os.getcwd()
        self.cache_dir: str = "zstash"  # # Default sub-directory to hold cache
        self.config: Config = Config()
        self.keep: bool = False  # Defaults to False
        self.prev_transfers: List[str] = []
        self.curr_transfers: List[str] = []
        # Use set_dir_to_archive:
        self.dir_to_archive_relative: Optional[str] = None
        # Use set_hpss_parameters:
        self.hpss_type: Optional[HPSSType] = None
        self.globus_info: Optional[GlobusInfo] = None

    def set_dir_to_archive(self, path: str):
        abs_path = os.path.abspath(path)
        if abs_path is not None:
            self.config.path = abs_path
            self.dir_to_archive_relative = path
        else:
            raise ValueError(f"Invalid path={path}")

    def set_and_scale_maxsize(self, maxsize):
        self.config.maxsize = int(1024 * 1024 * 1024 * maxsize)

    def validate_maxsize(self):
        if self.config.maxsize is not None:
            self.config.maxsize = int(self.config.maxsize)
        else:
            raise ValueError("config.maxsize is undefined")

    def set_hpss_parameters(self, hpss_path: str, null_hpss_allowed=False):
        self.config.hpss = hpss_path
        if hpss_path == "none":
            self.hpss_type = HPSSType.NO_HPSS
        elif hpss_path is not None:
            url = urlparse(hpss_path)
            if url.scheme == "globus":
                self.hpss_type = HPSSType.GLOBUS
                self.globus_info = GlobusInfo(hpss_path)
                globus_cfg: str = os.path.expanduser("~/.globus-native-apps.cfg")
                logger.info(f"Checking if {globus_cfg} exists")
                if os.path.exists(globus_cfg):
                    logger.info(
                        f"{globus_cfg} exists. If this file does not have the proper settings, it may cause a TransferAPIError (e.g., 'Token is not active', 'No credentials supplied')"
                    )
                else:
                    logger.info(
                        f"{globus_cfg} does not exist. zstash will need to prompt for authentications twice, and then you will need to re-run."
                    )
            else:
                self.hpss_type = HPSSType.SAME_MACHINE_HPSS
        elif null_hpss_allowed:
            self.hpss_type = HPSSType.UNDEFINED
        else:
            raise ValueError("hpss_path is undefined")
        logger.debug(f"Setting hpss_type={self.hpss_type}")
        logger.debug(f"Setting hpss={self.config.hpss}")

    def update_config_using_db(self, cur: sqlite3.Cursor):
        # Retrieve some configuration settings from database
        # Loop through all attributes of config.
        for attr in dir(self.config):
            value: Any = getattr(self.config, attr)
            if not callable(value) and not attr.startswith("__"):
                # config.{attr} is not a function.
                # The attribute name does not start with "__"
                # Get the value (column 2) for attribute `attr` (column 1)
                # i.e., for the row where column 1 is the attribute, get the value from column 2
                cur.execute("select value from config where arg=?", (attr,))
                value = cur.fetchone()[0]
                # Update config with the new attribute-value pair
                setattr(self.config, attr, value)
        logger.debug(
            f"Updated config using db. Now, maxsize={self.config.maxsize}, path={self.config.path}, hpss={self.config.hpss}, hpss_type={self.hpss_type}"
        )

    def get_db_name(self) -> str:
        return os.path.join(self.cache_dir, "index.db")

    def list_cache_dir(self):
        logger.info(
            f"Contents of cache {self.cache_dir} = {os.listdir(self.cache_dir)}"
        )

    def list_hpss_path(self):
        if self.hpss_type == HPSSType.SAME_MACHINE_HPSS:
            command = "hsi ls -l {}".format(self.config.hpss)
            error_str = f"Attempted to list contents at config.hpss={self.config.hpss}"
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
