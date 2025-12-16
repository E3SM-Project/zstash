from __future__ import absolute_import, print_function

import os
import shlex
import sqlite3
import stat as stat_module
import subprocess
from collections import OrderedDict
from datetime import datetime, timezone
from fnmatch import fnmatch
from typing import Any, Dict, List, Tuple

from .settings import TupleTarsRow, config, logger


# Classes #####################################################################
class DirectoryScanner:
    """Helper class to scan directories and collect file stats."""

    def __init__(self, cache_path: str):
        self.cache_path = cache_path
        self.file_stats: Dict[str, Tuple[int, datetime]] = {}
        self.dir_count = 0
        self.file_count = 0
        self.empty_dir_count = 0

    def scan_directory(self, path: str):
        """Recursively scan directory using os.scandir() for efficiency."""
        try:
            entries = list(os.scandir(path))
            # Sort entries to match os.walk's deterministic order.
            # This ensures consistent ordering across runs and filesystems.
            entries.sort(key=lambda e: e.name)
        except PermissionError:
            logger.warning(f"Permission denied: {path}")
            return

        self.dir_count += 1
        has_contents = False
        subdirs_to_process = []  # Defer subdirectory recursion

        for entry in entries:
            # Skip the cache directory entirely
            if entry.path == self.cache_path or entry.path.startswith(
                self.cache_path + os.sep
            ):
                continue

            try:
                # Get stat info - scandir provides this efficiently
                # Use entry.stat(follow_symlinks=False) to match os.lstat() behavior
                stat_info = entry.stat(follow_symlinks=False)
                mode = stat_info.st_mode

                if entry.is_dir(follow_symlinks=False):
                    # Don't recurse yet - just remember it
                    has_contents = True
                    subdirs_to_process.append(entry.path)
                else:
                    # It's a file or symlink
                    has_contents = True
                    self.file_count += 1

                    # For symbolic links or directories, size should be 0
                    if stat_module.S_ISLNK(mode):
                        size = 0
                    else:
                        size = stat_info.st_size

                    mtime = datetime.utcfromtimestamp(stat_info.st_mtime)

                    # Normalize the path.
                    # By building from path + entry.name,
                    # we guarantee the argument to normpath is constructed
                    # identically to how os.walk did it in previous code iterations.
                    normalized_path = os.path.normpath(os.path.join(path, entry.name))
                    self.file_stats[normalized_path] = (size, mtime)

            except (OSError, PermissionError) as e:
                logger.warning(f"Error accessing {entry.path}: {e}")
                continue

        # Handle empty directories BEFORE recursing into subdirs
        if not has_contents and path != ".":
            self.empty_dir_count += 1
            normalized_path = os.path.normpath(path)
            # Get actual mtime for empty directory
            try:
                stat_info = os.lstat(path)
                mtime = datetime.utcfromtimestamp(stat_info.st_mtime)
                self.file_stats[normalized_path] = (0, mtime)
            except (OSError, PermissionError):
                # Fallback if we can't stat the directory
                self.file_stats[normalized_path] = (0, datetime.utcnow())

        # Now recurse into subdirectories
        for subdir in subdirs_to_process:
            self.scan_directory(subdir)


# Functions #####################################################################


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


# Sort paths to match original behavior (directory first, then filename)
def path_sort_key(path: str) -> Tuple[str, str]:
    directory: str = os.path.dirname(path)
    filename: str = os.path.basename(path)
    return (directory, filename)


def get_files_to_archive_with_stats(
    cache: str, include: str, exclude: str
) -> Dict[str, Tuple[int, datetime]]:
    """
    OPTIMIZED VERSION: Gather list of files to archive along with their stats.

    Uses os.scandir() to get file stats during the directory walk,
    eliminating the need to stat files again later during database comparison.

    Returns:
        Dictionary mapping file_path -> (size, mtime)
    """

    logger.info("Gathering list of files to archive")
    cache_path = os.path.join(".", cache)
    scanner = DirectoryScanner(cache_path)
    scanner.scan_directory(".")
    file_stats = scanner.file_stats

    # Apply include/exclude filters
    if include is not None:
        file_list = list(file_stats.keys())
        filtered_list = include_files(include, file_list)
        # Keep only files that passed the filter
        file_stats = {path: file_stats[path] for path in filtered_list}
    if exclude is not None:
        file_list = list(file_stats.keys())
        filtered_list = exclude_files(exclude, file_list)
        # Keep only files that passed the filter
        file_stats = {path: file_stats[path] for path in filtered_list}

    # Extract directory and filename BEFORE normalization for sorting
    # For empty directories, preserve original behavior: use directory path with empty filename
    path_components = []
    for path in file_stats.keys():
        size, mtime = file_stats[path]
        # Empty directories (size 0) should use full path as directory, empty string as filename
        if size == 0 and not os.path.isfile(path):
            directory = path
            filename = ""
        else:
            directory = os.path.dirname(path)
            filename = os.path.basename(path)
        path_components.append((directory, filename, path))

    # Sort on directory and filename like original
    path_components.sort(key=lambda x: (x[0], x[1]))

    # NOW build the ordered dict with normalized paths
    file_stats = OrderedDict((x[2], file_stats[x[2]]) for x in path_components)

    return file_stats


def get_files_to_archive(cache: str, include: str, exclude: str) -> List[str]:
    """
    LEGACY VERSION: Still used for `zstash create`.
    Uses the optimized version but returns only the file list.
    """
    file_stats = get_files_to_archive_with_stats(cache, include, exclude)
    return list(file_stats.keys())


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
