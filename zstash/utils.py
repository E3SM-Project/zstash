from __future__ import absolute_import, print_function

import os
import shlex
import sqlite3
import stat as stat_module
import subprocess
import time
from collections import OrderedDict
from datetime import datetime, timezone
from fnmatch import fnmatch
from typing import Any, Dict, List, Tuple

from .settings import TupleTarsRow, config, logger


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


# C901 'get_files_to_archive_with_stats' is too complex (19)
def get_files_to_archive_with_stats(  # noqa: C901
    cache: str, include: str, exclude: str
) -> Dict[str, Tuple[int, datetime]]:
    """
    OPTIMIZED VERSION: Gather list of files to archive along with their stats.

    Uses os.scandir() to get file stats during the directory walk,
    eliminating the need to stat files again later during database comparison.

    Returns:
        Dictionary mapping file_path -> (size, mtime)
    """
    # PERFORMANCE: Start timing file gathering
    gather_total_start = time.time()
    logger.debug("-" * 80)
    logger.debug(
        "PERFORMANCE (get_files_to_archive_with_stats): Starting file discovery with stats"
    )

    # List of files with their stats
    logger.debug("Gathering list of files to archive (with stats)")

    # PERFORMANCE: Time the os.scandir operation
    walk_start = time.time()
    # Dictionary mapping path -> (size, mtime)
    file_stats: Dict[str, Tuple[int, datetime]] = {}
    dir_count = 0
    file_count = 0
    empty_dir_count = 0
    cache_path = os.path.join(".", cache)

    def scan_directory(path: str):
        """Recursively scan directory using os.scandir() for efficiency."""
        nonlocal dir_count, file_count, empty_dir_count

        try:
            entries = list(os.scandir(path))
        except PermissionError:
            logger.warning(f"Permission denied: {path}")
            return

        dir_count += 1
        has_contents = False

        for entry in entries:
            # Skip the cache directory entirely
            if entry.path == cache_path or entry.path.startswith(cache_path + os.sep):
                continue

            try:
                # Get stat info - scandir provides this efficiently
                # Use entry.stat(follow_symlinks=False) to match os.lstat() behavior
                stat_info = entry.stat(follow_symlinks=False)
                mode = stat_info.st_mode

                if entry.is_dir(follow_symlinks=False):
                    # Recursively scan subdirectory
                    scan_directory(entry.path)
                    has_contents = True
                else:
                    # It's a file or symlink
                    has_contents = True
                    file_count += 1

                    # For symbolic links or directories, size should be 0
                    if stat_module.S_ISLNK(mode):
                        size = 0
                    else:
                        size = stat_info.st_size

                    mtime = datetime.utcfromtimestamp(stat_info.st_mtime)

                    # Normalize the path
                    normalized_path = os.path.normpath(entry.path)
                    file_stats[normalized_path] = (size, mtime)

            except (OSError, PermissionError) as e:
                logger.warning(f"Error accessing {entry.path}: {e}")
                continue

        # Handle empty directories
        if not has_contents and path != ".":
            empty_dir_count += 1
            normalized_path = os.path.normpath(path)
            # Get actual mtime for empty directory
            try:
                stat_info = os.lstat(path)
                mtime = datetime.utcfromtimestamp(stat_info.st_mtime)
                file_stats[normalized_path] = (0, mtime)
            except (OSError, PermissionError):
                # Fallback if we can't stat the directory
                file_stats[normalized_path] = (0, datetime.utcnow())

        # Progress logging every 1000 directories
        if dir_count % 1000 == 0:
            elapsed = time.time() - walk_start
            rate = dir_count / elapsed if elapsed > 0 else 0
            logger.debug(
                f"PERFORMANCE (scandir): Scanned {dir_count} directories, "
                f"{file_count} files ({rate:.1f} dirs/sec, {elapsed:.1f}s elapsed)"
            )

    # Start scanning from current directory
    scan_directory(".")

    walk_elapsed = time.time() - walk_start
    logger.debug("PERFORMANCE (scandir): Completed filesystem walk with stats")
    logger.debug(f"  - Directories scanned: {dir_count}")
    logger.debug(f"  - Files found: {file_count}")
    logger.debug(f"  - Empty directories: {empty_dir_count}")
    logger.debug(f"  - Time: {walk_elapsed:.2f} seconds")
    logger.debug(
        f"  - Rate: {dir_count / walk_elapsed:.1f} dirs/sec, {file_count / walk_elapsed:.1f} files/sec"
    )

    initial_file_count = len(file_stats)

    # Apply include/exclude filters
    # PERFORMANCE: Time include filtering
    include_elapsed = 0.0
    if include is not None:
        include_start = time.time()
        file_list = list(file_stats.keys())
        filtered_list = include_files(include, file_list)
        # Keep only files that passed the filter
        file_stats = {path: file_stats[path] for path in filtered_list}
        include_elapsed = time.time() - include_start
        logger.debug(
            f"PERFORMANCE (include filter): Applied include pattern '{include}': {include_elapsed:.2f} seconds"
        )
        logger.debug(
            f"  - Files after include: {len(file_stats)} (filtered out {initial_file_count - len(file_stats)})"
        )
        initial_file_count = len(file_stats)

    # PERFORMANCE: Time exclude filtering
    exclude_elapsed = 0.0
    if exclude is not None:
        exclude_start = time.time()
        file_list = list(file_stats.keys())
        filtered_list = exclude_files(exclude, file_list)
        # Keep only files that passed the filter
        file_stats = {path: file_stats[path] for path in filtered_list}
        exclude_elapsed = time.time() - exclude_start
        logger.debug(
            f"PERFORMANCE (exclude filter): Applied exclude pattern '{exclude}': {exclude_elapsed:.2f} seconds"
        )
        logger.debug(
            f"  - Files after exclude: {len(file_stats)} (filtered out {initial_file_count - len(file_stats)})"
        )

    # PERFORMANCE: Time the sorting operation to maintain deterministic order
    sort_start = time.time()
    # Sort paths to match original behavior (directory first, then filename)
    # Use an OrderedDict to preserve the sorted order
    sorted_paths = sorted(file_stats.keys())
    file_stats = OrderedDict((path, file_stats[path]) for path in sorted_paths)
    sort_elapsed = time.time() - sort_start
    logger.debug(
        f"PERFORMANCE (sort): Sorted {len(file_stats)} entries: {sort_elapsed:.2f} seconds"
    )

    gather_total_elapsed = time.time() - gather_total_start
    logger.debug("-" * 80)
    logger.debug(
        f"PERFORMANCE (get_files_to_archive_with_stats): TOTAL TIME: {gather_total_elapsed:.2f} seconds"
    )
    logger.debug(
        f"PERFORMANCE (get_files_to_archive_with_stats): Final file count: {len(file_stats)}"
    )

    # Breakdown percentages
    if gather_total_elapsed > 0:
        logger.debug("PERFORMANCE (get_files_to_archive_with_stats): Time breakdown:")
        logger.debug(
            f"  - Filesystem walk with stats: {walk_elapsed:.2f}s ({walk_elapsed / gather_total_elapsed * 100:.1f}%)"
        )
        if include is not None:
            logger.debug(
                f"  - Include filtering: {include_elapsed:.2f}s ({include_elapsed / gather_total_elapsed * 100:.1f}%)"
            )
        if exclude is not None:
            logger.debug(
                f"  - Exclude filtering: {exclude_elapsed:.2f}s ({exclude_elapsed / gather_total_elapsed * 100:.1f}%)"
            )
        logger.debug(
            f"  - Sorting: {sort_elapsed:.2f}s ({sort_elapsed / gather_total_elapsed * 100:.1f}%)"
        )
    logger.debug("-" * 80)

    return file_stats


def get_files_to_archive(cache: str, include: str, exclude: str) -> List[str]:
    """
    LEGACY VERSION: For backward compatibility.
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
