from __future__ import absolute_import, print_function

import os
import shlex
import sqlite3
import subprocess
import time
from datetime import datetime, timezone
from fnmatch import fnmatch
from typing import Any, List, Tuple

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


def get_files_to_archive(cache: str, include: str, exclude: str) -> List[str]:
    # PERFORMANCE: Start timing file gathering
    gather_total_start = time.time()
    logger.info("-" * 80)
    logger.info("PERFORMANCE (get_files_to_archive): Starting file discovery")

    # List of files
    logger.info("Gathering list of files to archive")

    # PERFORMANCE: Time the os.walk operation
    walk_start = time.time()
    # Tuples of the form (path, filename)
    file_tuples: List[Tuple[str, str]] = []
    dir_count = 0
    file_count = 0
    empty_dir_count = 0

    # Walk the current directory
    for root, dirnames, filenames in os.walk("."):
        dir_count += 1

        if not dirnames and not filenames:
            # There are no subdirectories nor are there files.
            # This directory is empty.
            file_tuples.append((root, ""))
            empty_dir_count += 1
        for filename in filenames:
            # Loop over files
            # filenames is a list, so if it is empty, no looping will occur.
            file_tuples.append((root, filename))
            file_count += 1

        # Progress logging every 1000 directories
        if dir_count % 1000 == 0:
            elapsed = time.time() - walk_start
            rate = dir_count / elapsed if elapsed > 0 else 0
            logger.info(
                f"PERFORMANCE (walk): Scanned {dir_count} directories, "
                f"{file_count} files ({rate:.1f} dirs/sec, {elapsed:.1f}s elapsed)"
            )

    walk_elapsed = time.time() - walk_start
    logger.info("PERFORMANCE (walk): Completed filesystem walk")
    logger.info(f"  - Directories scanned: {dir_count}")
    logger.info(f"  - Files found: {file_count}")
    logger.info(f"  - Empty directories: {empty_dir_count}")
    logger.info(f"  - Time: {walk_elapsed:.2f} seconds")
    logger.info(
        f"  - Rate: {dir_count / walk_elapsed:.1f} dirs/sec, {file_count / walk_elapsed:.1f} files/sec"
    )

    # PERFORMANCE: Time the sorting operation
    sort_start = time.time()
    # Sort first on directories (x[0])
    # Further sort on filenames (x[1])
    file_tuples = sorted(file_tuples, key=lambda x: (x[0], x[1]))
    sort_elapsed = time.time() - sort_start
    logger.info(
        f"PERFORMANCE (sort): Sorted {len(file_tuples)} entries: {sort_elapsed:.2f} seconds"
    )

    # PERFORMANCE: Time the path normalization
    normalize_start = time.time()
    # Relative file paths, excluding the cache
    cache_path = os.path.join(".", cache)
    files: List[str] = []
    cache_excluded_count = 0

    for x in file_tuples:
        if x[0] != cache_path:
            files.append(os.path.normpath(os.path.join(x[0], x[1])))
        else:
            cache_excluded_count += 1

    normalize_elapsed = time.time() - normalize_start
    logger.info(
        f"PERFORMANCE (normalize): Normalized paths: {normalize_elapsed:.2f} seconds"
    )
    logger.info(f"  - Files after cache exclusion: {len(files)}")
    logger.info(f"  - Cache entries excluded: {cache_excluded_count}")

    initial_file_count = len(files)

    # PERFORMANCE: Time include filtering
    include_elapsed = 0.0
    if include is not None:
        include_start = time.time()
        files = include_files(include, files)
        include_elapsed = time.time() - include_start
        logger.info(
            f"PERFORMANCE (include filter): Applied include pattern '{include}': {include_elapsed:.2f} seconds"
        )
        logger.info(
            f"  - Files after include: {len(files)} (filtered out {initial_file_count - len(files)})"
        )
        initial_file_count = len(files)

    # PERFORMANCE: Time exclude filtering
    exclude_elapsed = 0.0
    if exclude is not None:
        exclude_start = time.time()
        files = exclude_files(exclude, files)
        exclude_elapsed = time.time() - exclude_start
        logger.info(
            f"PERFORMANCE (exclude filter): Applied exclude pattern '{exclude}': {exclude_elapsed:.2f} seconds"
        )
        logger.info(
            f"  - Files after exclude: {len(files)} (filtered out {initial_file_count - len(files)})"
        )

    gather_total_elapsed = time.time() - gather_total_start
    logger.info("-" * 80)
    logger.info(
        f"PERFORMANCE (get_files_to_archive): TOTAL TIME: {gather_total_elapsed:.2f} seconds"
    )
    logger.info(f"PERFORMANCE (get_files_to_archive): Final file count: {len(files)}")

    # Breakdown percentages
    if gather_total_elapsed > 0:
        logger.info("PERFORMANCE (get_files_to_archive): Time breakdown:")
        logger.info(
            f"  - Filesystem walk: {walk_elapsed:.2f}s ({walk_elapsed / gather_total_elapsed * 100:.1f}%)"
        )
        logger.info(
            f"  - Sorting: {sort_elapsed:.2f}s ({sort_elapsed / gather_total_elapsed * 100:.1f}%)"
        )
        logger.info(
            f"  - Path normalization: {normalize_elapsed:.2f}s ({normalize_elapsed / gather_total_elapsed * 100:.1f}%)"
        )
        if include is not None:
            logger.info(
                f"  - Include filtering: {include_elapsed:.2f}s ({include_elapsed / gather_total_elapsed * 100:.1f}%)"
            )
        if exclude is not None:
            logger.info(
                f"  - Exclude filtering: {exclude_elapsed:.2f}s ({exclude_elapsed / gather_total_elapsed * 100:.1f}%)"
            )
    logger.info("-" * 80)

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
