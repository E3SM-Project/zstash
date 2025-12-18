from __future__ import absolute_import, print_function

import argparse
import logging
import os.path
import sqlite3
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from .globus import globus_activate, globus_finalize
from .hpss import hpss_get, hpss_put
from .hpss_utils import add_files
from .settings import DEFAULT_CACHE, TIME_TOL, config, get_db_filename, logger
from .utils import get_files_to_archive_with_stats, update_config


# Classes #####################################################################
class UpdatePerformanceLogger:
    """
    Performance logger for tracking and reporting timing metrics
    during the update_database operation.
    """

    def __init__(self):
        self.overall_start: float = 0
        self.db_start: float = 0
        self.db_elapsed: float = 0
        self.gather_start: float = 0
        self.gather_elapsed: float = 0
        self.check_start: float = 0
        self.check_elapsed: float = 0
        self.db_load_start: float = 0
        self.db_load_elapsed: float = 0
        self.comparison_start: float = 0
        self.comparison_elapsed: float = 0
        self.tar_prep_start: float = 0
        self.tar_prep_elapsed: float = 0
        self.add_files_start: float = 0
        self.add_files_elapsed: float = 0

    def start_overall(self):
        """Start timing the overall operation."""
        self.overall_start = time.time()
        logger.debug("=" * 80)
        logger.debug("PERFORMANCE PROFILING: Starting update_database")
        logger.debug("=" * 80)

    def start_database_open(self):
        """Start timing database opening."""
        self.db_start = time.time()
        logger.debug("Opening index database")

    def end_database_open(self):
        """End timing database opening and config update."""
        self.db_elapsed = time.time() - self.db_start
        logger.debug(
            f"PERFORMANCE: Database open and config update: {self.db_elapsed:.2f} seconds"
        )

    def start_file_gathering(self):
        """Start timing file gathering operation."""
        self.gather_start = time.time()
        logger.debug("PERFORMANCE: Starting file gathering with stats (OPTIMIZED)...")

    def end_file_gathering(self, file_count: int):
        """End timing file gathering operation."""
        self.gather_elapsed = time.time() - self.gather_start
        logger.debug(
            f"PERFORMANCE: File gathering completed: {self.gather_elapsed:.2f} seconds"
        )
        logger.debug(f"PERFORMANCE: Total files found: {file_count}")

    def start_database_check(self):
        """Start timing database comparison."""
        self.check_start = time.time()
        logger.debug(
            "PERFORMANCE: Starting database comparison (OPTIMIZED - NO STATS)..."
        )

    def start_database_load(self):
        """Start timing database loading into memory."""
        self.db_load_start = time.time()
        logger.debug("PERFORMANCE: Loading database into memory...")

    def end_database_load(self, archived_count: int):
        """End timing database loading."""
        self.db_load_elapsed = time.time() - self.db_load_start
        logger.debug(
            f"PERFORMANCE: Database loaded: {self.db_load_elapsed:.2f} seconds"
        )
        logger.debug(f"PERFORMANCE: Archived files in database: {archived_count}")

    def start_comparison(self):
        """Start timing the comparison operation."""
        self.comparison_start = time.time()

    def log_comparison_progress(
        self, files_checked: int, total_files: int, interval: int = 1000
    ):
        """Log comparison progress at regular intervals."""
        if files_checked % interval == 0:
            elapsed_so_far = time.time() - self.comparison_start
            rate = files_checked / elapsed_so_far if elapsed_so_far > 0 else 0
            logger.debug(
                f"PERFORMANCE: Compared {files_checked}/{total_files} files "
                f"({rate:.1f} files/sec, {elapsed_so_far:.1f}s elapsed)"
            )

    def end_database_check(self, files_checked: int, new_files_count: int):
        """End timing database comparison and log detailed metrics."""
        self.comparison_elapsed = time.time() - self.comparison_start
        self.check_elapsed = time.time() - self.check_start

        logger.debug("=" * 80)
        logger.debug("PERFORMANCE: Database comparison completed (OPTIMIZED)")
        logger.debug(
            f"PERFORMANCE: Total comparison time: {self.check_elapsed:.2f} seconds"
        )
        logger.debug(f"PERFORMANCE: Files checked: {files_checked}")
        logger.debug(f"PERFORMANCE: New files to archive: {new_files_count}")
        logger.debug(
            f"PERFORMANCE: Average rate: {files_checked / self.check_elapsed:.1f} files/sec"
        )
        logger.debug("-" * 80)
        logger.debug("PERFORMANCE: Time breakdown:")
        logger.debug(
            f"  - database load: {self.db_load_elapsed:.2f}s "
            f"({self.db_load_elapsed / self.check_elapsed * 100:.1f}%)"
        )
        logger.debug(
            f"  - comparison (in-memory): {self.comparison_elapsed:.2f}s "
            f"({self.comparison_elapsed / self.check_elapsed * 100:.1f}%)"
        )
        logger.debug("-" * 80)
        logger.debug("PERFORMANCE: Optimization impact:")
        logger.debug(f"  - stat operations eliminated: {files_checked} (100%)")
        logger.debug("  - All stats performed during initial filesystem walk")
        logger.debug("=" * 80)

    def start_tar_preparation(self):
        """Start timing tar archive preparation."""
        self.tar_prep_start = time.time()
        logger.debug("PERFORMANCE: Finding last used tar archive...")

    def end_tar_preparation(self):
        """End timing tar archive preparation."""
        self.tar_prep_elapsed = time.time() - self.tar_prep_start
        logger.debug(
            f"PERFORMANCE: Tar archive preparation: {self.tar_prep_elapsed:.2f} seconds"
        )

    def start_add_files(self):
        """Start timing add_files operation."""
        self.add_files_start = time.time()
        logger.debug("PERFORMANCE: Starting add_files operation...")

    def end_add_files(self):
        """End timing add_files operation."""
        self.add_files_elapsed = time.time() - self.add_files_start
        logger.debug(
            f"PERFORMANCE: add_files operation completed: {self.add_files_elapsed:.2f} seconds"
        )

    def log_overall_summary(self):
        """Log the complete performance summary."""
        overall_elapsed = time.time() - self.overall_start

        logger.debug("=" * 80)
        logger.debug("PERFORMANCE: Update complete - Summary:")
        logger.debug(
            f"  - Database open/config: {self.db_elapsed:.2f}s "
            f"({self.db_elapsed / overall_elapsed * 100:.1f}%)"
        )
        logger.debug(
            f"  - File gathering: {self.gather_elapsed:.2f}s "
            f"({self.gather_elapsed / overall_elapsed * 100:.1f}%)"
        )
        logger.debug(
            f"  - Database comparison: {self.check_elapsed:.2f}s "
            f"({self.check_elapsed / overall_elapsed * 100:.1f}%)"
        )
        logger.debug(
            f"  - Tar preparation: {self.tar_prep_elapsed:.2f}s "
            f"({self.tar_prep_elapsed / overall_elapsed * 100:.1f}%)"
        )
        logger.debug(
            f"  - Add files: {self.add_files_elapsed:.2f}s "
            f"({self.add_files_elapsed / overall_elapsed * 100:.1f}%)"
        )
        logger.debug(f"  - TOTAL TIME: {overall_elapsed:.2f} seconds")
        logger.debug("=" * 80)

    def log_early_exit(self, reason: str = ""):
        """Log performance for early exits (no updates, dry-run)."""
        overall_elapsed = time.time() - self.overall_start
        suffix = f" ({reason})" if reason else ""
        logger.debug(
            f"PERFORMANCE: Total execution time{suffix}: {overall_elapsed:.2f} seconds"
        )


# Functions #####################################################################
def update():

    args: argparse.Namespace
    cache: str
    args, cache = setup_update()

    result: Optional[List[str]] = update_database(args, cache)

    if result is None:
        # There was either nothing to update or `--dry-run` was set.
        return
    else:
        failures = result

    # Transfer to HPSS. Always keep a local copy of the database.
    if config.hpss is not None:
        hpss = config.hpss
    else:
        raise TypeError("Invalid config.hpss={}".format(config.hpss))
    hpss_put(hpss, get_db_filename(cache), cache, keep=args.keep, is_index=True)

    globus_finalize(non_blocking=args.non_blocking)

    # List failures
    if len(failures) > 0:
        logger.warning("Some files could not be archived")
        for file_path in failures:
            logger.error("Archiving {}".format(file_path))


def setup_update() -> Tuple[argparse.Namespace, str]:
    # Parser
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        usage="zstash update [<args>]", description="Update an existing zstash archive"
    )
    parser.add_argument_group("required named arguments")
    optional: argparse._ArgumentGroup = parser.add_argument_group(
        "optional named arguments"
    )
    optional.add_argument(
        "--hpss",
        type=str,
        help=(
            'path to storage on HPSS. Set to "none" for local archiving. It also can be a Globus URL, '
            'globus://<GLOBUS_ENDPOINT_UUID>/<PATH>. Names "alcf" and "nersc" are recognized as referring to the ALCF HPSS '
            "and NERSC HPSS endpoints, e.g. globus://nersc/~/my_archive."
        ),
    )
    optional.add_argument(
        "--include", type=str, help="comma separated list of file patterns to include"
    )
    optional.add_argument(
        "--exclude", type=str, help="comma separated list of file patterns to exclude"
    )
    optional.add_argument(
        "--dry-run",
        help="dry run, only list files to be updated in archive",
        action="store_true",
    )
    optional.add_argument(
        "--maxsize",
        type=float,
        help="maximum size of tar archives (in GB, default 256)",
        default=256,
    )

    optional.add_argument(
        "--keep",
        help='if --hpss is not "none", keep the tar files in the local archive (cache) after uploading to the HPSS archive. Default is to delete the tar files. If --hpss=none, this flag has no effect.',
        action="store_true",
    )
    optional.add_argument(
        "--cache",
        type=str,
        help='path to the zstash archive on the local file system. The default name is "zstash".',
    )
    optional.add_argument(
        "--non-blocking",
        action="store_true",
        help="do not wait for each Globus transfer until it completes.",
    )
    optional.add_argument(
        "--error-on-duplicate-tar",
        action="store_true",
        help="FOR ADVANCED USERS ONLY: Raise an error if a tar file with the same name already exists in the database. If this flag is set, zstash will exit if it sees a duplicate tar. If it is not set, zstash's behavior will depend on whether or not the --overwrite-duplicate-tar flag is set.",
    )
    optional.add_argument(
        "--overwrite-duplicate-tars",
        action="store_true",
        help="FOR ADVANCED USERS ONLY: If a duplicate tar is encountered, overwrite the existing database record with the new one (i.e., it will assume the latest tar is the correct one). If this flag is not set, zstash will permit multiple entries for the same tar in its database.",
    )
    optional.add_argument(
        "-v", "--verbose", action="store_true", help="increase output verbosity"
    )
    optional.add_argument(
        "--follow-symlinks",
        action="store_true",
        help="Hard copy symlinks. This is useful for preventing broken links. Note that a broken link will result in a failed update.",
    )
    args: argparse.Namespace = parser.parse_args(sys.argv[2:])

    if (not args.hpss) or (args.hpss.lower() == "none"):
        args.hpss = "none"
        args.keep = True

    # Copy configuration
    # config.path = os.path.abspath(args.path)
    config.hpss = args.hpss
    config.maxsize = int(1024 * 1024 * 1024 * args.maxsize)

    cache: str
    if args.cache:
        cache = args.cache
    else:
        cache = DEFAULT_CACHE
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    return args, cache


# C901 'update_database' is too complex (20)
def update_database(  # noqa: C901
    args: argparse.Namespace, cache: str
) -> Optional[List[str]]:

    # Initialize performance logger
    perf = UpdatePerformanceLogger()
    perf.start_overall()

    # Open database
    perf.start_database_open()
    if not os.path.exists(get_db_filename(cache)):
        # The database file doesn't exist in the cache.
        # We need to retrieve it from HPSS
        if args.hpss is not None:
            config.hpss = args.hpss
            if config.hpss is not None:
                hpss: str = config.hpss
            else:
                raise TypeError("Invalid config.hpss={}".format(config.hpss))
            globus_activate(hpss)
            hpss_get(hpss, get_db_filename(cache), cache)
        else:
            error_str: str = (
                "--hpss argument is required when local copy of database is unavailable"
            )
            logger.error(error_str)
            raise ValueError(error_str)

    con: sqlite3.Connection = sqlite3.connect(
        get_db_filename(cache), detect_types=sqlite3.PARSE_DECLTYPES
    )
    cur: sqlite3.Cursor = con.cursor()

    update_config(cur)
    perf.end_database_open()

    if config.maxsize is not None:
        maxsize = config.maxsize
    else:
        raise TypeError("Invalid config.maxsize={}".format(config.maxsize))
    config.maxsize = int(maxsize)

    keep: bool
    # The command line arg should always have precedence
    if args.hpss == "none":
        # If no HPSS is available, always keep the files.
        keep = True
    else:
        # If HPSS is used, let the user specify whether or not to keep the files.
        keep = args.keep

    if args.hpss is not None:
        config.hpss = args.hpss

    # Start doing actual work
    logger.debug("Running zstash update")
    logger.debug("Local path : {}".format(config.path))
    logger.debug("HPSS path  : {}".format(config.hpss))
    logger.debug("Max size  : {}".format(maxsize))
    logger.debug("Keep local tar files  : {}".format(keep))

    # Gather files to archive
    perf.start_file_gathering()
    file_stats: Dict[str, Tuple[int, datetime]] = get_files_to_archive_with_stats(
        cache, args.include, args.exclude
    )
    files: List[str] = list(file_stats.keys())
    perf.end_file_gathering(len(files))

    # Database checking - OPTIMIZED VERSION
    perf.start_database_check()

    # Load all archived files into memory once
    perf.start_database_load()

    # Dictionary mapping file path -> (size, mtime) for O(1) lookup
    archived_files: Dict[str, Tuple[int, datetime]] = {}

    cur.execute("SELECT name, size, mtime FROM files")
    db_rows = cur.fetchall()

    for row in db_rows:
        file_path: str = row[0]
        size: int = row[1]
        mtime: datetime = row[2]

        # If file appears multiple times, keep the one with latest mtime
        if file_path in archived_files:
            existing_mtime = archived_files[file_path][1]
            if mtime > existing_mtime:
                archived_files[file_path] = (size, mtime)
        else:
            archived_files[file_path] = (size, mtime)

    perf.end_database_load(len(archived_files))

    # Compare using pre-collected stats - NO os.lstat() calls!
    perf.start_comparison()
    newfiles: List[str] = []
    files_checked = 0

    for file_path in files:
        # Get the stat info we already collected during filesystem walk
        size_new, mdtime_new = file_stats[file_path]

        # Check if file exists in database
        if file_path not in archived_files:
            # File not in database - it's new
            newfiles.append(file_path)
        else:
            # File exists in database - check if it changed
            archived_size, archived_mtime = archived_files[file_path]

            if not (
                (size_new == archived_size)
                and (abs((mdtime_new - archived_mtime).total_seconds()) <= TIME_TOL)
            ):
                # File has changed
                newfiles.append(file_path)

        files_checked += 1

        # Progress logging every 1000 files
        perf.log_comparison_progress(files_checked, len(files))

    perf.end_database_check(files_checked, len(newfiles))

    # Anything to do?
    if len(newfiles) == 0:
        logger.info("Nothing to update")
        # Close database
        con.commit()
        con.close()

        perf.log_early_exit()
        return None

    # --dry-run option
    if args.dry_run:
        print("List of files to be updated")
        for file_path in newfiles:
            print(file_path)
        # Close database
        con.commit()
        con.close()

        perf.log_early_exit("dry-run")
        return None

    # Find last used tar archive
    perf.start_tar_preparation()
    itar: int = -1
    cur.execute("select distinct tar from files")
    tfiles: List[Tuple[str]] = cur.fetchall()
    for tfile in tfiles:
        tfile_string: str = tfile[0]
        itar = max(itar, int(tfile_string[0:6], 16))

    perf.end_tar_preparation()

    # Add files
    perf.start_add_files()

    failures: List[str]
    if args.follow_symlinks:
        try:
            # Add files
            failures = add_files(
                cur,
                con,
                itar,
                newfiles,
                cache,
                keep,
                args.follow_symlinks,
                non_blocking=args.non_blocking,
                error_on_duplicate_tar=args.error_on_duplicate_tar,
                overwrite_duplicate_tars=args.overwrite_duplicate_tars,
            )
        except FileNotFoundError:
            raise Exception("Archive update failed due to broken symlink.")
    else:
        # Add files
        failures = add_files(
            cur,
            con,
            itar,
            newfiles,
            cache,
            keep,
            args.follow_symlinks,
            non_blocking=args.non_blocking,
            error_on_duplicate_tar=args.error_on_duplicate_tar,
            overwrite_duplicate_tars=args.overwrite_duplicate_tars,
        )

    perf.end_add_files()

    # Close database
    con.commit()
    con.close()

    perf.log_overall_summary()

    return failures
