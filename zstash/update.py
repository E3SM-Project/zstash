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

    # PERFORMANCE: Start overall timing
    overall_start = time.time()
    logger.info("=" * 80)
    logger.info("PERFORMANCE PROFILING: Starting update_database")
    logger.info("=" * 80)

    # Open database
    db_start = time.time()
    logger.debug("Opening index database")
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
    db_elapsed = time.time() - db_start
    logger.info(
        f"PERFORMANCE: Database open and config update: {db_elapsed:.2f} seconds"
    )

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

    # PERFORMANCE: Time file gathering WITH STATS (OPTIMIZATION)
    gather_start = time.time()
    logger.info("PERFORMANCE: Starting file gathering with stats (OPTIMIZED)...")
    file_stats: Dict[str, Tuple[int, datetime]] = get_files_to_archive_with_stats(
        cache, args.include, args.exclude
    )
    files: List[str] = list(file_stats.keys())
    gather_elapsed = time.time() - gather_start
    logger.info(f"PERFORMANCE: File gathering completed: {gather_elapsed:.2f} seconds")
    logger.info(f"PERFORMANCE: Total files found: {len(files)}")

    # PERFORMANCE: Time database checking - OPTIMIZED VERSION
    check_start = time.time()
    logger.info("PERFORMANCE: Starting database comparison (OPTIMIZED - NO STATS)...")

    # OPTIMIZATION: Load all archived files into memory once
    db_load_start = time.time()
    logger.info("PERFORMANCE: Loading database into memory...")

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

    db_load_elapsed = time.time() - db_load_start
    logger.info(f"PERFORMANCE: Database loaded: {db_load_elapsed:.2f} seconds")
    logger.info(f"PERFORMANCE: Archived files in database: {len(archived_files)}")

    # OPTIMIZATION: Compare using pre-collected stats - NO os.lstat() calls!
    comparison_start = time.time()
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
        if files_checked % 1000 == 0:
            elapsed_so_far = time.time() - comparison_start
            rate = files_checked / elapsed_so_far if elapsed_so_far > 0 else 0
            logger.info(
                f"PERFORMANCE: Compared {files_checked}/{len(files)} files "
                f"({rate:.1f} files/sec, {elapsed_so_far:.1f}s elapsed)"
            )

    comparison_elapsed = time.time() - comparison_start
    check_elapsed = time.time() - check_start

    logger.info("=" * 80)
    logger.info("PERFORMANCE: Database comparison completed (OPTIMIZED)")
    logger.info(f"PERFORMANCE: Total comparison time: {check_elapsed:.2f} seconds")
    logger.info(f"PERFORMANCE: Files checked: {files_checked}")
    logger.info(f"PERFORMANCE: New files to archive: {len(newfiles)}")
    logger.info(
        f"PERFORMANCE: Average rate: {files_checked / check_elapsed:.1f} files/sec"
    )
    logger.info("-" * 80)
    logger.info("PERFORMANCE: Time breakdown:")
    logger.info(
        f"  - database load: {db_load_elapsed:.2f}s ({db_load_elapsed / check_elapsed * 100:.1f}%)"
    )
    logger.info(
        f"  - comparison (in-memory): {comparison_elapsed:.2f}s ({comparison_elapsed / check_elapsed * 100:.1f}%)"
    )
    logger.info("-" * 80)
    logger.info("PERFORMANCE: Optimization impact:")
    logger.info(f"  - stat operations eliminated: {files_checked} (100%)")
    logger.info("  - All stats performed during initial filesystem walk")
    logger.info("=" * 80)

    # Anything to do?
    if len(newfiles) == 0:
        logger.info("Nothing to update")
        # Close database
        con.commit()
        con.close()

        overall_elapsed = time.time() - overall_start
        logger.info(f"PERFORMANCE: Total execution time: {overall_elapsed:.2f} seconds")
        return None

    # --dry-run option
    if args.dry_run:
        print("List of files to be updated")
        for file_path in newfiles:
            print(file_path)
        # Close database
        con.commit()
        con.close()

        overall_elapsed = time.time() - overall_start
        logger.info(
            f"PERFORMANCE: Total execution time (dry-run): {overall_elapsed:.2f} seconds"
        )
        return None

    # PERFORMANCE: Time tar archive preparation
    tar_prep_start = time.time()
    logger.info("PERFORMANCE: Finding last used tar archive...")

    # Find last used tar archive
    itar: int = -1
    cur.execute("select distinct tar from files")
    tfiles: List[Tuple[str]] = cur.fetchall()
    for tfile in tfiles:
        tfile_string: str = tfile[0]
        itar = max(itar, int(tfile_string[0:6], 16))

    tar_prep_elapsed = time.time() - tar_prep_start
    logger.info(f"PERFORMANCE: Tar archive preparation: {tar_prep_elapsed:.2f} seconds")

    # PERFORMANCE: Time file addition
    add_files_start = time.time()
    logger.info("PERFORMANCE: Starting add_files operation...")

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

    add_files_elapsed = time.time() - add_files_start
    logger.info(
        f"PERFORMANCE: add_files operation completed: {add_files_elapsed:.2f} seconds"
    )

    # Close database
    con.commit()
    con.close()

    overall_elapsed = time.time() - overall_start
    logger.info("=" * 80)
    logger.info("PERFORMANCE: Update complete - Summary:")
    logger.info(
        f"  - Database open/config: {db_elapsed:.2f}s ({db_elapsed / overall_elapsed * 100:.1f}%)"
    )
    logger.info(
        f"  - File gathering: {gather_elapsed:.2f}s ({gather_elapsed / overall_elapsed * 100:.1f}%)"
    )
    logger.info(
        f"  - Database comparison: {check_elapsed:.2f}s ({check_elapsed / overall_elapsed * 100:.1f}%)"
    )
    logger.info(
        f"  - Tar preparation: {tar_prep_elapsed:.2f}s ({tar_prep_elapsed / overall_elapsed * 100:.1f}%)"
    )
    logger.info(
        f"  - Add files: {add_files_elapsed:.2f}s ({add_files_elapsed / overall_elapsed * 100:.1f}%)"
    )
    logger.info(f"  - TOTAL TIME: {overall_elapsed:.2f} seconds")
    logger.info("=" * 80)

    return failures
