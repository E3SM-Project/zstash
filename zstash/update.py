from __future__ import absolute_import, print_function

import argparse
import logging
import os.path
import sqlite3
import stat
import sys
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from six.moves.urllib.parse import urlparse

from .globus import globus_activate, globus_finalize
from .hpss import hpss_get, hpss_put
from .hpss_utils import add_files
from .settings import (
    DEFAULT_CACHE,
    TIME_TOL,
    FilesRow,
    TupleFilesRow,
    config,
    get_db_filename,
    logger,
)
from .utils import get_files_to_archive, update_config


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
        "--modified-since",
        type=str,
        help=(
            "only consider files modified after this timestamp (ISO format: YYYY-MM-DDTHH:MM:SS). "
            "Use this to significantly speed up updates by skipping unchanged files. "
            "Example: --modified-since=2025-12-08T14:00:00"
        ),
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
    # Open database
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

    # Check Globus authentication early to fail fast before file scanning
    if config.hpss is not None and config.hpss != "none":
        url = urlparse(config.hpss)
        if url.scheme == "globus":
            logger.info("Checking Globus authentication before file scanning...")
            globus_activate(config.hpss)

    # Start doing actual work
    logger.debug("Running zstash update")
    logger.debug("Local path : {}".format(config.path))
    logger.debug("HPSS path  : {}".format(config.hpss))
    logger.debug("Max size  : {}".format(maxsize))
    logger.debug("Keep local tar files  : {}".format(keep))

    # Parse --modified-since if provided
    modified_since_dt: Optional[datetime] = None
    if args.modified_since:
        try:
            modified_since_dt = datetime.fromisoformat(args.modified_since)
            # If the parsed datetime is naive, make it timezone-aware (assume UTC)
            if modified_since_dt.tzinfo is None:
                modified_since_dt = modified_since_dt.replace(tzinfo=timezone.utc)
            logger.info(
                "Filtering files: only considering files modified after {}".format(
                    modified_since_dt
                )
            )
        except ValueError as e:
            error_str = (
                "Invalid --modified-since format. Expected ISO format (YYYY-MM-DDTHH:MM:SS): {}"
            ).format(e)
            logger.error(error_str)
            raise ValueError(error_str)

    files: List[str] = get_files_to_archive(cache, args.include, args.exclude)

    statinfo: os.stat_result
    # Pre-filter by modification time if --modified-since was provided
    if modified_since_dt is not None:
        files_before_filter = len(files)
        filtered_files: List[str] = []

        for file_path in files:
            try:
                statinfo = os.lstat(file_path)
                # Use timezone-aware datetime for comparison
                file_mtime: datetime = datetime.fromtimestamp(
                    statinfo.st_mtime, tz=timezone.utc
                )

                if file_mtime > modified_since_dt:
                    filtered_files.append(file_path)
            except (OSError, IOError) as e:
                # If we can't stat the file, include it to be safe
                logger.warning(
                    "Could not stat {}, including in scan: {}".format(file_path, e)
                )
                filtered_files.append(file_path)

        files = filtered_files
        skipped_count = files_before_filter - len(files)
        logger.info(
            "Pre-filtered {} files by modification time (skipped {} unchanged files)".format(
                len(files), skipped_count
            )
        )

    # Eliminate files that are already archived and up to date
    newfiles: List[str] = []
    for file_path in files:
        statinfo = os.lstat(file_path)
        mdtime_new: datetime = datetime.utcfromtimestamp(statinfo.st_mtime)
        mode: int = statinfo.st_mode
        # For symbolic links or directories, size should be 0
        size_new: int
        if stat.S_ISLNK(mode) or stat.S_ISDIR(mode):
            size_new = 0
        else:
            size_new = statinfo.st_size

        # Select the file matching the path.
        cur.execute("select * from files where name = ?", (file_path,))
        new: bool = True
        while True:
            # Get the corresponding row in the 'files' table
            match_: Optional[TupleFilesRow] = cur.fetchone()
            if match_ is None:
                break
            else:
                match: FilesRow = FilesRow(match_)

            if (size_new == match.size) and (
                abs((mdtime_new - match.mtime).total_seconds()) <= TIME_TOL
            ):
                # File exists with same size and modification time within tolerance
                new = False
                break
        if new:
            newfiles.append(file_path)

    # Anything to do?
    if len(newfiles) == 0:
        logger.info("Nothing to update")
        # Close database
        con.commit()
        con.close()
        return None

    # --dry-run option
    if args.dry_run:
        print("List of files to be updated")
        for file_path in newfiles:
            print(file_path)
        # Close database
        con.commit()
        con.close()
        return None

    # Find last used tar archive
    itar: int = -1
    cur.execute("select distinct tar from files")
    tfiles: List[Tuple[str]] = cur.fetchall()
    for tfile in tfiles:
        tfile_string: str = tfile[0]
        itar = max(itar, int(tfile_string[0:6], 16))

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

    # Close database
    con.commit()
    con.close()

    return failures
