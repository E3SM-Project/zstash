from __future__ import absolute_import, print_function

import argparse
import logging
import os.path
import sqlite3
import stat
import sys
from datetime import datetime
from typing import List, Optional, Tuple

from .globus import globus_activate, globus_finalize
from .hpss import hpss_get, hpss_put
from .hpss_utils import add_files
from .settings import TIME_TOL, FilesRow, TupleFilesRow, logger
from .utils import CommandInfo, HPSSType, get_files_to_archive


def update():
    command_info = CommandInfo("update")
    args: argparse.Namespace = setup_update(command_info)

    failures: Optional[List[str]] = update_database(command_info, args)
    if failures is None:
        # There was either nothing to update or `--dry-run` was set.
        return

    hpss_put(command_info, command_info.get_db_name())

    if command_info.globus_info:
        globus_finalize(command_info.globus_info, non_blocking=args.non_blocking)

    # List failures
    if len(failures) > 0:
        logger.warning("Some files could not be archived")
        for file_path in failures:
            logger.error(f"Archiving {file_path}")


def setup_update(command_info: CommandInfo) -> argparse.Namespace:
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
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    if args.cache:
        command_info.cache_dir = args.cache
    command_info.keep = args.keep
    command_info.set_dir_to_archive(os.getcwd())
    command_info.set_and_scale_maxsize(args.maxsize)
    command_info.set_hpss_parameters(args.hpss)

    return args


# C901 'update_database' is too complex (20)
def update_database(  # noqa: C901
    command_info: CommandInfo, args: argparse.Namespace
) -> Optional[List[str]]:
    # Open database
    logger.debug("Opening index database")
    if not os.path.exists(command_info.get_db_name()):
        # The database file doesn't exist in the cache.
        # We need to retrieve it from HPSS
        if command_info.hpss_type != HPSSType.NO_HPSS:
            if command_info.globus_info:
                globus_activate(command_info.globus_info)
            hpss_get(command_info, command_info.get_db_name())
        else:
            # NOTE: while --hpss is required in `create`, it is optional in `update`!
            # If --hpss is not provided, we assume it is 'none' => HPSSType.NO_HPSS
            error_str: str = (
                "--hpss argument (!= none) is required when local copy of database is unavailable"
            )
            logger.error(error_str)
            raise ValueError(error_str)

    con: sqlite3.Connection = sqlite3.connect(
        command_info.get_db_name(), detect_types=sqlite3.PARSE_DECLTYPES
    )
    cur: sqlite3.Cursor = con.cursor()

    command_info.update_config_using_db(cur)
    command_info.validate_maxsize()

    if command_info.hpss_type == HPSSType.NO_HPSS:
        # If not using HPSS, always keep the files.
        command_info.keep = True
    # else: keep command_info.keep set to args.keep

    # Start doing actual work
    logger.debug("Running zstash update")
    logger.debug(f"Local path : {command_info.config.path}")
    logger.debug(f"HPSS path  : {command_info.config.hpss}")
    logger.debug(f"Max size  : {command_info.config.maxsize}")
    logger.debug(f"Keep local tar files  : {command_info.keep}")

    files: List[str] = get_files_to_archive(
        command_info.cache_dir, args.include, args.exclude
    )

    # Eliminate files that are already archived and up to date
    newfiles: List[str] = []
    for file_path in files:
        # logger.debug(f"file_path={file_path}")
        statinfo: os.stat_result = os.lstat(file_path)
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
            # logger.debug(f"match_={match_}")
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
    # else:
    #     logger.debug(f"Number of files to update: {len(newfiles)}")
    #     for f in newfiles:
    #         logger.debug(f)

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
                command_info,
                cur,
                con,
                itar,
                newfiles,
                command_info.keep,
                args.follow_symlinks,
                non_blocking=args.non_blocking,
            )
        except FileNotFoundError:
            raise Exception("Archive update failed due to broken symlink.")
    else:
        # Add files
        failures = add_files(
            command_info,
            cur,
            con,
            itar,
            newfiles,
            args.follow_symlinks,
            non_blocking=args.non_blocking,
        )

    # Close database
    con.commit()
    con.close()

    return failures
