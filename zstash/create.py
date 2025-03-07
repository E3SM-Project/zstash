from __future__ import absolute_import, print_function

import argparse
import errno
import logging
import os.path
import sqlite3
import sys
from typing import Any, List

from six.moves.urllib.parse import urlparse

from .globus import globus_activate, globus_finalize
from .hpss import hpss_put
from .hpss_utils import add_files
from .settings import logger
from .utils import (
    CommandInfo,
    HPSSType,
    create_tars_table,
    get_files_to_archive,
    run_command,
    tars_table_exists,
    ts_utc,
)


def create():
    command_info = CommandInfo("create")
    args = setup_create(command_info)

    # Start doing actual work
    logger.debug(f"{ts_utc()}: Running zstash create")
    logger.debug(f"Local path: {command_info.dir_to_archive_absolute}")
    logger.debug(f"HPSS path: {command_info.hpss_path}")
    logger.debug(f"Max size: {command_info.maxsize}")
    logger.debug(f"Keep local tar files: {command_info.keep}")

    # Make sure input path exists and is a directory
    logger.debug("Making sure input path exists and is a directory")
    if not os.path.isdir(command_info.dir_to_archive_absolute):
        # Input path is not a directory
        input_path_error_str: str = f"Input path should be a directory: {command_info.dir_to_archive_absolute}"
        logger.error(input_path_error_str)
        raise NotADirectoryError(input_path_error_str)

    if command_info.hpss_type == HPSSType.GLOBUS:
        # identify globus endpoints
        logger.debug(f"{ts_utc()}: Calling globus_activate")
        globus_activate(command_info.globus_info)
    elif command_info.hpss_type == HPSSType.SAME_MACHINE_HPSS:
        logger.debug(f"{ts_utc()}: Creating target HPSS directory {command_info.hpss_path}")
        mkdir_command: str = f"hsi -q mkdir -p {command_info.hpss_path}"
        mkdir_error_str: str = f"Could not create HPSS directory: {command_info.hpss_path}"
        run_command(mkdir_command, mkdir_error_str)

        # Make sure it is exists and is empty
        logger.debug("Making sure target HPSS directory exists and is empty")

        ls_command: str = f'hsi -q "cd {command_info.hpss_path}; ls -l"'
        ls_error_str: str = "Target HPSS directory is not empty"
        run_command(ls_command, ls_error_str)

    # Create cache directory
    logger.debug(f"{ts_utc()}: Creating local cache directory")
    os.chdir(command_info.dir_to_archive_absolute)
    try:
        os.makedirs(command_info.cache_dir)
    except OSError as exc:
        if exc.errno != errno.EEXIST:
            cache_error_str: str = "Cannot create local cache directory"
            logger.error(cache_error_str)
            raise OSError(cache_error_str)

    # TODO: Verify that cache is empty

    # Create and set up the database
    logger.debug(f"{ts_utc()}: Calling create_database()")
    failures: List[str] = create_database(command_info, args)

    # Transfer to HPSS. Always keep a local copy of the database.
    logger.debug(f"{ts_utc()}: calling hpss_put() for {command_info.get_db_name()}")
    # TODO: (A) Continue refactoring from here
    hpss_put(command_info, command_info.get_db_name(), is_index=True)

    logger.debug(f"{ts_utc()}: calling globus_finalize()")
    globus_finalize(non_blocking=args.non_blocking)

    if len(failures) > 0:
        # List the failures
        logger.warning("Some files could not be archived")
        for file_path in failures:
            logger.error("Failed to archive {}".format(file_path))


def setup_create(ci: CommandInfo) -> argparse.Namespace:
    # Parser
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        usage="zstash create [<args>] path", description="Create a new zstash archive"
    )
    parser.add_argument("path", type=str, help="root directory to archive")
    required: argparse._ArgumentGroup = parser.add_argument_group(
        "required named arguments"
    )
    required.add_argument(
        "--hpss",
        type=str,
        help=(
            'path to storage on HPSS. Set to "none" for local archiving. It also can be a Globus URL, '
            'globus://<GLOBUS_ENDPOINT_UUID>/<PATH>. Names "alcf" and "nersc" are recognized as referring to the ALCF HPSS '
            "and NERSC HPSS endpoints, e.g. globus://nersc/~/my_archive."
        ),
        required=True,
    )
    optional: argparse._ArgumentGroup = parser.add_argument_group(
        "optional named arguments"
    )
    optional.add_argument(
        "--include", type=str, help="comma separated list of file patterns to include"
    )
    optional.add_argument(
        "--exclude", type=str, help="comma separated list of file patterns to exclude"
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
        help='the path to the zstash archive on the local file system. The default name is "zstash".',
    )
    optional.add_argument(
        "--non-blocking",
        action="store_true",
        help="do not wait for each Globus transfer to complete before creating additional archive files.  This option will use more intermediate disk-space, but can increase throughput.",
    )
    optional.add_argument(
        "-v", "--verbose", action="store_true", help="increase output verbosity"
    )
    optional.add_argument(
        "--no_tars_md5",
        action="store_true",
        help="For testing/debugging only. Will not create the tars table or compute the hashes of the tars.",
    )
    optional.add_argument(
        "--follow-symlinks",
        action="store_true",
        help="Hard copy symlinks. This is useful for preventing broken links. Note that a broken link will result in a failed create.",
    )
    # Now that we're inside a subcommand, ignore the first two argvs
    # (zstash create)
    args: argparse.Namespace = parser.parse_args(sys.argv[2:])
    if (not args.hpss) or (args.hpss.lower() == "none"):
        args.hpss = "none"
        args.keep = True
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    if args.cache:
        ci.cache_dir = args.cache
    ci.keep = args.keep
    ci.set_dir_to_archive(args.path)
    ci.set_maxsize(args.maxsize)
    ci.set_hpss_parameters(args.hpss)

    return args


def create_database(command_info: CommandInfo, args: argparse.Namespace) -> List[str]:
    # Create new database
    logger.debug(f"{ts_utc()}:Creating index database")
    db_name: str = command_info.get_db_name()
    if os.path.exists(db_name):
        # Remove old database
        os.remove(db_name)
    con: sqlite3.Connection = sqlite3.connect(
        db_name, detect_types=sqlite3.PARSE_DECLTYPES
    )
    cur: sqlite3.Cursor = con.cursor()

    # Create 'config' table
    cur.execute(
        """
create table config (
  arg text primary key,
  value text
);
    """
    )
    con.commit()

    # Create 'files' table
    cur.execute(
        """
create table files (
  id integer primary key,
  name text,
  size integer,
  mtime timestamp,
  md5 text,
  tar text,
  offset integer
);
    """
    )
    con.commit()

    if not args.no_tars_md5:
        create_tars_table(cur, con)
    elif tars_table_exists(cur):
        raise Exception("tars table exists but it should not")

    # Store configuration in database
    # Loop through all attributes of config.
    for attr in dir(command_info.config):
        value: Any = getattr(command_info.config, attr)
        if not callable(value) and not attr.startswith("__"):
            # config.{attr} is not a function.
            # The attribute name does not start with "__"
            # This creates a new row in the 'config' table.
            # Insert attr for column 1 ('arg')
            # Insert value for column 2 ('text')
            cur.execute("insert into config values (?,?)", (attr, value))
    con.commit()

    files: List[str] = get_files_to_archive(command_info.cache_dir, args.include, args.exclude)

    failures: List[str]
    if args.follow_symlinks:
        try:
            # Add files to archive
            failures = add_files(
                cur,
                con,
                -1,
                files,
                command_info.cache_dir,
                args.keep,
                args.follow_symlinks,
                skip_tars_md5=args.no_tars_md5,
                non_blocking=args.non_blocking,
            )
        except FileNotFoundError:
            raise Exception("Archive creation failed due to broken symlink.")
    else:
        # Add files to archive
        failures = add_files(
            cur,
            con,
            -1,
            files,
            command_info.cache_dir,
            args.keep,
            args.follow_symlinks,
            skip_tars_md5=args.no_tars_md5,
            non_blocking=args.non_blocking,
        )

    # Close database
    con.commit()
    con.close()

    return failures
