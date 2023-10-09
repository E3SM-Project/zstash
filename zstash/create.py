from __future__ import absolute_import, print_function

import argparse
import errno
import logging
import os.path
import sqlite3
import sys
from typing import Any, List, Tuple

from six.moves.urllib.parse import urlparse

from .globus import globus_activate, globus_finalize
from .hpss import hpss_put
from .hpss_utils import add_files
from .settings import DEFAULT_CACHE, config, get_db_filename, logger
from .utils import (
    create_tars_table,
    get_files_to_archive,
    run_command,
    tars_table_exists,
)


def create():
    cache: str
    exclude: str
    cache, args = setup_create()

    # Check config fields
    if config.path is not None:
        path: str = config.path
    else:
        raise TypeError("Invalid config.path={}".format(config.path))
    if config.hpss is not None:
        hpss: str = config.hpss
    else:
        raise TypeError("Invalid config.hpss={}".format(config.hpss))

    # Start doing actual work
    logger.debug("Running zstash create")
    logger.debug("Local path : {}".format(path))
    logger.debug("HPSS path  : {}".format(hpss))
    logger.debug("Max size  : {}".format(config.maxsize))
    logger.debug("Keep local tar files  : {}".format(args.keep))

    # Make sure input path exists and is a directory
    logger.debug("Making sure input path exists and is a directory")
    if not os.path.isdir(path):
        # Input path is not a directory
        input_path_error_str: str = "Input path should be a directory: {}".format(path)
        logger.error(input_path_error_str)
        raise NotADirectoryError(input_path_error_str)

    if hpss != "none":
        url = urlparse(hpss)
        if url.scheme == "globus":
            globus_activate(hpss)
        else:
            # config.hpss is not "none", so we need to
            # create target HPSS directory
            logger.debug("Creating target HPSS directory")
            mkdir_command: str = "hsi -q mkdir -p {}".format(hpss)
            mkdir_error_str: str = "Could not create HPSS directory: {}".format(hpss)
            run_command(mkdir_command, mkdir_error_str)

            # Make sure it is exists and is empty
            logger.debug("Making sure target HPSS directory exists and is empty")

            ls_command: str = 'hsi -q "cd {}; ls -l"'.format(hpss)
            ls_error_str: str = "Target HPSS directory is not empty"
            run_command(ls_command, ls_error_str)

    # Create cache directory
    logger.debug("Creating local cache directory")
    os.chdir(path)
    try:
        os.makedirs(cache)
    except OSError as exc:
        if exc.errno != errno.EEXIST:
            cache_error_str: str = "Cannot create local cache directory"
            logger.error(cache_error_str)
            raise OSError(cache_error_str)

    # TODO: Verify that cache is empty

    # Create and set up the database
    failures: List[str] = create_database(cache, args)

    # Transfer to HPSS. Always keep a local copy.
    hpss_put(hpss, get_db_filename(cache), cache, keep=True)

    globus_finalize(non_blocking=args.non_blocking)

    if len(failures) > 0:
        # List the failures
        logger.warning("Some files could not be archived")
        for file_path in failures:
            logger.error("Failed to archive {}".format(file_path))


def setup_create() -> Tuple[str, argparse.Namespace]:
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
        help="do not wait for each Globus transfer until it completes.",
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
    if args.hpss and args.hpss.lower() == "none":
        args.hpss = "none"
    if args.non_blocking:
        args.keep = True
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    # Copy configuration
    config.path = os.path.abspath(args.path)
    config.hpss = args.hpss
    config.maxsize = int(1024 * 1024 * 1024 * args.maxsize)
    cache: str
    if args.cache:
        cache = args.cache
    else:
        cache = DEFAULT_CACHE

    return cache, args


def create_database(cache: str, args: argparse.Namespace) -> List[str]:
    # Create new database
    logger.debug("Creating index database")
    if os.path.exists(get_db_filename(cache)):
        # Remove old database
        os.remove(get_db_filename(cache))
    con: sqlite3.Connection = sqlite3.connect(
        get_db_filename(cache), detect_types=sqlite3.PARSE_DECLTYPES
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
    for attr in dir(config):
        value: Any = getattr(config, attr)
        if not callable(value) and not attr.startswith("__"):
            # config.{attr} is not a function.
            # The attribute name does not start with "__"
            # This creates a new row in the 'config' table.
            # Insert attr for column 1 ('arg')
            # Insert value for column 2 ('text')
            cur.execute("insert into config values (?,?)", (attr, value))
    con.commit()

    files: List[str] = get_files_to_archive(cache, args.include, args.exclude)

    failures: List[str]
    if args.follow_symlinks:
        try:
            # Add files to archive
            failures = add_files(
                cur,
                con,
                -1,
                files,
                cache,
                args.keep,
                args.follow_symlinks,
                skip_tars_md5=args.no_tars_md5,
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
            cache,
            args.keep,
            args.follow_symlinks,
            skip_tars_md5=args.no_tars_md5,
        )

    # Close database
    con.commit()
    con.close()

    return failures
