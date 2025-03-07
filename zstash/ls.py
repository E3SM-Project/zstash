from __future__ import absolute_import, print_function

import argparse
import logging
import os
import sqlite3
import sys
from typing import List, Union

from .hpss import hpss_get
from .settings import FilesRow, TarsRow, TupleFilesRow, TupleTarsRow, logger
from .utils import CommandInfo, HPSSType, tars_table_exists


def ls():
    """
    List all of the files in the HPSS path.
    Supports the '-l' argument for more information.
    """
    command_info = CommandInfo("ls")
    args: argparse.Namespace = setup_ls(command_info)
    matches: List[FilesRow] = ls_database(command_info, args)

    print_matches(args, matches)

    if args.tars:
        tar_matches: List[TarsRow] = ls_tars_database(command_info, args)
        print_matches(args, tar_matches)


def setup_ls(command_info: CommandInfo) -> argparse.Namespace:
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        usage="zstash ls [<args>] [files]",
        description="List the files from an existing archive. If `files` is specified, then only the files specified will be listed. If `hpss=none`, then this will list the directories and files in the current directory excluding the cache.",
    )
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
        "-l",
        dest="long",
        action="store_const",
        const=True,
        help="show more information for the files",
    )
    optional.add_argument(
        "--cache",
        type=str,
        help='the path to the zstash archive on the local file system. The default name is "zstash".',
    )
    optional.add_argument("--tars", action="store_true", help="Display tars")
    optional.add_argument(
        "-v", "--verbose", action="store_true", help="increase output verbosity"
    )

    parser.add_argument("files", nargs="*", default=["*"])
    args: argparse.Namespace = parser.parse_args(sys.argv[2:])

    if args.hpss and (args.hpss.lower() == "none"):
        args.hpss = "none"
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    if args.cache:
        command_info.cache_dir = args.cache
    command_info.set_dir_to_archive(os.getcwd())
    command_info.set_hpss_parameters(args.hpss, null_hpss_allowed=True)

    return args


def ls_database(command_info: CommandInfo, args: argparse.Namespace) -> List[FilesRow]:
    # Open database
    logger.debug("Opening index database")
    if not os.path.exists(command_info.get_db_name()):
        # Will need to retrieve from HPSS
        if command_info.hpss_type != HPSSType.UNDEFINED:
            try:
                # Retrieve from HPSS
                hpss_get(command_info, command_info.get_db_name())
            except RuntimeError:
                raise FileNotFoundError("There was nothing to ls.")
        else:
            error_str: str = (
                "--hpss argument is required when local copy of database is unavailable"
            )
            logger.error(error_str)
            raise ValueError(error_str)

    con: sqlite3.Connection = sqlite3.connect(
        command_info.get_db_name(), detect_types=sqlite3.PARSE_DECLTYPES
    )
    cur: sqlite3.Cursor = con.cursor()

    command_info.update_config_using_db(cur)
    command_info.validate_maxsize()

    # Start doing actual work
    logger.debug("Running zstash ls")
    logger.debug(f"HPSS path  : {command_info.config.hpss}")

    # Find matching files
    matches_: List[TupleFilesRow] = []
    for args_file in args.files:
        cur.execute(
            "select * from files where name GLOB ? or tar GLOB ?",
            (args_file, args_file),
        )
        matches_ = matches_ + cur.fetchall()

    if matches_ == []:
        raise FileNotFoundError("There was nothing to ls.")

    # Remove duplicates
    matches_ = list(set(matches_))
    matches: List[FilesRow] = list(map(FilesRow, matches_))

    # Sort by tape and order within tapes (offset)
    matches = sorted(matches, key=lambda t: (t.tar, t.offset))

    if args.long:
        # Get the names of the columns
        cur.execute("PRAGMA table_info(files);")
        cols = [str(col_info[1]) for col_info in cur.fetchall()]
        print("\t".join(cols))

    # Close database
    con.commit()
    con.close()

    return matches


def ls_tars_database(
    command_info: CommandInfo, args: argparse.Namespace
) -> List[TarsRow]:
    con: sqlite3.Connection = sqlite3.connect(
        command_info.get_db_name(), detect_types=sqlite3.PARSE_DECLTYPES
    )
    cur: sqlite3.Cursor = con.cursor()

    if not tars_table_exists(cur):
        print("\ntars table does not exist")
        return []

    # Find matching files
    cur.execute("select * from tars")
    matches_: List[TupleTarsRow] = cur.fetchall()

    # Remove duplicates
    matches_ = list(set(matches_))
    matches: List[TarsRow] = list(map(TarsRow, matches_))

    # Sort by name
    matches = sorted(matches, key=lambda t: (t.name))

    if matches != []:
        print("\nTars:")
        if args.long:
            # Get the names of the columns
            cur.execute("PRAGMA table_info(tars);")
            cols = [str(col_info[1]) for col_info in cur.fetchall()]
            print("\t".join(cols))

    # Close database
    con.commit()
    con.close()

    return matches


def print_matches(
    args: argparse.Namespace, matches: Union[List[FilesRow], List[TarsRow]]
):
    # Print the results
    match: Union[FilesRow, TarsRow]
    for match in matches:
        if args.long:
            # Print all contents of each match
            for col in match.to_tuple():
                print(col, end="\t")
            print("")
        else:
            # Just print the name
            print(match.name)
