from __future__ import absolute_import, print_function

import argparse
import logging
import os.path
import sqlite3
import stat
import sys
from datetime import datetime
from typing import List, Tuple

from .hpss import hpss_get, hpss_put
from .hpss_utils import add_files
from .settings import DEFAULT_CACHE, TIME_TOL, config, get_db_filename, logger
from .utils import exclude_files

con = None
cur = None


# FIXME: C901 'update' is too complex (26)
def update():  # noqa: C901

    # Parser
    parser = argparse.ArgumentParser(
        usage="zstash update [<args>]", description="Update an existing zstash archive"
    )
    # FIXME: F841 local variable 'required' is assigned to but never used
    required = parser.add_argument_group("required named arguments")  # noqa: F841
    optional = parser.add_argument_group("optional named arguments")
    optional.add_argument(
        "--hpss",
        type=str,
        help='path to storage on HPSS. Set to "none" for local archiving. Must be set to "none" if the machine does not have HPSS access.',
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
        "-v", "--verbose", action="store_true", help="increase output verbosity"
    )
    args = parser.parse_args(sys.argv[2:])
    if args.hpss and args.hpss.lower() == "none":
        args.hpss = "none"
    if args.cache:
        cache = args.cache
    else:
        cache = DEFAULT_CACHE
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    # Open database
    logger.debug("Opening index database")
    if not os.path.exists(get_db_filename(cache)):
        # will need to retrieve from HPSS
        if args.hpss is not None:
            config.hpss = args.hpss
            hpss_get(config.hpss, get_db_filename(cache), cache)
        else:
            error_str = (
                "--hpss argument is required when local copy of database is unavailable"
            )
            logger.error(error_str)
            raise Exception(error_str)
    global con, cur
    con = sqlite3.connect(get_db_filename(cache), detect_types=sqlite3.PARSE_DECLTYPES)
    cur = con.cursor()

    # Retrieve some configuration settings from database
    for attr in dir(config):
        value = getattr(config, attr)
        if not callable(value) and not attr.startswith("__"):
            cur.execute(u"select value from config where arg=?", (attr,))
            value = cur.fetchone()[0]
            setattr(config, attr, value)
    # FIXME: Incompatible types in assignment (expression has type "bool", variable has type "None") mypy(error)
    config.maxsize = int(config.maxsize)  # type: ignore
    # FIXME: Incompatible types in assignment (expression has type "bool", variable has type "None") mypy(error)
    config.keep = bool(int(config.keep))  # type: ignore

    # The command line arg should always have precedence
    if args.hpss == "none":
        # If no HPSS is available, always keep the files.
        # FIXME: Incompatible types in string interpolation (expression has type "None", placeholder has type "Union[int, float, SupportsInt]") mypy(error)
        config.keep = True  # type: ignore
    else:
        config.keep = args.keep
    if args.hpss is not None:
        config.hpss = args.hpss

    # Start doing actual work
    logger.debug("Running zstash update")
    logger.debug("Local path : %s" % (config.path))
    logger.debug("HPSS path  : %s" % (config.hpss))
    # FIXME: Incompatible types in string interpolation (expression has type "None", placeholder has type "Union[int, float, SupportsInt]") mypy(error)
    logger.debug("Max size  : %i" % (config.maxsize))  # type: ignore
    logger.debug("Keep local tar files  : %s" % (config.keep))

    # List of files
    logger.info("Gathering list of files to archive")
    files: List[Tuple[str, str]] = []
    for root, dirnames, filenames in os.walk("."):
        # Empty directory
        if not dirnames and not filenames:
            files.append((root, ""))
        # Loop over files
        for filename in filenames:
            files.append((root, filename))

    # Sort files by directories and filenames
    files = sorted(files, key=lambda x: (x[0], x[1]))

    # Relative file path, eliminating top level zstash directory
    # FIXME: Name 'files' already defined mypy(error)
    files: List[str] = [  # type: ignore
        os.path.normpath(os.path.join(x[0], x[1]))
        for x in files
        if x[0] != os.path.join(".", cache)
    ]

    # Eliminate files based on exclude pattern
    if args.exclude is not None:
        files = exclude_files(args.exclude, files)

    # Eliminate files that are already archived and up to date
    newfiles = []
    for file_path in files:
        # FIXME: Argument 1 to "lstat" has incompatible type "Tuple[str, str]"; expected "Union[str, bytes, _PathLike[str], _PathLike[bytes]]" mypy(error)
        statinfo = os.lstat(file_path)  # type: ignore
        mdtime_new = datetime.utcfromtimestamp(statinfo.st_mtime)
        mode = statinfo.st_mode
        # For symbolic links or directories, size should be 0
        if stat.S_ISLNK(mode) or stat.S_ISDIR(mode):
            size_new = 0
        else:
            size_new = statinfo.st_size

        cur.execute(u"select * from files where name = ?", (file_path,))
        new = True
        while True:
            match = cur.fetchone()
            if match is None:
                break
            size = match[2]
            mdtime = match[3]

            if (size_new == size) and (
                abs((mdtime_new - mdtime).total_seconds()) <= TIME_TOL
            ):
                # File exists with same size and modification time within tolerance
                new = False
                break
            # print(file,size_new,size,mdtime_new,mdtime)
        if new:
            newfiles.append(file_path)

    # Anything to do?
    if len(newfiles) == 0:
        logger.info("Nothing to update")
        return

    # --dry-run option
    if args.dry_run:
        print("List of files to be updated")
        for file_path in newfiles:
            print(file_path)
        return

    # Find last used tar archive
    itar = -1
    cur.execute(u"select distinct tar from files")
    tfiles = cur.fetchall()
    for tfile in tfiles:
        itar = max(itar, int(tfile[0][0:6], 16))

    # Add files
    failures = add_files(cur, con, itar, newfiles, cache)

    # Close database and transfer to HPSS. Always keep local copy
    con.commit()
    con.close()
    hpss_put(config.hpss, get_db_filename(cache), cache, keep=True)

    # List failures
    if len(failures) > 0:
        logger.warning("Some files could not be archived")
        for file_path in failures:
            # FIXME: Not all arguments converted during string formatting mypy(error)
            logger.error("Archiving %s" % (file_path))  # type: ignore
