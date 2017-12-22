"""
Module to update existing zstash archives

Originally written by Chris Golaz
Updated by Sterling Baldwin
"""

import sys
import os
import stat
import argparse
import logging
import sqlite3

from datetime import datetime
from lib.util import hpss_get, hpss_put, excludeFiles, addfiles


# -----------------------------------------------------------------------------
def update(config):
    """
    Command to update existing zstash archive

    Parameters:
        config (Config): The main config object
    Returns:
        None
    """
    # Parser
    parser = argparse.ArgumentParser(
        usage='zstash update [<args>]',
        description='Update an existing zstash archive')
    required = parser.add_argument_group('required named arguments')
    optional = parser.add_argument_group('optional named arguments')
    optional.add_argument('--hpss', type=str, help='path to HPSS storage')
    optional.add_argument(
        '--exclude', type=str,
        help='comma separated list of file patterns to exclude')
    optional.add_argument(
        '--dry-run',
        help='dry run, only list files to be updated in archive',
        action="store_true")
    args = parser.parse_args(sys.argv[2:])

    # Open database
    logging.debug('Opening index database')
    if not os.path.exists(config.db_filename):
        # will need to retrieve from HPSS
        if args.hpss is not None:
            config.hpss = args.hpss
            hpss_get(config.hpss, config.db_filename)
        else:
            logging.error('--hpss argument is required when local copy of '
                          'database is unavailable')
            raise Exception
    config.connnection = sqlite3.connect(
        config.db_filename,
        detect_types=sqlite3.PARSE_DECLTYPES)
    config.cursor = config.connnection.cursor()

    # Retrieve configuration from database
    for attr in dir(config):
        value = getattr(config, attr)
        if not callable(value) and not attr.startswith("__"):
            config.cursor.execute(
                u"select value from config where arg=?", (attr,))
            value = config.cursor.fetchone()[0]
            setattr(config, attr, value)
    config.maxsize = int(config.maxsize)

    # Start doing actual work
    logging.debug('Running zstash update')

    # List of files
    logging.info('Gathering list of files to archive')
    files = []
    for root, dirnames, filenames in os.walk('.'):
        # Empty directory
        if not dirnames and not filenames:
            files.append((root, ''))
        # Loop over files
        for filename in filenames:
            files.append((root, filename))

    # Sort files by directories and filenames
    files = sorted(files, key=lambda x: (x[0], x[1]))

    # Relative file path, eliminating top level zstash directory
    files = [os.path.normpath(os.path.join(x[0], x[1]))
             for x in files if x[0] != os.path.join('.', config.cache)]

    # Eliminate files based on exclude pattern
    if args.exclude is not None:
        files = excludeFiles(args.exclude, files)

    # Eliminate files that are already archived and up to date
    newfiles = []
    for file in files:

        statinfo = os.lstat(file)
        mdtime_new = datetime.utcfromtimestamp(statinfo.st_mtime)
        mode = statinfo.st_mode
        # For symbolic links or directories, size should be 0
        if stat.S_ISLNK(mode) or stat.S_ISDIR(mode):
            size_new = 0
        else:
            size_new = statinfo.st_size

        config.cursor.execute(u"select * from files where name = ?", (file,))
        new = True
        while True:
            match = config.cursor.fetchone()
            if match is None:
                break
            size = match[2]
            mdtime = match[3]
            if (size_new == size) and (mdtime_new == mdtime):
                # File exists with same size and modification time
                new = False
                break
            # print(file,size_new,size,mdtime_new,mdtime)
        if (new):
            newfiles.append(file)

    # Anything to do?
    if len(newfiles) == 0:
        logging.info('Nothing to update')
        return

    # --dry-run option
    if args.dry_run:
        print("List of files to be updated")
        for file in newfiles:
            print(file)
        return

    # Find last used tar archive
    itar = -1
    config.cursor.execute(u"select distinct tar from files")
    tfiles = config.cursor.fetchall()
    for tfile in tfiles:
        itar = max(itar, int(tfile[0][0:6], 16))

    # Add files
    failures = addfiles(itar, newfiles, config)

    # Close database and transfer to HPSS. Always keep local copy
    config.connection.commit()
    config.connection.close()
    hpss_put(config.hpss, config.db_filename, keep=True)

    # List failures
    if len(failures) > 0:
        logging.warning('Some files could not be archived')
        for file in failures:
            logging.error('Archiving %s' % (file))
