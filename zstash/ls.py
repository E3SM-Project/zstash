from __future__ import print_function, absolute_import

import os
import sys
import argparse
import logging
import sqlite3
from .hpss import hpss_get
from .settings import config, logger, BLOCK_SIZE, DEFAULT_CACHE, get_db_filename


def ls():
    """
    List all of the files in the HPSS path.
    Supports the '-l' argument for more information.
    """
    parser = argparse.ArgumentParser(
        usage='zstash ls [<args>] [files]',
        description='List the files from an existing archive. If `files` is specified, then only the files specified will be listed. If `hpss=none`, then this will list the directories and files in the current directory excluding the cache.')
    optional = parser.add_argument_group('optional named arguments')
    optional.add_argument('--hpss', type=str, help='path to HPSS storage')
    optional.add_argument('-l', dest='long', action='store_const', const=True,
                          help='show more information for the files')
    optional.add_argument(
        '--cache', type=str, help='the path to the zstash archive on the local file system. The default name is "zstash".')
    optional.add_argument('-v', '--verbose', action="store_true", 
                          help="increase output verbosity")
    
    parser.add_argument('files', nargs='*', default=['*'])
    args = parser.parse_args(sys.argv[2:])
    if args.hpss and args.hpss.lower() == 'none':
        args.hpss = 'none'
    if args.cache:
        cache = args.cache
    else:
        cache = DEFAULT_CACHE
    if args.verbose: logger.setLevel(logging.DEBUG)

    # Open database
    logger.debug('Opening index database')
    if not os.path.exists(get_db_filename(cache)):
        # Will need to retrieve from HPSS
        if args.hpss is not None:
            config.hpss = args.hpss
            hpss_get(config.hpss, get_db_filename(cache), cache)
        else:
            error_str = '--hpss argument is required when local copy of database is unavailable'
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
    config.maxsize = int(config.maxsize)
    config.keep = bool(int(config.keep))

    # The command line arg should always have precedence
    if args.hpss is not None:
        config.hpss = args.hpss

    # Start doing actual work
    logger.debug('Running zstash ls')
    logger.debug('HPSS path  : %s' % (config.hpss))

    # Find matching files
    matches = []
    for args_file in args.files:
        cur.execute(u"select * from files where name GLOB ? or tar GLOB ?", (args_file, args_file))
        matches = matches + cur.fetchall()

    # Remove duplicates
    matches = list(set(matches))

    # Sort by tape and order within tapes (offset)
    matches = sorted(matches, key=lambda x: (x[5], x[6]))

    if args.long:
        # Get the names of the cols
        cur.execute(u"PRAGMA table_info(files);")
        cols = [str(col_info[1]) for col_info in cur.fetchall()]
        print('\t'.join(cols))

    # Print the results
    for match in matches:
        if args.long:
            # Print all contents of each match
            for col in match:
                print(col, end='\t')
            print('')
        else:
            # Just print the file name
            print(match[1])
