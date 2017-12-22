"""
A module to extract files from a zstash archive

Originally written by Chris Golaz
Updated by Sterling Baldwin
"""

import sys
import os
import logging
import argparse
import sqlite3

from lib.util import hpss_get, extractFiles

def extract(config):
    """
    Extract files from a zstash archive

    Parameters:
        config (Config): the main config object
    Returns:
        None
    """
    parser = argparse.ArgumentParser(
        usage='zstash extract [<args>] [files]',
        description='Extract files from existing archive')
    required = parser.add_argument_group('required named arguments')
    optional = parser.add_argument_group('optional named arguments')
    optional.add_argument('--hpss', type=str, help='path to HPSS storage')
    parser.add_argument('files', nargs='*', default=['*'])
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
            parser.print_help()
            return
    config.connection = sqlite3.connect(
        config.db_filename,
        detect_types=sqlite3.PARSE_DECLTYPES)
    config.cursor = config.connection.cursor()

    # Retrieve configuration from database
    for attr in dir(config):
        value = getattr(config, attr)
        if not callable(value) and not attr.startswith("__"):
            config.cursor.execute(u"select value from config where arg=?", (attr,))
            value = config.cursor.fetchone()[0]
            setattr(config, attr, value)
    config.maxsize = int(config.maxsize)

    # Find matching files
    matches = []
    for file in args.files:
        config.cursor.execute(u"select * from files where name GLOB ?", (file,))
        matches = matches + config.cursor.fetchall()

    # Remove duplicates
    matches = list(set(matches))

    # Sort by tape and order within tapes (offset)
    matches = sorted(matches, key=lambda x: (x[5], x[6]))

    # Retrieve from tapes
    extractFiles(matches, config)

    # Close database
    logging.debug('Closing index database')
    config.connection.close()
