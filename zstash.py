"""
Zstash, a python utility for storing and retrieving directories
from an HPSS system

Originally written by Chris Golaz
Updated by Sterling Baldwin
"""
import argparse
import logging
import os.path
import sys

from zstash.commands.create import create
from zstash.commands.update import update
from zstash.commands.extract import extract
from zstash.lib.config import Config
from zstash.lib.strings import main_usage

# Block size
BLOCK_SIZE = 1024 * 1014

# Sub-directory to hold cache
CACHE = 'zstash'

# Database filename
DB_FILENAME = os.path.join(CACHE, 'index.db')


# -----------------------------------------------------------------------------
def main():
    """
    Main method. Parses command to use and dispatches to handler
    """
    logging.basicConfig(format='%(levelname)s: %(message)s',
                        level=logging.DEBUG)
    parser = argparse.ArgumentParser(usage=main_usage)
    parser.add_argument(
        'command',
        help='command to run (create, update, extract, ...)')
    # parse_args defaults to [1:] for args, but you need to
    # exclude the rest of the args too, or validation will fail
    args = parser.parse_args(sys.argv[1:2])

    config = Config(
        cache=CACHE,
        db_filename=DB_FILENAME,
        block_size=BLOCK_SIZE)

    command_map = {
        'create': create,
        'update': update,
        'extract': extract
    }

    if command_map.get(args.command):
        command_map[args.command](config)
    else:
        print 'Unrecognized command'
        parser.print_help()
        exit(1)


if __name__ == '__main__':
    main()
