import argparse
import logging
import os.path
import sys

import settings
from create import create
from update import update
from extract import extract


# -----------------------------------------------------------------------------
def main():

    logging.basicConfig(format='%(levelname)s: %(message)s',
                        level=logging.DEBUG)
    parser = argparse.ArgumentParser(
        usage='''zstash <command> [<args>]

Available zstash commands:
  create     create new archive
  update     update existing archive
  extract    extract files from archive

For help with a specific command
  zstash command --help
''')
    parser.add_argument('command',
                        help='command to run (create, update, extract, ...)')
    # parse_args defaults to [1:] for args, but you need to
    # exclude the rest of the args too, or validation will fail
    args = parser.parse_args(sys.argv[1:2])

    if args.command == 'create':
        create()
    elif args.command == 'update':
        update()
    elif args.command == 'extract':
        extract()
    else:
        print 'Unrecognized command'
        parser.print_help()
        exit(1)


# -----------------------------------------------------------------------------
if __name__ == '__main__':
    main()
