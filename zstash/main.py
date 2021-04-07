#!/usr/bin/env python
from __future__ import absolute_import, print_function

import argparse
import os
import os.path
import sys
from signal import SIGINT, signal

from . import __version__
from .check import check
from .chgrp import chgrp
from .create import create
from .extract import extract
from .ls import ls
from .update import update


# -----------------------------------------------------------------------------
# TODO: get the types of these parameters
def handler(signal_received, frame):

    # Handle any cleanup here
    print("SIGINT or CTRL-C detected. Exiting.")
    os._exit(1)


# -----------------------------------------------------------------------------
def main():

    # Run the handler() function when SIGINT is received
    signal(SIGINT, handler)

    # Parser
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        usage="""For {}, zstash <command> [<args>]

Available zstash commands:
  version    print the version of zstash
  create     create new archive
  update     update existing archive
  extract    extract files from archive
  chgrp      change the group of an archive
  check      check the integrity of the files in the archive
  ls         list the files in an archive

For help with a specific command
  zstash command --help
""".format(
            __version__
        )
    )
    parser.add_argument("command", help="command to run (create, update, extract, ...)")
    # parse_args defaults to [1:] for args, but you need to
    # exclude the rest of the args too, or validation will fail
    args: argparse.Namespace = parser.parse_args(sys.argv[1:2])

    if args.command == "version":
        print(__version__)
    elif args.command == "create":
        create()
    elif args.command == "update":
        update()
    elif args.command == "extract":
        extract()
    elif args.command == "chgrp":
        chgrp()
    elif args.command == "check":
        check()
    elif args.command == "ls":
        ls()
    else:
        print("Unrecognized command")
        parser.print_help()
        sys.exit(1)


# -----------------------------------------------------------------------------
if __name__ == "__main__":
    main()
