from __future__ import absolute_import, print_function

import argparse
import logging
import sys
from typing import List

from .hpss import hpss_chgrp
from .settings import logger


def chgrp():
    args: argparse.Namespace = setup_chgrp(sys.argv)
    recurse: bool = True if args.R else False
    hpss_chgrp(args.hpss, args.group, recurse)


def setup_chgrp(arg_list: List[str]) -> argparse.Namespace:
    # Parser
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        usage="zstash chgrp [<args>] group hpss_archive",
        description="Change the group of an HPSS repository.",
    )
    parser.add_argument("group", type=str, help="new group name of file(s)")
    parser.add_argument("hpss", type=str, help="path to HPSS storage")
    parser.add_argument(
        "-R", action="store_const", const=True, help="recurse through subdirectories"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="increase output verbosity"
    )

    args: argparse.Namespace = parser.parse_args(arg_list[2:])
    if args.hpss and args.hpss.lower() == "none":
        args.hpss = "none"
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    return args
