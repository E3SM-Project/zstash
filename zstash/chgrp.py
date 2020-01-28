from __future__ import print_function, absolute_import

import argparse
import logging
import sys
from .hpss import hpss_chgrp
from .settings import logger

def chgrp():

    # Parser
    parser = argparse.ArgumentParser(
        usage='zstash chgrp [<args>] group hpss_archive',
        description='Change the group of an HPSS repository.')
    parser.add_argument('group', type=str, help='new group name of file(s)')
    parser.add_argument('hpss', type=str, help='path to HPSS storage')
    parser.add_argument('-R', action='store_const', const=True, help='recurse through subdirectories')
    parser.add_argument('-v', '--verbose', action="store_true", 
                        help="increase output verbosity")

    args = parser.parse_args(sys.argv[2:])
    if args.verbose: logger.setLevel(logging.DEBUG)
    recurse = True if args.R else False
    hpss_chgrp(args.hpss, args.group, recurse)

