from __future__ import print_function, absolute_import

import sys
import argparse
from .hpss import hpss_chgrp

def chgrp():

    # Parser
    parser = argparse.ArgumentParser(
        usage='zstash chgrp [<args>] group hpss_archive',
        description='Change the group of an HPSS repository.')
    parser.add_argument('group', type=str, help='new group name of file(s)')
    parser.add_argument('hpss', type=str, help='path to HPSS storage')
    parser.add_argument('-R', action='store_const', const=True, help='recurse through subdirectories')

    args = parser.parse_args(sys.argv[2:])
    recurse = True if args.R else False
    hpss_chgrp(args.hpss, args.group, recurse)

