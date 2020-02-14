from __future__ import print_function, absolute_import

import argparse
import errno
import logging
import os.path
import shlex
import sys
import sqlite3
import tarfile
from subprocess import Popen, PIPE
from .hpss import hpss_put
from .utils import addfiles, excludeFiles
from .settings import config, logger, CACHE, BLOCK_SIZE, DB_FILENAME


def create():

    # Parser
    parser = argparse.ArgumentParser(
        usage='zstash create [<args>] path',
        description='Create a new zstash archive')
    parser.add_argument('path', type=str, help='root directory to archive')
    required = parser.add_argument_group('required named arguments')
    required.add_argument(
        '--hpss', type=str, help='path to HPSS storage',
        required=True)
    optional = parser.add_argument_group('optional named arguments')
    optional.add_argument(
        '--exclude', type=str,
        help='comma separated list of file patterns to exclude')
    optional.add_argument(
        '--maxsize', type=float,
        help='maximum size of tar archives (in GB, default 256)',
        default=256)
    optional.add_argument(
        '--keep',
        help='keep tar files in local cache (default off)',
        action="store_true")
    optional.add_argument('-v', '--verbose', action="store_true", 
                          help="increase output verbosity")
    # Now that we're inside a subcommand, ignore the first two argvs
    # (zstash create)
    args = parser.parse_args(sys.argv[2:])
    if args.hpss and args.hpss.lower() == 'none':
        args.hpss = 'none'
    if args.verbose: logger.setLevel(logging.DEBUG)

    # Copy configuration
    config.path = os.path.abspath(args.path)
    config.hpss = args.hpss
    config.maxsize = int(1024*1024*1024 * args.maxsize)
    config.keep = args.keep

    # Start doing actual work
    logger.debug('Running zstash create')
    logger.debug('Local path : %s' % (config.path))
    logger.debug('HPSS path  : %s' % (config.hpss))
    logger.debug('Max size  : %i' % (config.maxsize))
    logger.debug('Keep local tar files  : %s' % (config.keep))

    # Make sure input path exists and is a directory
    logger.debug('Making sure input path exists and is a directory')
    if not os.path.isdir(config.path):
        logger.error('Input path should be a directory: %s', config.path)
        raise Exception

    if config.hpss != 'none':
        # Create target HPSS directory if needed
        logger.debug('Creating target HPSS directory')
        p1 = Popen(['hsi', '-q', 'mkdir', '-p', config.hpss],
                   stdout=PIPE, stderr=PIPE)
        (stdout, stderr) = p1.communicate()
        status = p1.returncode
        if status != 0:
            logger.error('Could not create HPSS directory: %s', config.hpss)
            logger.debug('stdout:\n%s', stdout)
            logger.debug('stderr:\n%s', stderr)
            raise Exception
        
        # Make sure it is empty
        logger.debug('Making sure target HPSS directory exists and is empty')
        cmd = 'hsi -q "cd %s; ls -l"' % (config.hpss)
        p1 = Popen(shlex.split(cmd), stdout=PIPE, stderr=PIPE)
        (stdout, stderr) = p1.communicate()
        status = p1.returncode
        if status != 0 or len(stdout) != 0 or len(stderr) != 0:
            logger.error('Target HPSS directory is not empty')
            logger.debug('stdout:\n%s', stdout)
            logger.debug('stderr:\n%s', stderr)
            raise Exception
    
    
    # Create cache directory
    logger.debug('Creating local cache directory')
    os.chdir(config.path)
    try:
        os.makedirs(CACHE)
    except OSError as exc:
        if exc.errno != errno.EEXIST:
            logger.error('Cannot create local cache directory')
            raise Exception
        pass

    # Verify that cache is empty
    # ...to do (?)

    # Create new database
    logger.debug('Creating index database')
    if os.path.exists(DB_FILENAME):
        os.remove(DB_FILENAME)
    global con, cur
    con = sqlite3.connect(DB_FILENAME, detect_types=sqlite3.PARSE_DECLTYPES)
    cur = con.cursor()

    # Create 'config' table
    cur.execute(u"""
create table config (
  arg text primary key,
  value text
);
    """)
    con.commit()

    # Create 'files' table
    cur.execute(u"""
create table files (
  id integer primary key,
  name text,
  size integer,
  mtime timestamp,
  md5 text,
  tar text,
  offset integer
);
    """)
    con.commit()

    # Store configuration in database
    for attr in dir(config):
        value = getattr(config, attr)
        if not callable(value) and not attr.startswith("__"):
            cur.execute(u"insert into config values (?,?)", (attr, value))
    con.commit()

    # List of files
    logger.info('Gathering list of files to archive')
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
             for x in files if x[0] != os.path.join('.', CACHE)]

    # Eliminate files based on exclude pattern
    if args.exclude is not None:
        files = excludeFiles(args.exclude, files)

    # Add files to archive
    failures = addfiles(cur, con, -1, files)

    # Close database and transfer to HPSS. Always keep local copy
    con.commit()
    con.close()
    hpss_put(config.hpss, DB_FILENAME, keep=True)

    # List failures
    if len(failures) > 0:
        logger.warning('Some files could not be archived')
        for file in failures:
            logger.error('Archiving %s' % (file))
