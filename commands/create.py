"""
The Create command for Zstash

Originally written by Chris Golaz
Updated by Sterling Baldwin
"""
import os
import sys
import logging
import argparse
import shlex
import errno
import sqlite3

from subprocess import Popen, PIPE
from lib.util import hpss_put, addfiles, excludeFiles


def create(config):
    """
    Create a new HPSS tar archive

    Parameters:
        config (Config): The master config object
    Returns:
        None
    """
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
        help='keep files in local cache (default off)',
        action="store_true")
    # Now that we're inside a subcommand, ignore the first two argvs
    # (zstash create)
    args = parser.parse_args(sys.argv[2:])

    # Copy configuration
    config.path = os.path.abspath(args.path)
    config.hpss = args.hpss
    config.maxsize = int(1024 * 1024 * 1024 * args.maxsize)
    config.keep = args.keep

    # Start doing actual work
    logging.debug('Running zstash create')
    logging.debug('Local path : %s', config.path)
    logging.debug('HPSS path  : %s', config.hpss)
    logging.debug('Max size  : %i', config.maxsize)

    # Make sure input path exists and is a directory
    logging.debug('Making sure input path exists and is a directory')
    if not os.path.isdir(config.path):
        logging.error('Input path should be a directory: %s', config.path)
        raise Exception

    # Create target HPSS directory if needed
    logging.debug('Creating target HPSS directory')
    proc = Popen(['hsi', '-q', 'mkdir', '-p', config.hpss],
                 stdout=PIPE, stderr=PIPE)
    (stdout, stderr) = proc.communicate()
    status = proc.returncode
    if status != 0:
        logging.error('Could not create HPSS directory: %s', config.hpss)
        logging.debug('stdout:\n%s', stdout)
        logging.debug('stderr:\n%s', stderr)
        raise Exception

    # Make sure it is empty
    logging.debug('Making sure target HPSS directory exists and is empty')
    cmd = 'hsi -q "cd %s; ls -l"' % (config.hpss)
    proc = Popen(shlex.split(cmd), stdout=PIPE, stderr=PIPE)
    (stdout, stderr) = proc.communicate()
    status = proc.returncode
    if status != 0 or \
            len(stdout) != 0 or \
            len(stderr) != 0:
        logging.error('Target HPSS directory is not empty')
        logging.debug('stdout:\n%s', stdout)
        logging.debug('stderr:\n%s', stderr)
        raise Exception

    # Create cache directory
    logging.debug('Creating local cache directory')
    os.chdir(config.path)
    try:
        os.makedirs(config.cache)
    except OSError as exc:
        if exc.errno != errno.EEXIST:
            logging.error('Cannot create local cache directory')
            raise Exception

    # Verify that cache is empty
    # ...to do (?)

    # Create new database
    logging.debug('Creating index database')
    if os.path.exists(config.db_filename):
        os.remove(config.db_filename)
    config.connection = sqlite3.connect(
        config.db_filename, detect_types=sqlite3.PARSE_DECLTYPES)
    config.cursor = config.connection.cursor()

    # Create 'config' table
    config.cursor.execute(u"""
create table config (
  arg text primary key,
  value text
);
    """)
    config.connection.commit()

    # Create 'files' table
    config.cursor.execute(u"""
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
    config.connection.commit()

    # Store configuration in database
    for attr, value in config.items():
        if not callable(value) and not attr.startswith("__"):
            config.cursor.execute(
                u"insert into config values (?,?)", (attr, value))
    config.connection.commit()

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

    # Add files to archive
    failures = addfiles(-1, files, config)

    # Close database and transfer to HPSS. Always keep local copy
    config.connection.commit()
    config.connection.close()
    hpss_put(config.hpss, config.db_filename, keep=True)

    # List failures
    if len(failures) > 0:
        logging.warning('Some files could not be archived')
        for file in failures:
            logging.error('Archiving %s', file)
