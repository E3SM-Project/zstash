#!/usr/bin/env python

import argparse
import errno
import hashlib
import logging
import os.path
import shlex
import sqlite3
import stat
import sys
import tarfile

from datetime import datetime
from fnmatch import fnmatch
from subprocess import Popen, PIPE

# Block size
BLOCK_SIZE = 1024*1014

# Sub-directory to hold cache
CACHE = 'zstash'

# Database filename
DB_FILENAME = os.path.join(CACHE, 'index.db')


# Class to hold configuration
class Config(object):
    path = None
    hpss = None
    maxsize = None
    keep = None

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

    global config
    config = Config()

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


def create():

    # Parser
    parser = argparse.ArgumentParser(
        usage='zstash create [<args>] path',
        description='Create a new zstash archive')
    parser.add_argument('path', type=str, help='root directory to archive')
    required = parser.add_argument_group('required named arguments')
    required.add_argument('--hpss', type=str, help='path to HPSS storage',
                          required=True)
    optional = parser.add_argument_group('optional named arguments')
    optional.add_argument("--exclude", type=str,
                          help="comma separated list of file patterns to exclude")
    optional.add_argument("--maxsize", type=float,
                          help="maximum size of tar archives "
                               "(in GB, default 256)",
                          default=256)
    optional.add_argument('--keep',
                          help='keep files in local cache (default off)',
                          action="store_true")
    # Now that we're inside a subcommand, ignore the first two argvs
    # (zstash create)
    args = parser.parse_args(sys.argv[2:])

    # Copy configuration
    config.path = os.path.abspath(args.path)
    config.hpss = args.hpss
    config.maxsize = int(1024*1024*1024 * args.maxsize)
    config.keep = args.keep

    # Start doing actual work
    logging.debug('Running zstash create')
    logging.debug('Local path : %s' % (config.path))
    logging.debug('HPSS path  : %s' % (config.hpss))
    logging.debug('Max size  : %i' % (config.maxsize))

    # Make sure input path exists and is a directory
    logging.debug('Making sure input path exists and is a directory')
    if not os.path.isdir(config.path):
        logging.error('Input path should be a directory: %s', config.path)
        raise Exception

    # Create target HPSS directory if needed
    logging.debug('Creating target HPSS directory')
    p1 = Popen(['hsi', '-q', 'mkdir', '-p', config.hpss],
               stdout=PIPE, stderr=PIPE)
    (stdout, stderr) = p1.communicate()
    status = p1.returncode
    if status != 0:
        logging.error('Could not create HPSS directory: %s', config.hpss)
        logging.debug('stdout:\n%s', stdout)
        logging.debug('stderr:\n%s', stderr)
        raise Exception

    # Make sure it is empty
    logging.debug('Making sure target HPSS directory exists and is empty')
    cmd = 'hsi -q "cd %s; ls -l"' % (config.hpss)
    p1 = Popen(shlex.split(cmd), stdout=PIPE, stderr=PIPE)
    (stdout, stderr) = p1.communicate()
    status = p1.returncode
    if status != 0 or len(stdout) != 0 or len(stderr) != 0:
        logging.error('Target HPSS directory is not empty')
        logging.debug('stdout:\n%s', stdout)
        logging.debug('stderr:\n%s', stderr)
        raise Exception

    # Create cache directory
    logging.debug('Creating local cache directory')
    os.chdir(config.path)
    try:
        os.makedirs(CACHE)
    except OSError as exc:
        if exc.errno != errno.EEXIST:
            logging.error('Cannot create local cache directory')
            raise Exception
        pass

    # Verify that cache is empty
    # ...to do (?)

    # Create new database
    logging.debug('Creating index database')
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
             for x in files if x[0] != os.path.join('.', CACHE)]

    # Eliminate files based on exclude pattern
    if args.exclude != None:
        files = excludeFiles(args.exclude, files)

    # Add files to archive
    failures = addfiles(-1, files)

    # Close database and transfer to HPSS. Always keep local copy
    con.commit()
    con.close()
    hpss_put(config.hpss, DB_FILENAME, keep=True)

    # List failures
    if len(failures) > 0:
        logging.warning('Some files could not be archived')
        for file in failures:
            logging.error('Archiving %s' % (file))


def excludeFiles(exclude, files):

    # Construct lits of files to exclude, based on
    #  https://codereview.stackexchange.com/questions/33624/
    #  filtering-a-long-list-of-files-through-a-set-of-ignore-patterns-using-iterators
    exclude_patterns = exclude.split(',')
    exclude_files = []
    for file in files:
        if any(fnmatch(file, pattern) for pattern in exclude_patterns):
            exclude_files.append(file)
            continue

    # Now, remove them
    new_files = [f for f in files if f not in exclude_files]

    return new_files


def addfiles(itar, files):

    # File size
    files = [(x, os.path.getsize(x)) for x in files]

    # Now, perform the actual archiving
    failures = []
    newtar = True
    nfiles = len(files)
    for i in range(nfiles):

        # New tar archive in the local cache
        if newtar:
            newtar = False
            tarsize = 0
            itar += 1
            tname = "{0:0{1}x}".format(itar, 6)
            tfname = "%s.tar" % (tname)
            logging.info('Creating new tar archive %s' % (tfname))
            tar = tarfile.open(os.path.join(CACHE, tfname), "w")

        # Add current file to tar archive
        file = files[i]
        logging.info('Archiving %s' % (file[0]))
        try:
            offset, size, mtime, md5 = addfile(tar, file[0])
            cur.execute(u"insert into files values (NULL,?,?,?,?,?,?)",
                        (file[0], size, mtime, md5, tfname, offset))
            con.commit()
            tarsize += file[1]
        except:
            logging.error('Archiving %s' % (file[0]))
            failures.append(file[0])

        # Close tar archive if current file is the last one or adding one more
        # would push us over the limit.
        if (i == nfiles-1 or tarsize+files[i+1][1] > config.maxsize):

            # Close current temporary file
            logging.debug('Closing tar archive %s' % (tfname))
            tar.close()

            # Transfer tar archive to HPSS
            hpss_put(config.hpss, os.path.join(CACHE, tfname), config.keep)

            # Open new archive next time
            newtar = True

    return failures

def update():

    # Parser
    parser = argparse.ArgumentParser(
        usage='zstash update [<args>]',
        description='Update an existing zstash archive')
    required = parser.add_argument_group('required named arguments')
    optional = parser.add_argument_group('optional named arguments')
    optional.add_argument('--hpss', type=str, help='path to HPSS storage')
    optional.add_argument("--exclude", type=str,
                          help="comma separated list of file patterns to exclude")
    optional.add_argument('--dry-run',
                          help='dry run, only list files to be updated in archive',
                          action="store_true")
    args = parser.parse_args(sys.argv[2:])

    # Open database
    logging.debug('Opening index database')
    if not os.path.exists(DB_FILENAME):
        # will need to retrieve from HPSS
        if args.hpss is not None:
            config.hpss = args.hpss
            hpss_get(config.hpss, DB_FILENAME)
        else:
            logging.error('--hpss argument is required when local copy of '
                          'database is unavailable')
            raise Exception
    global con, cur
    con = sqlite3.connect(DB_FILENAME, detect_types=sqlite3.PARSE_DECLTYPES)
    cur = con.cursor()

    # Retrieve configuration from database
    for attr in dir(config):
        value = getattr(config, attr)
        if not callable(value) and not attr.startswith("__"):
            cur.execute(u"select value from config where arg=?", (attr,))
            value = cur.fetchone()[0]
            setattr(config, attr, value)
    config.maxsize = int(config.maxsize)

    # Start doing actual work
    logging.debug('Running zstash update')

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
             for x in files if x[0] != os.path.join('.', CACHE)]

    # Eliminate files based on exclude pattern
    if args.exclude != None:
        files = excludeFiles(args.exclude, files)

    # Eliminate files that are already archived and up to date
    newfiles = []
    for file in files:

        statinfo = os.lstat(file)
        mdtime_new = datetime.utcfromtimestamp(statinfo.st_mtime)
        mode = statinfo.st_mode
        # For symbolic links or directories, size should be 0
        if stat.S_ISLNK(mode) or stat.S_ISDIR(mode):
           size_new = 0
        else:
           size_new = statinfo.st_size

        cur.execute(u"select * from files where name = ?", (file,))
        new = True
        while True:
            match = cur.fetchone()
            if match == None:
                break;
            size = match[2]
            mdtime = match[3]
            if (size_new == size) and (mdtime_new == mdtime):
                # File exists with same size and modification time
                new = False
                break;
            #print(file,size_new,size,mdtime_new,mdtime)
        if (new):
            newfiles.append(file)

    # Anything to do?
    if len(newfiles) == 0:
        logging.info('Nothing to update')
        return

    # --dry-run option
    if args.dry_run:
        print("List of files to be updated")
        for file in newfiles:
            print(file)
        return

    # Find last used tar archive
    itar = -1
    cur.execute(u"select distinct tar from files")
    tfiles = cur.fetchall()
    for tfile in tfiles:
        itar = max(itar, int(tfile[0][0:6], 16))

    # Add files
    failures = addfiles(itar, newfiles)

    # Close database and transfer to HPSS. Always keep local copy
    con.commit()
    con.close()
    hpss_put(config.hpss, DB_FILENAME, keep=True)

    # List failures
    if len(failures) > 0:
        logging.warning('Some files could not be archived')
        for file in failures:
            logging.error('Archiving %s' % (file))

# Add file to tar archive while computing its hash
# Return file offset (in tar archive), size and md5 hash
def addfile(tar, file):
    offset = tar.offset
    tarinfo = tar.gettarinfo(file)
    tar.addfile(tarinfo)
    if tarinfo.isfile():
        f = open(file, "rb")
        hash_md5 = hashlib.md5()
        while True:
            s = f.read(BLOCK_SIZE)
            if len(s) > 0:
                tar.fileobj.write(s)
                hash_md5.update(s)
            if len(s) < BLOCK_SIZE:
                blocks, remainder = divmod(tarinfo.size, tarfile.BLOCKSIZE)
                if remainder > 0:
                    tar.fileobj.write(tarfile.NUL *
                                      (tarfile.BLOCKSIZE - remainder))
                    blocks += 1
                tar.offset += blocks * tarfile.BLOCKSIZE
                break
        f.close()
        md5 = hash_md5.hexdigest()
    else:
        md5 = None
    size = tarinfo.size
    mtime = datetime.utcfromtimestamp(tarinfo.mtime)
    return offset, size, mtime, md5


# Put file to hpss
def hpss_put(hpss, file, keep=True):

    logging.info('Transferring file to HPSS: %s' % (file))
    path, name = os.path.split(file)

    # Need to be in local directory for hsi put to work
    cwd = os.getcwd()
    if path != '':
        os.chdir(path)

    # Transfer file using hsi put
    cmd = 'hsi -q "cd %s; put %s"' % (hpss, name)
    p1 = Popen(shlex.split(cmd), stdout=PIPE, stderr=PIPE)
    (stdout, stderr) = p1.communicate()
    status = p1.returncode
    if status != 0:
        logging.error('Transferring file to HPSS: %s' % (name))
        logging.debug('stdout:\n%s', stdout)
        logging.debug('stderr:\n%s', stderr)
        raise Exception

    # Back to original working directory
    if path != '':
        os.chdir(cwd)

    # Remove local file if requested
    if not keep:
        os.remove(file)


# Get file from hpss
def hpss_get(hpss, file):

    logging.info('Transferring from HPSS: %s' % (file))
    path, name = os.path.split(file)

    # Need to be in local directory for hsi get to work
    cwd = os.getcwd()
    if path != '':
        if not os.path.isdir(path):
            os.makedirs(path)
        os.chdir(path)

    # Transfer file using hsi get
    cmd = 'hsi -q "cd %s; get %s"' % (hpss, name)
    p1 = Popen(shlex.split(cmd), stdout=PIPE, stderr=PIPE)
    (stdout, stderr) = p1.communicate()
    status = p1.returncode
    if status != 0:
        logging.error('Transferring file from HPSS: %s' % (name))
        logging.debug('stdout:\n%s', stdout)
        logging.debug('stderr:\n%s', stderr)
        raise Exception

    # Back to original working directory
    if path != '':
        os.chdir(cwd)


def extractFiles(files):

    failures = []
    tfname = None
    newtar = True
    nfiles = len(files)
    for i in range(nfiles):

        file = files[i]

        # Open new tar archive
        if newtar:
            newtar = False
            tfname = os.path.join(CACHE, file[5])
            if not os.path.exists(tfname):
                # will need to retrieve from HPSS
                hpss_get(config.hpss, tfname)
            logging.info('Opening tar archive %s' % (tfname))
            tar = tarfile.open(tfname, "r")

        # Extract file
        logging.info('Extracting %s' % (file[1]))
        try:

            # Seek file position
            tar.fileobj.seek(file[6])

            # Get next member
            tarinfo = tar.tarinfo.fromtarfile(tar)

            if tarinfo.isfile():
                # fileobj to extract
                fin = tar.extractfile(tarinfo)
                fname = tarinfo.name
                path, name = os.path.split(fname)
                if path != '':
                    if not os.path.isdir(path):
                        os.makedirs(path)
                fout = open(fname, 'w')
                hash_md5 = hashlib.md5()
                while True:
                    s = fin.read(BLOCK_SIZE)
                    if len(s) > 0:
                        fout.write(s)
                        hash_md5.update(s)
                    if len(s) < BLOCK_SIZE:
                        break
                fin.close()
                fout.close()
                md5 = hash_md5.hexdigest()
                tar.chown(tarinfo, fname)
                tar.chmod(tarinfo, fname)
                tar.utime(tarinfo, fname)
                # Verify size
                if os.path.getsize(fname) != file[2]:
                    logging.error('size mismatch for: %s' % (fname))
                # Verify md5 checksum
                if md5 != file[4]:
                    logging.error('md5 mismatch for: %s' % (fname))
                    logging.error('md5 of extracted file: %s' % (md5))
                    logging.error('md5 of original file:  %s' % (file[4]))
                else:
                    logging.debug('Valid md5: %s %s' % (md5,fname))

            else:
                tar.extract(tarinfo)
                # Note: tar.extract() will not restore time stamps of symbolic
                # links. Could not find a Python-way to restore it either, so
                # relying here on 'touch'. This is not the prettiest solution.
                # Maybe a better one can be implemented later.
                if tarinfo.issym():
                    tmp1 = tarinfo.mtime
                    tmp2 = datetime.fromtimestamp(tmp1)
                    tmp3 = tmp2.strftime("%Y%m%d%H%M.%S")
                    os.system('touch -h -t %s %s' % (tmp3,tarinfo.name))

        except:
            logging.error('Retrieving %s' % (file[1]))

        # Close current archive?
        if (i == nfiles-1 or files[i][5] != files[i+1][5]):

            # Close current archive file
            logging.debug('Closing tar archive %s' % (tfname))
            tar.close()

            # Open new archive next time
            newtar = True


def extract():
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
    if not os.path.exists(DB_FILENAME):
        # will need to retrieve from HPSS
        if args.hpss is not None:
            config.hpss = args.hpss
            hpss_get(config.hpss, DB_FILENAME)
        else:
            logging.error('--hpss argument is required when local copy of '
                          'database is unavailable')
            raise Exception
    global con, cur
    con = sqlite3.connect(DB_FILENAME, detect_types=sqlite3.PARSE_DECLTYPES)
    cur = con.cursor()

    # Retrieve configuration from database
    for attr in dir(config):
        value = getattr(config, attr)
        if not callable(value) and not attr.startswith("__"):
            cur.execute(u"select value from config where arg=?", (attr,))
            value = cur.fetchone()[0]
            setattr(config, attr, value)
    config.maxsize = int(config.maxsize)

    # Find matching files
    matches = []
    for file in args.files:
        cur.execute(u"select * from files where name GLOB ?", (file,))
        matches = matches + cur.fetchall()

    # Remove duplicates
    matches = list(set(matches))

    # Sort by tape and order within tapes (offset)
    matches = sorted(matches, key=lambda x: (x[5], x[6]))

    # Retrieve from tapes
    extractFiles(matches)

    # Close database
    logging.debug('Closing index database')
    con.close()


if __name__ == '__main__':
    main()
