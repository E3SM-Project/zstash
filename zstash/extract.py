import argparse
import hashlib
import logging
import os.path
import sqlite3
import sys
import tarfile
import traceback
from datetime import datetime
from hpss import hpss_get
from settings import config, CACHE, BLOCK_SIZE, DB_FILENAME


def extract(keep_files=True):
    """
    Given an HPSS path in the zstash database or passed via the command line,
    extract the archived data based on the file pattern (if given).
    """
    parser = argparse.ArgumentParser(
        usage='zstash extract [<args>] [files]',
        description='Extract files from existing archive')
    optional = parser.add_argument_group('optional named arguments')
    optional.add_argument('--hpss', type=str, help='path to HPSS storage')
    parser.add_argument('files', nargs='*', default=['*'])
    args = parser.parse_args(sys.argv[2:])

    # Open database
    logging.debug('Opening index database')
    if not os.path.exists(DB_FILENAME):
        # Will need to retrieve from HPSS
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
    config.keep = bool(int(config.keep))
    # The command line arg should always have precedence
    if args.hpss is not None:
        config.hpss = args.hpss

    # Start doing actual work
    cmd = 'extract' if keep_files else 'check'
    logging.debug('Running zstash ' + cmd)
    logging.debug('Local path : %s' % (config.path))
    logging.debug('HPSS path  : %s' % (config.hpss))
    logging.debug('Max size  : %i' % (config.maxsize))
    logging.debug('Keep local tar files  : %s' % (config.keep))

    # Find matching files
    matches = []
    for file in args.files:
        cur.execute(u"select * from files where name GLOB ? or tar GLOB ?", (file, file))
        matches = matches + cur.fetchall()

    # Remove duplicates
    matches = list(set(matches))

    # Sort by tape and order within tapes (offset)
    matches = sorted(matches, key=lambda x: (x[5], x[6]))

    # Retrieve from tapes
    failures = extractFiles(matches, keep_files)

    # Close database
    logging.debug('Closing index database')
    con.close()

    if failures:
        logging.error('Encountered an error for files:')
        for fail in failures:
            logging.error('{} in {}'.format(fail[1], fail[5]))

        broken_tars = set(sorted([f[5] for f in failures]))
        logging.error('The following tar archives had errors:')
        for tar in broken_tars:
            logging.error(tar)


def extractFiles(files, keep_files):
    """
    Given a list of database rows, extract the files from the
    tar archives to the current location on disk.

    If keep_files is False, the files are not extracted.
    This is used for when checking if the files in an HPSS
    repository are valid.
    """
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
        cmd = 'Extracting' if keep_files else 'Checking'
        logging.info(cmd + ' %s' % (file[1]))
        try:

            # Seek file position
            tar.fileobj.seek(file[6])

            # Get next member
            tarinfo = tar.tarinfo.fromtarfile(tar)

            if tarinfo.isfile():
                # fileobj to extract
                try:
                    fin = tar.extractfile(tarinfo)
                    fname = tarinfo.name
                    path, name = os.path.split(fname)
                    if path != '':
                        if not os.path.isdir(path):
                            os.makedirs(path)
                    if keep_files:
                        fout = open(fname, 'w')

                    hash_md5 = hashlib.md5()
                    while True:
                        s = fin.read(BLOCK_SIZE)
                        if len(s) > 0:
                            hash_md5.update(s)
                            if keep_files:
                                fout.write(s)
                        if len(s) < BLOCK_SIZE:
                            break
                finally:
                    fin.close()
                    if keep_files:
                        fout.close()

                md5 = hash_md5.hexdigest()
                if keep_files:
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
                    failures.append(file)
                else:
                    logging.debug('Valid md5: %s %s' % (md5, fname))

            elif keep_files:
                tar.extract(tarinfo)
                # Note: tar.extract() will not restore time stamps of symbolic
                # links. Could not find a Python-way to restore it either, so
                # relying here on 'touch'. This is not the prettiest solution.
                # Maybe a better one can be implemented later.
                if tarinfo.issym():
                    tmp1 = tarinfo.mtime
                    tmp2 = datetime.fromtimestamp(tmp1)
                    tmp3 = tmp2.strftime("%Y%m%d%H%M.%S")
                    os.system('touch -h -t %s %s' % (tmp3, tarinfo.name))

        except:
            traceback.print_exc()
            logging.error('Retrieving %s' % (file[1]))
            failures.append(file)

        # Close current archive?
        if (i == nfiles-1 or files[i][5] != files[i+1][5]):

            # Close current archive file
            logging.debug('Closing tar archive %s' % (tfname))
            tar.close()

            # Open new archive next time
            newtar = True

    return failures
