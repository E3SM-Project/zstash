import argparse
import hashlib
import logging
import os.path
import sqlite3
import sys
import tarfile

from datetime import datetime

from hpss import hpss_get
from settings import config, CACHE, BLOCK_SIZE, DB_FILENAME


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
                    logging.debug('Valid md5: %s %s' % (md5, fname))

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
                    os.system('touch -h -t %s %s' % (tmp3, tarinfo.name))

        except:
            logging.error('Retrieving %s' % (file[1]))

        # Close current archive?
        if (i == nfiles-1 or files[i][5] != files[i+1][5]):

            # Close current archive file
            logging.debug('Closing tar archive %s' % (tfname))
            tar.close()

            # Open new archive next time
            newtar = True
