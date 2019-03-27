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


def should_extract_file(db_row):
    """
    If a file is on disk already with the correct
    timestamp and size, don't extract the file.
    """
    file_name, size, mod_date = db_row[1], db_row[2], db_row[3]
    '''
    print(file_name, size, mod_date)
    if os.path.exists(file_name):
        print('size on disk/size in db: {}/{}'.format(os.path.getsize(file_name), size))
        print('mod date on disk/size in db: {}/{}'.format(datetime.utcfromtimestamp(os.path.getmtime(file_name)), mod_date.replace(microsecond=0)))
        # print('mod date on disk/size in db: {}/{}'.format(datetime.utcfromtimestamp(os.stat(file_name).st_mtime_ns), mod_date.replace(microsecond=0)))
    '''

    # Compare the timestamps.
    is_same_time = os.path.exists(file_name) and datetime.utcfromtimestamp(os.path.getmtime(file_name)) == mod_date.replace(microsecond=0)
    # Compare the sizes.
    # The index.db stores the size of symlinks and dirs as 0, but os.path.getsize() gets them as 11 and 512.
    # So when we have a symlink or dir, don't bother checking for the size.
    # is_same_size = os.path.exists(file_name) and (os.path.islink(file_name) or os.path.isdir(file_name) or os.path.getsize(file_name) == size)
    is_same_size = os.path.exists(file_name) and os.path.getsize(file_name) == size

    return not(is_same_time and is_same_size)


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
        # The current structure of each of the db row, `file`, is:
        # (id, name, size, mtime, md5, tar, offset)
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

        if keep_files and not should_extract_file(file):
            # If we were going to extract, but aren't
            # because a matching file is on disk
            msg = 'Not extracting {}, because it'
            msg += ' already exists on disk with the same'
            msg += ' size and modification date.'
            logging.info(msg.format(file[1]))

        # True if we should actually extract the file from the tar
        extract_this_file = keep_files and should_extract_file(file)

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
                    if path != '' and extract_this_file:
                        if not os.path.isdir(path):
                            os.makedirs(path)
                    if extract_this_file:
                        # If we're keeping the files,
                        # then have an output file
                        fout = open(fname, 'w')

                    hash_md5 = hashlib.md5()
                    while True:
                        s = fin.read(BLOCK_SIZE)
                        if len(s) > 0:
                            hash_md5.update(s)
                            if extract_this_file:
                                fout.write(s)
                        if len(s) < BLOCK_SIZE:
                            break
                finally:
                    fin.close()
                    if extract_this_file:
                        fout.close()

                md5 = hash_md5.hexdigest()
                if extract_this_file:
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

            elif extract_this_file:
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
