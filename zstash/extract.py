from __future__ import print_function, absolute_import

import argparse
import hashlib
import logging
import os.path
import sqlite3
import sys
import tarfile
import traceback
import collections
import heapq
import multiprocessing
from datetime import datetime
from .hpss import hpss_get
from .settings import config, CACHE, BLOCK_SIZE, DB_FILENAME, TIME_TOL, logger
from . import parallel


def multiprocess_extract(num_workers, matches, keep_files, keep_tars):
    """
    Extract the files from the matches in parallel.

    A single unit of work is a tar and all of
    the files in it to extract.
    """
    # A dict of tar -> size of files in it.
    # This is because we're trying to balance the load between
    # the processes.
    tar_to_size = collections.defaultdict(float)
    for db_row in matches:
        tar, size = db_row[5], db_row[2]
        tar_to_size[tar] += size
    # Sort by the size.
    tar_to_size = collections.OrderedDict(sorted(tar_to_size.items(), key=lambda x: x[1]))

    # We don't want to instantiate more processes than we need to.
    num_workers = min(num_workers, len(tar_to_size))

    # For worker i, workers_to_tars[i] is a set of tars
    # that worker i will work on.
    workers_to_tars = [set() for _ in range(num_workers)]
    # A min heap, of (work, worker_idx) tuples, work is the size of data
    # that worker_idx needs to work on.
    # We can efficiently get the worker with the least amount of work.
    work_to_workers = [(0, i) for i in range(num_workers)]
    heapq.heapify(workers_to_tars)

    # Using a greedy approach, populate workers_to_tars.
    for _, tar in enumerate(tar_to_size):
        # The worker with the least work should get the current largest amount of work.
        workers_work, worker_idx = heapq.heappop(work_to_workers)
        workers_to_tars[worker_idx].add(tar)
        # Add this worker back to the heap, with the new amount of work.
        heapq.heappush(work_to_workers, (workers_work+tar_to_size[tar], worker_idx))

    # For worker i, workers_to_matches[i] is a list of 
    # matches from the database for it to process.
    workers_to_matches = [[] for _ in range(num_workers)]
    for db_row in matches:
        tar = db_row[5]
        for worker_idx in range(len(workers_to_tars)):
            if tar in workers_to_tars[worker_idx]:
                # This worker gets this db_row.
                workers_to_matches[worker_idx].append(db_row)
    
    tar_ordering = sorted([tar for tar in tar_to_size])
    monitor = parallel.PrintMonitor(tar_ordering)

    # The return value for extractFiles will be added here.
    failure_queue = multiprocessing.Queue()
    processes = []
    for matches in workers_to_matches:
        tars_for_this_worker = list(set(match[5] for match in matches))
        worker = parallel.ExtractWorker(monitor, tars_for_this_worker, failure_queue)
        process = multiprocessing.Process(target=extractFiles,
                    args=(matches, keep_files, keep_tars, worker))
        process.start()
        processes.append(process)

    # While the processes are running, we need to empty the queue.
    # Otherwise, it causes hanging.
    # No need to join() each of the processes when doing this,
    # cause we'll be in this loop until completion.
    failures = []
    while any(p.is_alive() for p in processes):
        while not failure_queue.empty():
            failures.append(failure_queue.get())

    # Sort the failures, since they can come in at any order.
    failures.sort(key=lambda x: (x[1], x[5], x[6]))
    return failures

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
    optional.add_argument('--workers', type=int, default=1, help='num of multiprocess workers')
    optional.add_argument('--keep', action='store_true', help='keep tar files in local cache (default off)')
    optional.add_argument('-v', '--verbose', action="store_true", 
                          help="increase output verbosity")
    parser.add_argument('files', nargs='*', default=['*'])
    args = parser.parse_args(sys.argv[2:])
    if args.hpss and args.hpss.lower() == 'none':
        args.hpss = 'none'
    # Note: setting logging level to anything other than DEBUG doesn't work with 
    # multiple workers. This must have someting to do with the custom logger 
    # implemented for multiple workers.
    if args.verbose or args.workers > 1: logger.setLevel(logging.DEBUG)

    # Open database
    logger.debug('Opening index database')
    if not os.path.exists(DB_FILENAME):
        # Will need to retrieve from HPSS
        if args.hpss is not None:
            config.hpss = args.hpss
            hpss_get(config.hpss, DB_FILENAME)
        else:
            logger.error('--hpss argument is required when local copy of '
                          'database is unavailable')

            raise Exception
    global con, cur
    con = sqlite3.connect(DB_FILENAME, detect_types=sqlite3.PARSE_DECLTYPES)
    cur = con.cursor()

    # Retrieve some configuration settings from database
    for attr in dir(config):
        value = getattr(config, attr)
        if not callable(value) and not attr.startswith("__"):
            cur.execute(u"select value from config where arg=?", (attr,))
            value = cur.fetchone()[0]
            setattr(config, attr, value)
    config.maxsize = int(config.maxsize)
    config.keep = bool(int(config.keep))

    # The command line arg should always have precedence
    if args.hpss == 'none':
        # If no HPSS is available, always keep the files.
        config.keep = True
    else:
        config.keep = args.keep
    if args.hpss is not None:
        config.hpss = args.hpss

    # Start doing actual work
    cmd = 'extract' if keep_files else 'check'

    logger.debug('Running zstash ' + cmd)
    logger.debug('Local path : %s' % (config.path))
    logger.debug('HPSS path  : %s' % (config.hpss))
    logger.debug('Max size  : %i' % (config.maxsize))
    logger.debug('Keep local tar files  : %s' % (config.keep))

    # Find matching files
    matches = []
    for file in args.files:
        cur.execute(u"select * from files where name GLOB ? or tar GLOB ?", (file, file))
        matches = matches + cur.fetchall()
    
    # Sort by the filename, tape (so the tar archive),
    # and order within tapes (offset).
    matches.sort(key=lambda x: (x[1], x[5], x[6]))

    # Based off the filenames, keep only the last instance of a file.
    # This is because we may have different versions of the
    # same file across many tars.
    insert_idx, iter_idx = 0, 1
    for iter_idx in range(1, len(matches)):
        # If the filenames are unique, just increment insert_idx. 
        # iter_idx will increment after this iteration.
        if matches[insert_idx][1] != matches[iter_idx][1]:
            insert_idx += 1
        # Always copy over the value at the correct location. 
        matches[insert_idx] = matches[iter_idx] 

    matches = matches[:insert_idx+1]

    # Sort by tape and offset, so that we make sure
    # that extract the files by tape order.
    matches.sort(key=lambda x: (x[5], x[6]))

    # Retrieve from tapes
    if args.workers > 1:
        logger.debug('Running zstash {} with multiprocessing'.format(cmd))
        failures = multiprocess_extract(args.workers, matches, keep_files, config.keep)
    else:
        failures = extractFiles(matches, keep_files, config.keep)

    # Close database
    logger.debug('Closing index database')
    con.close()

    if failures:
        logger.error('Encountered an error for files:')

        for fail in failures:
            logger.error('{} in {}'.format(fail[1], fail[5]))

        broken_tars = sorted(set([f[5] for f in failures]))

        logger.error('The following tar archives had errors:')
        for tar in broken_tars:
            logger.error(tar)
    else:
        verb = 'extracting' if keep_files else 'checking'
        logger.info('No failures detected when {} the files.'.format(verb))

def should_extract_file(db_row):
    """
    If a file is on disk already with the correct
    timestamp and size, don't extract the file.
    """
    file_name, size_db, mod_time_db = db_row[1], db_row[2], db_row[3]

    if not os.path.exists(file_name):
        # We must get files that are not on disk.
        return True
    
    size_disk = os.path.getsize(file_name)
    mod_time_disk = datetime.utcfromtimestamp(os.path.getmtime(file_name))

    # Only extract when the times and sizes are not the same. 
    # We have a TIME_TOL because mod_time_disk doesn't have the microseconds.
    return not(size_disk == size_db and abs(mod_time_disk - mod_time_db).total_seconds() < TIME_TOL)


def extractFiles(files, keep_files, keep_tars, multiprocess_worker=None):
    """
    Given a list of database rows, extract the files from the
    tar archives to the current location on disk.

    If keep_files is False, the files are not extracted.
    This is used for when checking if the files in an HPSS
    repository are valid.

    If keep_tars is True, the tar archives that are downloaded are kept,
    even after the program has terminated. Otherwise, they are deleted.

    If running in parallel, then multiprocess_worker is the Worker
    that called this function.
    We need a reference to it so we can signal it to print
    the contents of what's in it's print queue.
    """
    failures = []
    tfname = None
    newtar = True
    nfiles = len(files)
    if multiprocess_worker:
        # All messages to the logger will now be sent to
        # this queue, instead of sys.stdout.
        sh = logging.StreamHandler(multiprocess_worker.print_queue)
        sh.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(levelname)s: %(message)s')
        sh.setFormatter(formatter)
        logger.addHandler(sh)
        # Don't have the logger print to the console as the message come in.
        logger.propagate = False

    for i in range(nfiles):
        # The current structure of each of the db row, `file`, is:
        # (id, name, size, mtime, md5, tar, offset)
        file = files[i]

        # Open new tar archive
        if newtar:
            newtar = False
            tfname = os.path.join(CACHE, file[5])
            # Everytime we're extracting a new tar, if running in parallel,
            # let the process know.
            # This is to synchronize the print statements.
            if multiprocess_worker:
                multiprocess_worker.set_curr_tar(file[5])

            if not os.path.exists(tfname):
                # Will need to retrieve from HPSS
                hpss_get(config.hpss, tfname)

            logger.info('Opening tar archive %s' % (tfname))
            tar = tarfile.open(tfname, "r")

        # Extract file
        cmd = 'Extracting' if keep_files else 'Checking'
        logger.info(cmd + ' %s' % (file[1]))
        # if multiprocess_worker:
        #     print('{} is {} {} from {}'.format(multiprocess_worker, cmd, file[1], file[5]))

        if keep_files and not should_extract_file(file):
            # If we were going to extract, but aren't
            # because a matching file is on disk
            msg = 'Not extracting {}, because it'
            msg += ' already exists on disk with the same'
            msg += ' size and modification date.'
            logger.info(msg.format(file[1]))

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
                        fout = open(fname, 'wb')

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
                    # numeric_owner is a required arg in Python 3.
                    # If True, "only the numbers for user/group names
                    # are used and not the names".
                    tar.chown(tarinfo, fname, numeric_owner=False)
                    tar.chmod(tarinfo, fname)
                    tar.utime(tarinfo, fname)
                    # Verify size
                    if os.path.getsize(fname) != file[2]:
                        logger.error('size mismatch for: %s' % (fname))

                # Verify md5 checksum
                if md5 != file[4]:
                    logger.error('md5 mismatch for: %s' % (fname))
                    logger.error('md5 of extracted file: %s' % (md5))
                    logger.error('md5 of original file:  %s' % (file[4]))

                    failures.append(file)
                else:
                    logger.debug('Valid md5: %s %s' % (md5, fname))

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
            logger.error('Retrieving %s' % (file[1]))
            failures.append(file)

        if multiprocess_worker:
            multiprocess_worker.print_contents()

        # Close current archive?
        if (i == nfiles-1 or files[i][5] != files[i+1][5]):
            # Close current archive file
            logger.debug('Closing tar archive %s' % (tfname))
            tar.close()

            if multiprocess_worker:
                multiprocess_worker.done_enqueuing_output_for_tar(file[5])

            # Open new archive next time
            newtar = True
            
            # Delete this tar if the corresponding command-line arg was used.
            if not keep_tars:
                os.remove(tfname)

    if multiprocess_worker:
        # If there are stuff left to print, print them.
        multiprocess_worker.print_all_contents()

        # Add the failures to the queue.
        # When running with multiprocessing, the function multiprocess_extract()
        # that calls this extractFiles() function will return the failures as a list.
        for f in failures:
            multiprocess_worker.failure_queue.put(f)
    else:    
        return failures
