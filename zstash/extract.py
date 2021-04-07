from __future__ import absolute_import, print_function

import argparse
import collections
import hashlib
import heapq
import logging
import multiprocessing
import os.path
import sqlite3
import sys
import tarfile
import traceback
from datetime import datetime
from typing import DefaultDict, List, Optional, Tuple

import _hashlib
import _io

from . import parallel
from .hpss import hpss_get
from .settings import (
    BLOCK_SIZE,
    DEFAULT_CACHE,
    TIME_TOL,
    FilesRow,
    FilesRowOptionalHash,
    config,
    get_db_filename,
    logger,
)
from .utils import update_config


def extract(keep_files: bool = True):
    """
    Given an HPSS path in the zstash database or passed via the command line,
    extract the archived data based on the file pattern (if given).
    """
    args: argparse.Namespace
    cache: str
    args, cache = setup_extract()

    failures: List[FilesRowOptionalHash] = extract_database(args, cache, keep_files)

    if failures:
        logger.error("Encountered an error for files:")

        for fail in failures:
            # (The `files` table has columns id, name, size, mtime, md5, tar, offset.
            # So, fail[1] is the name and fail[5] is the tar.)
            logger.error("{} in {}".format(fail[1], fail[5]))

        # (The `files` table has columns id, name, size, mtime, md5, tar, offset.
        # So, fail[5] is the tar.)
        broken_tars: List[str] = sorted(set([f[5] for f in failures]))

        logger.error("The following tar archives had errors:")
        for tar in broken_tars:
            logger.error(tar)
    else:
        verb: str = "extracting" if keep_files else "checking"
        logger.info("No failures detected when {} the files.".format(verb))


def setup_extract() -> Tuple[argparse.Namespace, str]:
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        usage="zstash extract [<args>] [files]",
        description="Extract files from existing archive",
    )
    optional: argparse._ArgumentGroup = parser.add_argument_group(
        "optional named arguments"
    )
    optional.add_argument(
        "--hpss",
        type=str,
        help='path to storage on HPSS. Set to "none" for local archiving. Must be set to "none" if the machine does not have HPSS access.',
    )
    optional.add_argument(
        "--workers", type=int, default=1, help="num of multiprocess workers"
    )
    optional.add_argument(
        "--keep",
        action="store_true",
        help='if --hpss is not "none", keep the downloaded tar files in the local archive (cache) after file extraction. Default is to delete the tar files. If --hpss=none, this flag has no effect.',
    )
    optional.add_argument(
        "--cache",
        type=str,
        help='path to the zstash archive on the local file system. The default name is "zstash".',
    )
    optional.add_argument(
        "-v", "--verbose", action="store_true", help="increase output verbosity"
    )
    parser.add_argument("files", nargs="*", default=["*"])
    args: argparse.Namespace = parser.parse_args(sys.argv[2:])
    if args.hpss and args.hpss.lower() == "none":
        args.hpss = "none"
    if args.cache:
        cache = args.cache
    else:
        cache = DEFAULT_CACHE
    # Note: setting logging level to anything other than DEBUG doesn't work with
    # multiple workers. This must have someting to do with the custom logger
    # implemented for multiple workers.
    if args.verbose or args.workers > 1:
        logger.setLevel(logging.DEBUG)

    return args, cache


def extract_database(
    args: argparse.Namespace, cache: str, keep_files: bool
) -> List[FilesRowOptionalHash]:

    # Open database
    logger.debug("Opening index database")
    if not os.path.exists(get_db_filename(cache)):
        # Will need to retrieve from HPSS
        if args.hpss is not None:
            config.hpss = args.hpss
            if config.hpss is not None:
                hpss: str = config.hpss
            else:
                raise Exception("Invalid config.hpss={}".format(config.hpss))
            hpss_get(hpss, get_db_filename(cache), cache)
        else:
            error_str: str = (
                "--hpss argument is required when local copy of database is unavailable"
            )
            logger.error(error_str)

            raise Exception(error_str)
    con: sqlite3.Connection = sqlite3.connect(
        get_db_filename(cache), detect_types=sqlite3.PARSE_DECLTYPES
    )
    cur: sqlite3.Cursor = con.cursor()

    update_config(cur)
    if config.maxsize is not None:
        maxsize = config.maxsize
    else:
        raise Exception("Invalid config.maxsize={}".format(config.maxsize))
    config.maxsize = int(maxsize)
    if config.keep is not None:
        keep = config.keep
    else:
        raise Exception("Invalid config.keep={}".format(config.keep))
    config.keep = bool(int(keep))

    # The command line arg should always have precedence
    if args.hpss is not None:
        config.hpss = args.hpss
    if config.hpss == "none":
        # If no HPSS is available, always keep the files.
        config.keep = True
    else:
        config.keep = args.keep

    # Start doing actual work
    cmd: str = "extract" if keep_files else "check"

    logger.debug("Running zstash " + cmd)
    logger.debug("Local path : {}".format(config.path))
    logger.debug("HPSS path  : {}".format(config.hpss))
    logger.debug("Max size  : {}".format(config.maxsize))
    logger.debug("Keep local tar files  : {}".format(config.keep))

    # Find matching files
    matches: List[FilesRow] = []
    for args_file in args.files:
        cur.execute(
            u"select * from files where name GLOB ? or tar GLOB ?",
            (args_file, args_file),
        )
        matches = matches + cur.fetchall()

    # Sort by the filename, tape (so the tar archive),
    # and order within tapes (offset).
    # (The `files` table has columns id, name, size, mtime, md5, tar, offset.
    # So, x[1] is the name, x[5] is the tar and x[6] is the offset.)
    matches.sort(key=lambda x: (x[1], x[5], x[6]))

    # Based off the filenames, keep only the last instance of a file.
    # This is because we may have different versions of the
    # same file across many tars.
    insert_idx: int
    iter_inx: int
    insert_idx, iter_idx = 0, 1
    for iter_idx in range(1, len(matches)):
        # If the filenames are unique, just increment insert_idx.
        # iter_idx will increment after this iteration.
        # (matches[x][1] is the name.)
        if matches[insert_idx][1] != matches[iter_idx][1]:
            insert_idx += 1
        # Always copy over the value at the correct location.
        matches[insert_idx] = matches[iter_idx]

    # `matches` will only be as long as the number of unique filenames
    matches = matches[: insert_idx + 1]

    # Sort by tape and offset, so that we make sure
    # that extract the files by tape order.
    # (The `files` table has columns id, name, size, mtime, md5, tar, offset.
    # So, x[5] is the tar and x[6] is the offset.)
    matches.sort(key=lambda x: (x[5], x[6]))

    # Retrieve from tapes
    failures: List[FilesRowOptionalHash]
    if args.workers > 1:
        logger.debug("Running zstash {} with multiprocessing".format(cmd))
        failures = multiprocess_extract(
            args.workers, matches, keep_files, config.keep, cache
        )
    else:
        failures = extractFiles(matches, keep_files, config.keep, cache)

    # Close database
    logger.debug("Closing index database")
    con.close()

    return failures


def multiprocess_extract(
    num_workers: int,
    matches: List[FilesRow],
    keep_files: bool,
    keep_tars: Optional[bool],
    cache: str,
) -> List[FilesRowOptionalHash]:
    """
    Extract the files from the matches in parallel.

    A single unit of work is a tar and all of
    the files in it to extract.
    """
    # A dict of tar -> size of files in it.
    # This is because we're trying to balance the load between
    # the processes.
    tar_to_size_unsorted: DefaultDict[str, float] = collections.defaultdict(float)
    tar: str
    size: int
    for db_row in matches:
        # (The `files` table has columns id, name, size, mtime, md5, tar, offset.
        # So, db_row[5] is the tar and db_row[2] is the size.)
        tar, size = db_row[5], db_row[2]
        tar_to_size_unsorted[tar] += size
    # Sort by the size.
    # (The `files` table has columns id, name, size, mtime, md5, tar, offset.
    # So, x[1] is the name.)
    tar_to_size: collections.OrderedDict[str, float] = collections.OrderedDict(
        sorted(tar_to_size_unsorted.items(), key=lambda x: x[1])
    )

    # We don't want to instantiate more processes than we need to.
    # So, if the number of tars is less than the number of workers,
    # set the number of workers to the number of tars.
    num_workers = min(num_workers, len(tar_to_size))

    # For worker i, workers_to_tars[i] is a set of tars
    # that worker i will work on.
    workers_to_tars: List[set] = [set() for _ in range(num_workers)]
    # A min heap, of (work, worker_idx) tuples, work is the size of data
    # that worker_idx needs to work on.
    # We can efficiently get the worker with the least amount of work.
    work_to_workers: List[Tuple[int, int]] = [(0, i) for i in range(num_workers)]
    heapq.heapify(workers_to_tars)

    # Using a greedy approach, populate workers_to_tars.
    for _, tar in enumerate(tar_to_size):
        # The worker with the least work should get the current largest amount of work.
        workers_work: int
        worker_idx: int
        workers_work, worker_idx = heapq.heappop(work_to_workers)
        workers_to_tars[worker_idx].add(tar)
        # Add this worker back to the heap, with the new amount of work.
        worker_tuple: Tuple[float, int] = (workers_work + tar_to_size[tar], worker_idx)
        # FIXME: error: Cannot infer type argument 1 of "heappush"
        heapq.heappush(work_to_workers, worker_tuple)  # type: ignore

    # For worker i, workers_to_matches[i] is a list of
    # matches from the database for it to process.
    workers_to_matches: List[List[FilesRow]] = [[] for _ in range(num_workers)]
    for db_row in matches:
        # (The `files` table has columns id, name, size, mtime, md5, tar, offset.
        # So, db_row[5] is the tar.)
        tar = db_row[5]
        workers_idx: int
        for worker_idx in range(len(workers_to_tars)):
            if tar in workers_to_tars[worker_idx]:
                # This worker gets this db_row.
                workers_to_matches[worker_idx].append(db_row)

    tar_ordering: List[str] = sorted([tar for tar in tar_to_size])
    monitor: parallel.PrintMonitor = parallel.PrintMonitor(tar_ordering)

    # The return value for extractFiles will be added here.
    failure_queue: multiprocessing.Queue[FilesRowOptionalHash] = multiprocessing.Queue()
    processes: List[multiprocessing.Process] = []
    for matches in workers_to_matches:
        # (The `files` table has columns id, name, size, mtime, md5, tar, offset.
        # So, db_row[5] is the tar.)
        tars_for_this_worker: List[str] = list(set(match[5] for match in matches))
        worker: parallel.ExtractWorker = parallel.ExtractWorker(
            monitor, tars_for_this_worker, failure_queue
        )
        process: multiprocessing.Process = multiprocessing.Process(
            target=extractFiles, args=(matches, keep_files, keep_tars, cache, worker)
        )
        process.start()
        processes.append(process)

    # While the processes are running, we need to empty the queue.
    # Otherwise, it causes hanging.
    # No need to join() each of the processes when doing this,
    # because we'll be in this loop until completion.
    failures: List[FilesRowOptionalHash] = []
    while any(p.is_alive() for p in processes):
        while not failure_queue.empty():
            failures.append(failure_queue.get())

    # Sort the failures, since they can come in at any order.
    failures.sort(key=lambda x: (x[1], x[5], x[6]))
    return failures


# TODO: need to improve readability for this function, should_extract_file, and anything involving parallel
# C901 'extractFiles' is too complex (33)
def extractFiles(  # noqa: C901
    files: List[FilesRow],
    keep_files: bool,
    keep_tars: Optional[bool],
    cache: str,
    multiprocess_worker: Optional[parallel.ExtractWorker] = None,
) -> List[FilesRowOptionalHash]:
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
    the contents of what's in its print queue.
    """
    failures: List[FilesRowOptionalHash] = []
    tfname: Optional[str] = None
    newtar: bool = True
    nfiles: int = len(files)
    if multiprocess_worker:
        # All messages to the logger will now be sent to
        # this queue, instead of sys.stdout.
        # error: Argument 1 to "StreamHandler" has incompatible type "PrintQueue"; expected "Optional[IO[str]]"
        sh = logging.StreamHandler(multiprocess_worker.print_queue)  # type: ignore
        sh.setLevel(logging.DEBUG)
        formatter: logging.Formatter = logging.Formatter("%(levelname)s: %(message)s")
        sh.setFormatter(formatter)
        logger.addHandler(sh)
        # Don't have the logger print to the console as the message come in.
        logger.propagate = False

    for i in range(nfiles):
        # The current structure of each of the db row, `file`, is:
        # (id, name, size, mtime, md5, tar, offset)
        file_tuple: FilesRowOptionalHash = files[i]

        # Open new tar archive
        if newtar:
            newtar = False
            tfname = os.path.join(cache, file_tuple[5])
            # Everytime we're extracting a new tar, if running in parallel,
            # let the process know.
            # This is to synchronize the print statements.
            if multiprocess_worker:
                multiprocess_worker.set_curr_tar(file_tuple[5])

            if not os.path.exists(tfname):
                # Will need to retrieve from HPSS
                if config.hpss is not None:
                    hpss: str = config.hpss
                else:
                    raise Exception("Invalid config.hpss={}".format(config.hpss))
                hpss_get(hpss, tfname, cache)

            logger.info("Opening tar archive %s" % (tfname))
            tar: tarfile.TarFile = tarfile.open(tfname, "r")

        # Extract file
        cmd: str = "Extracting" if keep_files else "Checking"
        logger.info(cmd + " %s" % (file_tuple[1]))
        # if multiprocess_worker:
        #     print('{} is {} {} from {}'.format(multiprocess_worker, cmd, file[1], file[5]))

        if keep_files and not should_extract_file(file_tuple):
            # If we were going to extract, but aren't
            # because a matching file is on disk
            msg: str = "Not extracting {}, because it"
            msg += " already exists on disk with the same"
            msg += " size and modification date."
            # (The `files` table has columns id, name, size, mtime, md5, tar, offset.
            # So, file_tuple[1] is the name.)
            logger.info(msg.format(file_tuple[1]))

        # True if we should actually extract the file from the tar
        extract_this_file: bool = keep_files and should_extract_file(file_tuple)

        try:
            # Seek file position
            if tar.fileobj is not None:
                fileobj: _io.BufferedReader = tar.fileobj
            else:
                raise Exception("Invalid tar.fileobj={}".format(tar.fileobj))
            # (The `files` table has columns id, name, size, mtime, md5, tar, offset.
            # So, file_tuple[6] is the offset.)
            fileobj.seek(file_tuple[6])

            # Get next member
            tarinfo: tarfile.TarInfo = tar.tarinfo.fromtarfile(tar)

            if tarinfo.isfile():
                # fileobj to extract
                # error: Name 'tarfile.ExFileObject' is not defined
                extracted_file: Optional[tarfile.ExFileObject] = tar.extractfile(tarinfo)  # type: ignore
                if extracted_file:
                    # error: Name 'tarfile.ExFileObject' is not defined
                    fin: tarfile.ExFileObject = extracted_file  # type: ignore
                else:
                    raise Exception("Invalid extracted_file={}".format(extracted_file))
                try:
                    fname: str = tarinfo.name
                    path: str
                    name: str
                    path, name = os.path.split(fname)
                    if path != "" and extract_this_file:
                        if not os.path.isdir(path):
                            # The path doesn't exist, so create it.
                            os.makedirs(path)
                    if extract_this_file:
                        # If we're keeping the files,
                        # then have an output file
                        fout: _io.BufferedWriter = open(fname, "wb")

                    hash_md5: _hashlib.HASH = hashlib.md5()
                    while True:
                        s: bytes = fin.read(BLOCK_SIZE)
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

                md5: str = hash_md5.hexdigest()
                if extract_this_file:
                    # numeric_owner is a required arg in Python 3.
                    # If True, "only the numbers for user/group names
                    # are used and not the names".
                    tar.chown(tarinfo, fname, numeric_owner=False)
                    tar.chmod(tarinfo, fname)
                    tar.utime(tarinfo, fname)
                    # Verify size
                    if os.path.getsize(fname) != file_tuple[2]:
                        logger.error("size mismatch for: %s" % (fname))

                # Verify md5 checksum
                # (The `files` table has columns id, name, size, mtime, md5, tar, offset.
                # So, file_tuple[4] is the md5 hash.)
                if md5 != file_tuple[4]:
                    logger.error("md5 mismatch for: {}".format(fname))
                    logger.error("md5 of extracted file: {}".format(md5))
                    logger.error("md5 of original file:  {}".format(file_tuple[4]))

                    failures.append(file_tuple)
                else:
                    logger.debug("Valid md5: {} {}".format(md5, fname))

            elif extract_this_file:
                tar.extract(tarinfo)
                # Note: tar.extract() will not restore time stamps of symbolic
                # links. Could not find a Python-way to restore it either, so
                # relying here on 'touch'. This is not the prettiest solution.
                # Maybe a better one can be implemented later.
                if tarinfo.issym():
                    tmp1: int = tarinfo.mtime
                    tmp2: datetime = datetime.fromtimestamp(tmp1)
                    tmp3: str = tmp2.strftime("%Y%m%d%H%M.%S")
                    os.system("touch -h -t %s %s" % (tmp3, tarinfo.name))

        except Exception:
            traceback.print_exc()
            logger.error("Retrieving %s" % (file_tuple[1]))
            failures.append(file_tuple)

        if multiprocess_worker:
            multiprocess_worker.print_contents()

        # Close current archive?
        if i == nfiles - 1 or files[i][5] != files[i + 1][5]:
            # We're either on the last file or the tar is distinct from the tar of the next file.
            # (The `files` table has columns id, name, size, mtime, md5, tar, offset.
            # So, files[i][5] is the tar.)

            # Close current archive file
            logger.debug("Closing tar archive {}".format(tfname))
            tar.close()

            if multiprocess_worker:
                multiprocess_worker.done_enqueuing_output_for_tar(file_tuple[5])

            # Open new archive next time
            newtar = True

            # Delete this tar if the corresponding command-line arg was used.
            if not keep_tars:
                if tfname is not None:
                    os.remove(tfname)
                else:
                    raise Exception("Invalid tfname={}".format(tfname))

    if multiprocess_worker:
        # If there are things left to print, print them.
        multiprocess_worker.print_all_contents()

        # Add the failures to the queue.
        # When running with multiprocessing, the function multiprocess_extract()
        # that calls this extractFiles() function will return the failures as a list.
        for f in failures:
            multiprocess_worker.failure_queue.put(f)
    return failures


def should_extract_file(db_row: FilesRowOptionalHash) -> bool:
    """
    If a file is on disk already with the correct
    timestamp and size, don't extract the file.
    """
    # (The `files` table has columns id, name, size, mtime, md5, tar, offset.
    # So, db_row[1] is the name, db_row[2] is the size, db_row[3] is the mtime.
    file_name: str
    size_db: int
    mod_time_db: datetime
    file_name, size_db, mod_time_db = db_row[1], db_row[2], db_row[3]

    if not os.path.exists(file_name):
        # The file doesn't exist locally.
        # We must get files that are not on disk.
        return True

    size_disk: int = os.path.getsize(file_name)
    mod_time_disk: datetime = datetime.utcfromtimestamp(os.path.getmtime(file_name))

    # Only extract when the times and sizes are not the same (within tolerance)
    # We have a TIME_TOL because mod_time_disk doesn't have the microseconds.
    return not (
        (size_disk == size_db)
        and (abs(mod_time_disk - mod_time_db).total_seconds() < TIME_TOL)
    )
