from __future__ import absolute_import, print_function

import argparse
import collections
import hashlib
import logging
import multiprocessing
import os.path
import re
import sqlite3
import sys
import tarfile
import time
import traceback
from datetime import datetime
from typing import DefaultDict, Dict, List, Optional, Set, Tuple

import _hashlib
import _io

from . import parallel
from .hpss import hpss_get
from .settings import (
    BLOCK_SIZE,
    DEFAULT_CACHE,
    TIME_TOL,
    FilesRow,
    TupleFilesRow,
    config,
    get_db_filename,
    logger,
)
from .utils import tars_table_exists, update_config


def extract(keep_files: bool = True):
    """
    Given an HPSS path in the zstash database or passed via the command line,
    extract the archived data based on the file pattern (if given).
    """
    args: argparse.Namespace
    cache: str
    args, cache = setup_extract()

    failures: List[FilesRow] = extract_database(args, cache, keep_files)

    if failures:
        logger.error("Encountered an error for files:")

        for fail in failures:
            logger.error("{} in {}".format(fail.name, fail.tar))

        broken_tars: List[str] = sorted(set([f.tar for f in failures]))

        logger.error("The following tar archives had errors:")
        for tar in broken_tars:
            logger.error(tar)
    else:
        verb: str = "extracting" if keep_files else "checking"
        logger.info(
            'No failures detected when {} the files. If you have a log file, run "grep -i Exception <log-file>" to double check.'.format(
                verb
            )
        )


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
        help=(
            'path to storage on HPSS. Set to "none" for local archiving. It also can be a Globus URL, '
            'globus://<GLOBUS_ENDPOINT_UUID>/<PATH>. Names "alcf" and "nersc" are recognized as referring to the ALCF HPSS '
            "and NERSC HPSS endpoints, e.g. globus://nersc/~/my_archive."
        ),
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
        "--retries", type=int, default=1, help="number of times to retry an hsi command"
    )
    optional.add_argument("--tars", type=str, help="specify which tars to process")
    optional.add_argument(
        "-v", "--verbose", action="store_true", help="increase output verbosity"
    )
    optional.add_argument(
        "--error-on-duplicate-tar",
        action="store_true",
        help="FOR ADVANCED USERS ONLY: Raise an error if a tar file with the same name already exists in the database. If this flag is set, zstash will exit if it sees a duplicate tar. If it is not set, zstash will check if the size matches the *most recent* entry.",
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


def parse_tars_option(tars: str, first_tar: str, last_tar: str) -> List[str]:
    tar_str_list: List[str] = tars.split(",")
    tar_list: List[str] = []
    tar_str: str
    for tar_str in tar_str_list:
        if tar_str.startswith('"'):
            tar_str = tar_str[1:]
        if tar_str.endswith('"'):
            tar_str = tar_str[:-1]
        if tar_str.startswith("-"):
            tar_str = "{}{}".format(first_tar, tar_str)
        elif tar_str.endswith("-"):
            tar_str = "{}{}".format(tar_str, last_tar)
        m: Optional[re.Match]
        m = re.match("(.*)-(.*)", tar_str)
        if m:
            m1: str = m.group(1)
            m2: str = m.group(2)
            # Remove .tar suffix
            if m1.endswith(".tar"):
                m1 = m1[:-4]
            if m2.endswith(".tar"):
                m2 = m2[:-4]
            beginning_tar: int = int(m1, 16)
            ending_tar: int = int(m2, 16)
            t: int
            for t in range(beginning_tar, ending_tar + 1):
                tar_list.append("{:06x}".format(t))
        else:
            # Remove .tar suffix
            if tar_str.endswith(".tar"):
                tar_str = tar_str[:-4]
            tar_list.append(tar_str)
    # Remove duplicates and sort tar_list
    tar_list = sorted(list(set(tar_list)))
    return tar_list


def extract_database(
    args: argparse.Namespace, cache: str, keep_files: bool
) -> List[FilesRow]:

    # Open database
    logger.debug("Opening index database")
    if not os.path.exists(get_db_filename(cache)):
        # Will need to retrieve from HPSS
        if args.hpss is not None:
            config.hpss = args.hpss
            if config.hpss is not None:
                hpss: str = config.hpss
            else:
                raise TypeError("Invalid config.hpss={}".format(config.hpss))
            hpss_get(hpss, get_db_filename(cache), cache)
        else:
            error_str: str = (
                "--hpss argument is required when local copy of database is unavailable"
            )
            logger.error(error_str)

            raise ValueError(error_str)
    con: sqlite3.Connection = sqlite3.connect(
        get_db_filename(cache), detect_types=sqlite3.PARSE_DECLTYPES
    )
    cur: sqlite3.Cursor = con.cursor()

    update_config(cur)
    if config.maxsize is not None:
        maxsize = config.maxsize
    else:
        raise TypeError("Invalid config.maxsize={}".format(config.maxsize))
    config.maxsize = int(maxsize)

    # The command line arg should always have precedence
    if args.hpss is not None:
        config.hpss = args.hpss
    keep: bool
    if config.hpss == "none":
        # If no HPSS is available, always keep the files.
        keep = True
    else:
        keep = args.keep

    # Start doing actual work
    cmd: str = "extract" if keep_files else "check"

    logger.debug("Running zstash " + cmd)
    logger.debug("Local path : {}".format(config.path))
    logger.debug("HPSS path  : {}".format(config.hpss))
    logger.debug("Max size  : {}".format(config.maxsize))
    logger.debug("Keep local tar files : {}".format(keep))

    matches_: List[TupleFilesRow] = []
    if args.tars is not None:
        # Ignore default value for args.files ("*")
        if args.files != ["*"]:
            raise ValueError("If --tars is used, <files> should not be listed.")
        tar_names_initial: List[Tuple[str]] = cur.execute(
            "select distinct tar from files"
        ).fetchall()
        tar_names: List[str] = sorted([x for (x,) in tar_names_initial])
        # Remove `.tar` with `[:-4]` for `parse_tars_option` to work properly
        tar_list: List[str] = parse_tars_option(
            args.tars, tar_names[0][:-4], tar_names[-1][:-4]
        )
        for tar in tar_list:
            cur.execute(
                "select * from files where tar GLOB ?",
                (tar + ".tar",),
            )
            matches_ = matches_ + cur.fetchall()
    else:
        # Find matching files
        for args_file in args.files:
            cur.execute(
                "select * from files where name GLOB ? or tar GLOB ?",
                (args_file, args_file),
            )
            match: List[TupleFilesRow] = cur.fetchall()
            if match:
                matches_ = matches_ + match
            else:
                logger.info("No matches for {}".format(args_file))

    if matches_ == []:
        raise FileNotFoundError("There was nothing to extract.")

    matches: List[FilesRow] = list(map(lambda match: FilesRow(match), matches_))

    # Sort by the filename, tape (so the tar archive),
    # and order within tapes (offset).
    matches.sort(key=lambda t: (t.name, t.tar, t.offset))

    # Based off the filenames, keep only the last instance of a file.
    # This is because we may have different versions of the
    # same file across many tars.
    insert_idx: int
    iter_idx: int
    insert_idx, iter_idx = 0, 1
    for iter_idx in range(1, len(matches)):
        # If the filenames are unique, just increment insert_idx.
        # iter_idx will increment after this iteration.
        # (matches[x][1] is the name.)
        if matches[insert_idx].name != matches[iter_idx].name:
            insert_idx += 1
        # Always copy over the value at the correct location.
        matches[insert_idx] = matches[iter_idx]

    # `matches` will only be as long as the number of unique filenames
    matches = matches[: insert_idx + 1]

    # Sort by tape and offset, so that we make sure
    # that extract the files by tape order.
    matches.sort(key=lambda t: (t.tar, t.offset))

    # Retrieve from tapes
    failures: List[FilesRow]
    if args.workers > 1:
        logger.debug("Running zstash {} with multiprocessing".format(cmd))
        failures = multiprocess_extract(
            args.workers, matches, keep_files, keep, cache, args
        )
    else:
        failures = extractFiles(matches, keep_files, keep, cache, args, None, cur)

    # Close database
    logger.debug("Closing index database")
    con.close()

    return failures


# FIXME: C901 'multiprocess_extract' is too complex (21)
def multiprocess_extract(  # noqa: C901
    num_workers: int,
    matches: List[FilesRow],
    keep_files: bool,
    keep_tars: Optional[bool],
    cache: str,
    args: argparse.Namespace,
) -> List[FilesRow]:
    """
    Extract the files from the matches in parallel.
    """
    # A dict of tar -> size of files in it.
    # This is because we're trying to balance the load between
    # the processes.
    tar_to_size_unsorted: DefaultDict[str, float] = collections.defaultdict(float)
    db_row: FilesRow
    tar: str
    size: int
    for db_row in matches:
        tar, size = db_row.tar, db_row.size
        tar_to_size_unsorted[tar] += size
    # Sort by the size.
    tar_to_size: collections.OrderedDict[str, float] = collections.OrderedDict(
        sorted(tar_to_size_unsorted.items(), key=lambda x: x[1])
    )

    # We don't want to instantiate more processes than we need to.
    # So, if the number of tars is less than the number of workers,
    # set the number of workers to the number of tars.
    num_workers = min(num_workers, len(tar_to_size))

    # For worker i, workers_to_tars[i] is a set of tars that worker i will work on.
    # Assign tars in round-robin fashion to maintain proper ordering
    workers_to_tars: List[set] = [set() for _ in range(num_workers)]
    for idx, tar in enumerate(sorted(tar_to_size.keys())):
        worker_idx = idx % num_workers
        workers_to_tars[worker_idx].add(tar)

    # For worker i, workers_to_matches[i] is a list of
    # matches from the database for it to process.
    workers_to_matches: List[List[FilesRow]] = [[] for _ in range(num_workers)]
    for db_row in matches:
        tar = db_row.tar
        workers_idx: int
        for workers_idx in range(len(workers_to_tars)):
            if tar in workers_to_tars[workers_idx]:
                # This worker gets this db_row.
                workers_to_matches[workers_idx].append(db_row)

    # Sort each worker's matches by tar to ensure they process in order
    for worker_matches in workers_to_matches:
        worker_matches.sort(key=lambda t: t.tar)

    tar_ordering: List[str] = sorted([tar for tar in tar_to_size])
    manager = multiprocessing.Manager()
    monitor: parallel.PrintMonitor = parallel.PrintMonitor(
        tar_ordering, manager=manager
    )

    # Create queues for failures and output
    failure_queue: multiprocessing.Queue[FilesRow] = multiprocessing.Queue()
    output_queue: multiprocessing.Queue[tuple] = multiprocessing.Queue()

    processes: List[multiprocessing.Process] = []
    for matches in workers_to_matches:
        tars_for_this_worker: List[str] = list(set(match.tar for match in matches))
        worker: parallel.ExtractWorker = parallel.ExtractWorker(
            monitor, tars_for_this_worker, failure_queue, output_queue
        )
        process: multiprocessing.Process = multiprocessing.Process(
            target=extractFiles,
            args=(matches, keep_files, keep_tars, cache, args, worker),
            daemon=True,
        )
        process.start()
        processes.append(process)

    # Collect output and failures while processes run
    failures: List[FilesRow] = []
    max_wait_time = 180
    start_time = time.time()
    last_log_time = start_time

    # Buffer for out-of-order output
    output_buffer: Dict[str, List[str]] = {}
    current_tar_idx = 0

    while (
        any(p.is_alive() for p in processes)
        or not output_queue.empty()
        or output_buffer
    ):
        elapsed = time.time() - start_time
        if elapsed > max_wait_time and any(p.is_alive() for p in processes):
            logger.error(f"Timeout after {elapsed:.1f}s. Terminating...")
            for p in processes:
                if p.is_alive():
                    p.terminate()
            break

        # Log progress every 30 seconds
        if time.time() - last_log_time > 30:
            alive_count = sum(1 for p in processes if p.is_alive())
            logger.debug(
                f"Still waiting for {alive_count} workers after {elapsed:.1f}s"
            )
            last_log_time = time.time()

        # Collect output into buffer
        while not output_queue.empty():
            tar_name, messages = output_queue.get()
            output_buffer[tar_name] = messages

        # Print in order from buffer
        while current_tar_idx < len(tar_ordering):
            tar_name = tar_ordering[current_tar_idx]
            if tar_name in output_buffer:
                for msg in output_buffer[tar_name]:
                    print(msg, end="", flush=True)
                del output_buffer[tar_name]
                current_tar_idx += 1
            else:
                break  # Wait for this tar's output

        # Collect failures
        while not failure_queue.empty():
            failures.append(failure_queue.get())

        time.sleep(0.1)

    # Print any remaining output
    remaining_output = []
    while not output_queue.empty():
        remaining_output.append(output_queue.get())

    # Sort and print remaining output
    remaining_output.sort(
        key=lambda x: tar_ordering.index(x[0]) if x[0] in tar_ordering else 999
    )
    for tar_name, messages in remaining_output:
        for msg in messages:
            print(msg, end="", flush=True)

    # Collect remaining failures
    while not failure_queue.empty():
        failures.append(failure_queue.get())

    failures.sort(key=lambda t: (t.name, t.tar, t.offset))
    return failures


def check_sizes_match(cur, tfname, error_on_duplicate_tar):
    if cur and tars_table_exists(cur):
        logger.info(f"{tfname} exists. Checking expected size matches actual size.")
        actual_size: int = os.path.getsize(tfname)
        name_only: str = os.path.split(tfname)[1]

        # Get ALL entries for this tar name
        cur.execute(
            "SELECT size FROM tars WHERE name = ? ORDER by id DESC", (name_only,)
        )
        results = cur.fetchall()

        if not results:
            # Cannot access size information; assume the sizes match.
            logger.error(f"No database entries found for {name_only}")
            return True

        # Check for multiple entries
        if len(results) > 1:
            # Extract just the size values
            sizes: List[int] = [row[0] for row in results]
            error_str: str = (
                f"Database corruption detected! Found {len(results)} database entries for {name_only}, with sizes {sizes}"
            )

            if error_on_duplicate_tar:
                # Tested by database_corruption.bash Case 5
                logger.error(error_str)
                raise RuntimeError(error_str)
            logger.warning(error_str)

            # We ordered the results by id DESC,
            # so the first entry is the most recent.
            most_recent_size: int = sizes[0]
            if actual_size == most_recent_size:
                # Tested by database_corruption.bash Case 7
                # If the actual size matches the most recent size,
                # then we can assume that the tar is valid.
                logger.info(
                    f"{name_only}: The most recent database entry has the same size as the actual file size: {actual_size}."
                )
                return True
            unique_sizes: Set[int] = set(sizes)
            if actual_size in unique_sizes:
                # Tested by database_corruption.bash Case 8
                logger.info(
                    f"{name_only}: A database entry matches the actual file size, {actual_size}, but it is not the most recent entry."
                )
            else:
                # Tested by database_corruption.bash Case 6
                logger.info(
                    f"{name_only}: No database entry matches the actual file size: {actual_size}."
                )
            return False
        else:
            # Tested by database_corruption.bash Cases 1,2,4
            # Single entry - normal case
            logger.info(f"{name_only}: Found a single database entry.")
            expected_size = results[0][0]

        # Now check if actual size matches expected size
        if expected_size != actual_size:
            error_msg = (
                f"{name_only}: Size mismatch! "
                f"Expected={expected_size} != {actual_size}=actual. "
                f"Difference={actual_size - expected_size}."
            )
            logger.error(error_msg)
            return False
        else:
            # Sizes match
            logger.info(f"{name_only}: Size check passed ({actual_size} bytes)")
            return True
    else:
        # Cannot access size information; assume the sizes match.
        logger.debug("Cannot access tar size information; assuming sizes match")
        return True


# FIXME: C901 'extractFiles' is too complex (33)
def extractFiles(  # noqa: C901
    files: List[FilesRow],
    keep_files: bool,
    keep_tars: Optional[bool],
    cache: str,
    args: argparse.Namespace,
    multiprocess_worker: Optional[parallel.ExtractWorker] = None,
    cur: Optional[sqlite3.Cursor] = None,
) -> List[FilesRow]:
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

    If cur is None (when running in parallel), a new database connection
    will be opened for this worker process.
    """
    try:
        result = _extractFiles_impl(
            files, keep_files, keep_tars, cache, args, multiprocess_worker, cur
        )
        return result
    except Exception as e:
        if multiprocess_worker:
            # Make sure we report failures even if there's an exception
            sys.stderr.write(f"ERROR: Worker encountered fatal error: {e}\n")
            sys.stderr.flush()
            traceback.print_exc(file=sys.stderr)
            for f in files:
                multiprocess_worker.failure_queue.put(f)
        raise


# FIXME: C901 '_extractFiles_impl' is too complex (42)
def _extractFiles_impl(  # noqa: C901
    files: List[FilesRow],
    keep_files: bool,
    keep_tars: Optional[bool],
    cache: str,
    args: argparse.Namespace,
    multiprocess_worker: Optional[parallel.ExtractWorker] = None,
    cur: Optional[sqlite3.Cursor] = None,
) -> List[FilesRow]:
    """
    Implementation of extractFiles - actual extraction logic.
    """
    # Set up logging for this worker (do this once at the start)
    if (
        multiprocess_worker and cur is None
    ):  # cur is None means we're in a worker process
        sh = logging.StreamHandler(multiprocess_worker.print_queue)
        sh.setLevel(logging.DEBUG)
        formatter: logging.Formatter = logging.Formatter("%(levelname)s: %(message)s")
        sh.setFormatter(formatter)
        logger.addHandler(sh)
        logger.propagate = False

    # Open database connection if not provided (parallel case)
    if cur is None:
        con: sqlite3.Connection = sqlite3.connect(
            get_db_filename(cache), detect_types=sqlite3.PARSE_DECLTYPES
        )
        cur = con.cursor()
        close_db: bool = True
    else:
        close_db = False

    failures: List[FilesRow] = []
    tfname: str
    newtar: bool = True
    nfiles: int = len(files)

    for i in range(nfiles):
        files_row: FilesRow = files[i]

        # Open new tar archive
        if newtar:
            newtar = False
            tfname = os.path.join(cache, files_row.tar)

            # CRITICAL: Wait for our turn BEFORE doing anything with this tar
            if multiprocess_worker:
                try:
                    multiprocess_worker.print_monitor.wait_turn(
                        multiprocess_worker, files_row.tar, indef_wait=True
                    )
                except TimeoutError as e:
                    logger.error(
                        f"Timeout waiting for turn to process {files_row.tar}: {e}"
                    )
                    # Mark all remaining files from this tar as failed
                    for j in range(i, nfiles):
                        if files[j].tar == files_row.tar:
                            failures.append(files[j])
                    # Skip to next tar
                    newtar = True
                    continue

                # Set current tar for this worker BEFORE any logging happens
                multiprocess_worker.set_curr_tar(files_row.tar)
                import sys

                sys.stderr.write(
                    f"DEBUG: Set curr_tar to {multiprocess_worker.print_queue.curr_tar}\n"
                )
                sys.stderr.flush()

            # Use args.hpss directly - it's always set correctly
            if args.hpss is not None:
                hpss: str = args.hpss
            else:
                raise TypeError("Invalid args.hpss={}".format(args.hpss))

            tries: int = args.retries + 1
            # Set to True to test the `--retries` option with a forced failure.
            # Then run `python -m unittest tests.test_extract.TestExtract.testExtractRetries`
            test_retry: bool = False
            while tries > 0:
                tries -= 1
                do_retrieve: bool

                if not os.path.exists(tfname):
                    do_retrieve = True
                else:
                    do_retrieve = not check_sizes_match(
                        cur, tfname, args.error_on_duplicate_tar
                    )

                try:
                    if test_retry:
                        test_retry = False
                        raise RuntimeError
                    if do_retrieve:
                        hpss_get(hpss, tfname, cache)
                        if not check_sizes_match(
                            cur, tfname, args.error_on_duplicate_tar
                        ):
                            raise RuntimeError(
                                f"{tfname} size does not match expected size."
                            )
                    # `hpss_get` successful or not needed: no more tries needed
                    break
                except RuntimeError as e:
                    if tries > 0:
                        logger.info(f"Retrying HPSS get: {tries} tries remaining.")
                        # Run the try-except block again
                        continue
                    else:
                        raise e

            logger.info("Opening tar archive %s" % (tfname))
            tar: tarfile.TarFile = tarfile.open(tfname, "r")

        # Extract file
        cmd: str = "Extracting" if keep_files else "Checking"
        logger.info(cmd + " %s" % (files_row.name))

        if keep_files and not should_extract_file(files_row):
            # If we were going to extract, but aren't
            # because a matching file is on disk
            msg: str = "Not extracting {}, because it"
            msg += " already exists on disk with the same"
            msg += " size and modification date."
            logger.info(msg.format(files_row.name))

        # True if we should actually extract the file from the tar
        extract_this_file: bool = keep_files and should_extract_file(files_row)

        try:
            # Seek file position
            if tar.fileobj is not None:
                fileobj = tar.fileobj
            else:
                raise TypeError("Invalid tar.fileobj={}".format(tar.fileobj))
            fileobj.seek(files_row.offset)

            # Get next member
            tarinfo: tarfile.TarInfo = tar.tarinfo.fromtarfile(tar)

            if tarinfo.isfile():
                # fileobj to extract
                # error: Name 'tarfile.ExFileObject' is not defined
                extracted_file: Optional[tarfile.ExFileObject] = tar.extractfile(tarinfo)  # type: ignore
                if extracted_file:
                    fin: tarfile.ExFileObject = extracted_file
                else:
                    raise TypeError("Invalid extracted_file={}".format(extracted_file))
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
                    if os.path.getsize(fname) != files_row.size:
                        logger.error("size mismatch for: {}".format(fname))

                # Verify md5 checksum
                files_row_md5: Optional[str] = files_row.md5
                if md5 != files_row_md5:
                    logger.error("md5 mismatch for: {}".format(fname))
                    logger.error("md5 of extracted file: {}".format(md5))
                    logger.error("md5 of original file:  {}".format(files_row_md5))

                    failures.append(files_row)
                else:
                    logger.debug("Valid md5: {} {}".format(md5, fname))

            elif extract_this_file:
                tar.extract(tarinfo)
                # Note: tar.extract() will not restore time stamps of symbolic
                # links. Could not find a Python-way to restore it either, so
                # relying here on 'touch'. This is not the prettiest solution.
                # Maybe a better one can be implemented later.
                if tarinfo.issym():
                    tmp1 = tarinfo.mtime
                    tmp2: datetime = datetime.fromtimestamp(tmp1)
                    tmp3: str = tmp2.strftime("%Y%m%d%H%M.%S")
                    os.system("touch -h -t %s %s" % (tmp3, tarinfo.name))

        except Exception:
            # Catch all exceptions here.
            traceback.print_exc()
            logger.error("Retrieving {}".format(files_row.name))
            failures.append(files_row)

        # Close current archive?
        if i == nfiles - 1 or files[i].tar != files[i + 1].tar:
            # We're either on the last file or the tar is distinct from the tar of the next file.

            # Close current archive file
            logger.debug("Closing tar archive {}".format(tfname))
            tar.close()

            if multiprocess_worker:
                # Mark that all output for this tar is queued
                multiprocess_worker.done_enqueuing_output_for_tar(files_row.tar)

                # Now print everything and advance to next tar
                try:
                    multiprocess_worker.print_all_contents()
                except (TimeoutError, Exception) as e:
                    logger.debug(f"Error printing contents for {files_row.tar}: {e}")

            # Open new archive next time
            newtar = True

            # Delete this tar if the corresponding command-line arg was used.
            if not keep_tars:
                if tfname is not None:
                    os.remove(tfname)
                else:
                    raise TypeError("Invalid tfname={}".format(tfname))

    # Close database connection if we opened it
    if close_db:
        cur.close()
        con.close()

    if multiprocess_worker:
        # Add failures to the queue
        for f in failures:
            multiprocess_worker.failure_queue.put(f)
    return failures


def should_extract_file(db_row: FilesRow) -> bool:
    """
    If a file is on disk already with the correct
    timestamp and size, don't extract the file.
    """
    file_name: str
    size_db: int
    mod_time_db: datetime
    file_name, size_db, mod_time_db = db_row.name, db_row.size, db_row.mtime

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
