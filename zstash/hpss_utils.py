from __future__ import absolute_import, print_function

import hashlib
import os
import os.path
import sqlite3
import tarfile
import traceback
from datetime import datetime
from typing import List, Optional, Tuple

import _hashlib

from .hpss import hpss_put
from .settings import TupleFilesRowNoId, TupleTarsRowNoId, config, logger
from .transfer_tracking import TransferManager
from .utils import create_tars_table, tars_table_exists, ts_utc


# This class holds parameters for developer options.
# I.e., these parameters should only ever be activated by developers during debugging and/or testing.
class DevOptions(object):
    def __init__(
        self,
        error_on_duplicate_tar: bool,
        overwrite_duplicate_tars: bool,
        force_database_corruption: str,
    ):
        self.error_on_duplicate_tar: bool = error_on_duplicate_tar
        self.overwrite_duplicate_tars: bool = overwrite_duplicate_tars
        self.force_database_corruption: str = force_database_corruption

    def simulate_row_existing(
        self,
        tfname: str,
        cur: sqlite3.Cursor,
        tar_tuple: TupleTarsRowNoId,
        tar_size: int,
        tar_md5: Optional[str],
    ):
        if self.force_database_corruption == "simulate_row_existing":
            # Tested by database_corruption.bash Cases 3, 5
            logger.info(
                f"TESTING/DEBUGGING ONLY: Simulating row existing for {tfname}."
            )
            cur.execute("INSERT INTO tars VALUES (NULL,?,?,?)", tar_tuple)
        elif self.force_database_corruption == "simulate_row_existing_bad_size":
            # Tested by database_corruption.bash Cases 4, 7
            logger.info(
                f"TESTING/DEBUGGING ONLY: Simulating row existing with bad size for {tfname}."
            )
            cur.execute(
                "INSERT INTO tars VALUES (NULL,?,?,?)",
                (tfname, tar_size + 1000, tar_md5),
            )


class TarWrapper(object):
    def __init__(self, tar_num: int, cache: str, do_hash: bool, follow_symlinks: bool):
        # Create a hex value at least 6 digits long
        tname: str = "{0:0{1}x}".format(tar_num, 6)
        # Create the tar file name by adding ".tar"
        self.tfname: str = f"{tname}.tar"
        logger.info(f"{ts_utc()}: Creating new tar archive {self.tfname}")
        # Open that tar file in the cache
        self.tarFileObject = HashIO(os.path.join(cache, self.tfname), "wb", do_hash)
        # FIXME: error: Argument "fileobj" to "open" has incompatible type "HashIO"; expected "Optional[IO[bytes]]"
        self.tar = tarfile.open(mode="w", fileobj=self.tarFileObject, dereference=follow_symlinks)  # type: ignore

    def process_file(
        self,
        current_file: str,
        tar_info: tarfile.TarInfo,
        archived: List[TupleFilesRowNoId],
        failures: List[str],
    ) -> int:
        logger.info(f"Archiving {current_file}")
        tar_size: int = 0
        try:
            offset: int
            size: int
            mtime: datetime
            md5: Optional[str]
            offset, size, mtime, md5 = add_file_to_tar_archive(
                self.tar, current_file, tar_info
            )
            t: TupleFilesRowNoId = (
                current_file,
                size,
                mtime,
                md5,
                self.tfname,
                offset,
            )
            archived.append(t)
            # Increase tar_size by the size of the current file.
            # Use `tell()` to also include the tar's metadata in the size.
            tar_size = self.tarFileObject.tell()
        except Exception:
            # Catch all exceptions here.
            traceback.print_exc()
            logger.error(f"Archiving {current_file}")
            failures.append(current_file)
        return tar_size

    def process_tar(
        self,
        cache: str,
        keep: bool,
        non_blocking: bool,
        transfer_manager: Optional[TransferManager],
        skip_tars_md5: bool,
        cur: sqlite3.Cursor,
        con: sqlite3.Connection,
        dev_options: DevOptions,
        archived: List[TupleFilesRowNoId],
    ):
        # 1. Close the tar ####################################################
        logger.debug(f"{ts_utc()}: Closing tar archive {self.tfname}")
        self.tar.close()

        tar_size = self.tarFileObject.tell()
        tar_md5: Optional[str] = self.tarFileObject.md5()
        self.tarFileObject.close()
        logger.info(f"{ts_utc()}: (process_tar): Completed archive file {self.tfname}")

        # 2. Transfer the tar to HPSS #########################################
        if config.hpss is not None:
            hpss: str = config.hpss
        else:
            raise TypeError("Invalid config.hpss={}".format(config.hpss))

        logger.debug(f"Contents of the cache prior to `hpss_put`: {os.listdir(cache)}")

        logger.info(
            f"{ts_utc()}: DIVING: (process_tar): Calling hpss_put to dispatch archive file {self.tfname} [keep, non_blocking] = [{keep}, {non_blocking}]"
        )
        # Actually transfer the tar file
        hpss_put(
            hpss,
            os.path.join(cache, self.tfname),
            cache,
            keep,
            non_blocking,
            is_index=False,
            transfer_manager=transfer_manager,
        )
        logger.info(
            f"{ts_utc()}: SURFACE (process_tar): Called hpss_put to dispatch archive file {self.tfname}"
        )

        # 3. Add the tar itself to the tars table #############################
        if not skip_tars_md5:
            tar_tuple: TupleTarsRowNoId = (self.tfname, tar_size, tar_md5)
            logger.info("tar name={}, tar size={}, tar md5={}".format(*tar_tuple))
            if not tars_table_exists(cur):
                # Need to create tars table
                create_tars_table(cur, con)

            # For developers only! For debugging/testing purposes only!
            dev_options.simulate_row_existing(
                self.tfname, cur, tar_tuple, tar_size, tar_md5
            )

            # We're done adding files to the tar.
            # And we've transferred it to HPSS.
            # Now we can insert the tar into the database.
            cur.execute("SELECT COUNT(*) FROM tars WHERE name = ?", (self.tfname,))
            tar_count: int = cur.fetchone()[0]
            if tar_count != 0:
                error_str: str = (
                    f"Database corruption detected! {self.tfname} is already in the database."
                )
                if dev_options.error_on_duplicate_tar:
                    # Tested by database_corruption.bash Case 3
                    # Exists - error out
                    logger.error(error_str)
                    raise RuntimeError(error_str)
                elif dev_options.overwrite_duplicate_tars:
                    # Tested by database_corruption.bash Case 4
                    # Exists - update with new size and md5
                    logger.warning(error_str)
                    logger.warning(f"Updating existing tar {self.tfname} to proceed.")
                    cur.execute(
                        "UPDATE tars SET size = ?, md5 = ? WHERE name = ?",
                        (tar_size, tar_md5, self.tfname),
                    )
                else:
                    # Tested by database_corruption.bash Cases 5,7
                    # Proceed as if we're in the typical case -- insert new
                    logger.warning(error_str)
                    logger.warning(f"Adding a new entry for {self.tfname}.")
                    cur.execute("INSERT INTO tars VALUES (NULL,?,?,?)", tar_tuple)
            elif dev_options.force_database_corruption == "simulate_no_correct_size":
                # Tested by database_corruption.bash Case 6
                # For developers only! For debugging purposes only!
                # Add this tar twice, with different sizes.
                logger.info(
                    f"TESTING/DEBUGGING ONLY: Simulating no correct size for {self.tfname}."
                )
                cur.execute(
                    "INSERT INTO tars VALUES (NULL,?,?,?)",
                    (self.tfname, tar_size + 1000, tar_md5),
                )
                cur.execute(
                    "INSERT INTO tars VALUES (NULL,?,?,?)",
                    (self.tfname, tar_size + 2000, tar_md5),
                )
            elif (
                dev_options.force_database_corruption
                == "simulate_bad_size_for_most_recent"
            ):
                # Tested by database_corruption.bash Case 8
                # For developers only! For debugging purposes only!
                # Add this tar twice, second time with bad size.
                logger.info(
                    f"TESTING/DEBUGGING ONLY: Simulating bad size for most recent entry for {self.tfname}."
                )
                cur.execute(
                    "INSERT INTO tars VALUES (NULL,?,?,?)",
                    (self.tfname, tar_size, tar_md5),
                )
                cur.execute(
                    "INSERT INTO tars VALUES (NULL,?,?,?)",
                    (self.tfname, tar_size + 2000, tar_md5),
                )
            else:
                # Tested by database_corruption.bash Cases 1,2
                # Typical case
                # Doesn't exist - insert new
                logger.info(f"Adding {self.tfname} to the database.")
                cur.execute("INSERT INTO tars VALUES (NULL,?,?,?)", tar_tuple)

            con.commit()

        # 4. Add the files included in this tar to the files table ############
        # Update database with the individual files that have been archived
        # Add a row to the "files" table,
        # the last 6 columns matching the values of `archived`
        cur.executemany("insert into files values (NULL,?,?,?,?,?,?)", archived)
        con.commit()


# Minimum output file object
class HashIO(object):
    def __init__(self, name: str, mode: str, do_hash: bool):
        self.f = open(name, mode)
        self.hash: Optional[_hashlib.HASH]
        if do_hash:
            self.hash = hashlib.md5()
        else:
            self.hash = None
        self.closed: bool = False
        self.position: int = 0

    def tell(self) -> int:
        return self.position

    def write(self, s):
        self.f.write(s)
        if self.hash:
            self.hash.update(s)
        self.position += len(s)

    def md5(self) -> Optional[str]:
        md5: Optional[str]
        if self.hash:
            md5 = self.hash.hexdigest()
        else:
            md5 = None
        return md5

    def close(self):
        if self.closed:
            return

        self.f.close()
        self.closed = True


# Add file to tar archive while computing its hash
# Return file offset (in tar archive), size and md5 hash
def add_file_to_tar_archive(
    tar: tarfile.TarFile, file_name: str, tar_info: tarfile.TarInfo
) -> Tuple[int, int, datetime, Optional[str]]:
    offset = tar.offset

    md5: Optional[str] = None

    # For files/hardlinks
    if tar_info.isfile() or tar_info.islnk():
        if tar_info.size > 0:
            # Non-empty files: stream with hash computation
            hash_md5 = hashlib.md5()
            with open(file_name, "rb") as f:
                wrapper = HashingFileWrapper(f, hash_md5)
                tar.addfile(tar_info, wrapper)
            md5 = hash_md5.hexdigest()
        else:
            # Empty files: just add to tar, compute hash of empty data
            tar.addfile(tar_info)
            md5 = hashlib.md5(b"").hexdigest()  # MD5 of empty bytes
    else:
        # Directories, symlinks, etc.
        # md5 will be None in these cases.
        tar.addfile(tar_info)

    size = tar_info.size
    mtime = datetime.utcfromtimestamp(tar_info.mtime)
    return offset, size, mtime, md5


def construct_tars(
    cur: sqlite3.Cursor,
    con: sqlite3.Connection,
    itar: int,
    files: List[str],
    cache: str,
    keep: bool,
    follow_symlinks: bool,
    dev_options: DevOptions,
    skip_tars_md5: bool = False,
    non_blocking: bool = False,
    transfer_manager: Optional[TransferManager] = None,
) -> List[str]:

    failures: List[str] = []
    nfiles: int = len(files)

    if config.maxsize is not None:
        max_size: int = config.maxsize
    else:
        raise TypeError(f"Invalid config.maxsize={config.maxsize}")

    operation: str
    if itar == -1:
        operation = "creation"
    else:
        operation = "update"

    i_file: int = 0
    carried_over_tar_info: Optional[tarfile.TarInfo] = None
    while i_file < nfiles:
        # Each iteration of this loop constructs one tar

        # `create` passes in itar=-1, so the first tar will be 000000.tar
        # `update` passes in itar=max existing tar number, so the first tar will be max+1
        itar += 1
        tar_size: int = 0
        archived: List[TupleFilesRowNoId] = []

        # Open a new tar
        # Note: if we're not skipping tars, we want to calculate the hash of the tars.
        tar_wrapper = TarWrapper(
            tar_num=itar,
            cache=cache,
            do_hash=not skip_tars_md5,
            follow_symlinks=follow_symlinks,
        )

        # Add files to the tar until we reach the max size
        while i_file < nfiles:
            current_file: str = files[i_file]
            if carried_over_tar_info:
                # If current_file wasn't added to the last tar,
                # then we can immediately add it now.
                # No need to repeat the size calculations.
                tar_info = carried_over_tar_info
                current_file_size = tar_info.size
                carried_over_tar_info = None  # Reset for next iteration
            else:
                try:
                    # It's ok that current_file isn't in the tar yet.
                    tar_info = tar_wrapper.tar.gettarinfo(current_file)
                    if tar_info.islnk():
                        tar_info.size = os.path.getsize(current_file)
                    current_file_size = tar_info.size
                except FileNotFoundError:
                    logger.error(f"Archiving {current_file}")
                    if follow_symlinks:
                        raise Exception(
                            f"Archive {operation} failed due to broken symlink."
                        )
                    else:
                        raise

            # Check if adding this file would send us over the max size.
            if (tar_size == 0) or (tar_size + current_file_size <= max_size):
                # Case 1: if the tar is empty, always add the file, even if it's over the max size.
                # Case 2: if the tar is nonempty, only add the file if it won't put us over the max size.
                new_tar_size = tar_wrapper.process_file(
                    current_file, tar_info, archived, failures
                )
                if new_tar_size != 0:
                    tar_size = new_tar_size
                # Else: process_file failed, so we should keep the original tar_size
                i_file += 1
            else:
                # Over the size limit:
                # Time to close and transfer this tar archive.
                carried_over_tar_info = tar_info  # Carry over this info to the next tar
                # Break out of the inner while-loop:
                # Done adding files to this particular tar.
                break

        # Close and transfer this tar archive, and update the database with the archived files.
        tar_wrapper.process_tar(
            cache,
            keep,
            non_blocking,
            transfer_manager,
            skip_tars_md5,
            cur,
            con,
            dev_options,
            archived,
        )

    return failures


# Create a wrapper that computes hash while data passes through
class HashingFileWrapper:
    def __init__(self, fileobj, hasher):
        self.fileobj = fileobj
        self.hasher = hasher

    def read(self, size=-1):
        data = self.fileobj.read(size)
        if data:
            self.hasher.update(data)
        return data
