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
import _io

from .hpss import hpss_put
from .settings import BLOCK_SIZE, TupleFilesRowNoId, TupleTarsRowNoId, config, logger
from .transfer_tracking import GlobusTransferCollection, HPSSTransferCollection
from .utils import create_tars_table, tars_table_exists, ts_utc


# Minimum output file object
class HashIO(object):
    def __init__(self, name: str, mode: str, do_hash: bool):
        self.f: _io.BufferedWriter = open(name, mode)
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


def add_files(
    cur: sqlite3.Cursor,
    con: sqlite3.Connection,
    itar: int,
    files: List[str],
    cache: str,
    keep: bool,
    follow_symlinks: bool,
    skip_tars_md5: bool = False,
    non_blocking: bool = False,
    error_on_duplicate_tar: bool = False,
    overwrite_duplicate_tars: bool = False,
    force_database_corruption: str = "",
    gtc: Optional[GlobusTransferCollection] = None,
    htc: Optional[HPSSTransferCollection] = None,
) -> List[str]:

    # Now, perform the actual archiving
    failures: List[str] = []
    create_new_tar: bool = True
    nfiles: int = len(files)
    archived: List[TupleFilesRowNoId]
    tarsize: int
    tname: str
    tfname: str
    tarFileObject: HashIO
    tar: tarfile.TarFile
    for i in range(nfiles):

        # New tar in the local cache
        if create_new_tar:
            create_new_tar = False
            archived = []
            tarsize = 0
            itar += 1
            # Create a hex value at least 6 digits long
            tname = "{0:0{1}x}".format(itar, 6)
            # Create the tar file name by adding ".tar"
            tfname = "{}.tar".format(tname)
            logger.info(f"{ts_utc()}: Creating new tar archive {tfname}")
            # Open that tar file in the cache
            do_hash: bool
            if not skip_tars_md5:
                # If we're not skipping tars, we want to calculate the hash of the tars.
                do_hash = True
            else:
                do_hash = False
            tarFileObject = HashIO(os.path.join(cache, tfname), "wb", do_hash)
            # FIXME: error: Argument "fileobj" to "open" has incompatible type "HashIO"; expected "Optional[IO[bytes]]"
            tar = tarfile.open(mode="w", fileobj=tarFileObject, dereference=follow_symlinks)  # type: ignore

        # Add current file to tar archive
        current_file: str = files[i]
        logger.info("Archiving {}".format(current_file))
        try:
            offset: int
            size: int
            mtime: datetime
            md5: Optional[str]
            offset, size, mtime, md5 = add_file(tar, current_file, follow_symlinks)
            t: TupleFilesRowNoId = (
                current_file,
                size,
                mtime,
                md5,
                tfname,
                offset,
            )
            archived.append(t)
            # Increase tarsize by the size of the current file.
            # Use `tell()` to also include the tar's metadata in the size.
            tarsize = tarFileObject.tell()
        except Exception:
            # Catch all exceptions here.
            traceback.print_exc()
            logger.error("Archiving {}".format(current_file))
            failures.append(current_file)

        # Close tar archive if current file is the last one or
        # if adding one more would push us over the limit.
        next_file_size: int = tar.gettarinfo(current_file).size
        if config.maxsize is not None:
            maxsize: int = config.maxsize
        else:
            raise TypeError("Invalid config.maxsize={}".format(config.maxsize))
        if i == nfiles - 1 or tarsize + next_file_size > maxsize:

            # Close current temporary file
            logger.debug(f"{ts_utc()}: Closing tar archive {tfname}")
            tar.close()

            tarsize = tarFileObject.tell()
            tar_md5: Optional[str] = tarFileObject.md5()
            tarFileObject.close()
            logger.info(f"{ts_utc()}: (add_files): Completed archive file {tfname}")

            # Transfer tar to HPSS
            if config.hpss is not None:
                hpss: str = config.hpss
            else:
                raise TypeError("Invalid config.hpss={}".format(config.hpss))

            logger.info(
                f"Contents of the cache prior to `hpss_put`: {os.listdir(cache)}"
            )

            logger.info(
                f"{ts_utc()}: DIVING: (add_files): Calling hpss_put to dispatch archive file {tfname} [keep, non_blocking] = [{keep}, {non_blocking}]"
            )
            hpss_put(
                hpss,
                os.path.join(cache, tfname),
                cache,
                keep,
                non_blocking,
                gtc=gtc,
                htc=htc,
            )
            logger.info(
                f"{ts_utc()}: SURFACE (add_files): Called hpss_put to dispatch archive file {tfname}"
            )

            if not skip_tars_md5:
                tar_tuple: TupleTarsRowNoId = (tfname, tarsize, tar_md5)
                logger.info("tar name={}, tar size={}, tar md5={}".format(*tar_tuple))
                if not tars_table_exists(cur):
                    # Need to create tars table
                    create_tars_table(cur, con)

                # For developers only! For debugging purposes only!
                if force_database_corruption == "simulate_row_existing":
                    # Tested by database_corruption.bash Cases 3, 5
                    logger.info(
                        f"TESTING/DEBUGGING ONLY: Simulating row existing for {tfname}."
                    )
                    cur.execute("INSERT INTO tars VALUES (NULL,?,?,?)", tar_tuple)
                elif force_database_corruption == "simulate_row_existing_bad_size":
                    # Tested by database_corruption.bash CaseS 4, 7
                    logger.info(
                        f"TESTING/DEBUGGING ONLY: Simulating row existing with bad size for {tfname}."
                    )
                    cur.execute(
                        "INSERT INTO tars VALUES (NULL,?,?,?)",
                        (tfname, tarsize + 1000, tar_md5),
                    )

                # We're done adding files to the tar.
                # And we've transferred it to HPSS.
                # Now we can insert the tar into the database.
                cur.execute("SELECT COUNT(*) FROM tars WHERE name = ?", (tfname,))
                tar_count: int = cur.fetchone()[0]
                if tar_count != 0:
                    error_str: str = (
                        f"Database corruption detected! {tfname} is already in the database."
                    )
                    if error_on_duplicate_tar:
                        # Tested by database_corruption.bash Case 3
                        # Exists - error out
                        logger.error(error_str)
                        raise RuntimeError(error_str)
                    elif overwrite_duplicate_tars:
                        # Tested by database_corruption.bash Case 4
                        # Exists - update with new size and md5
                        logger.warning(error_str)
                        logger.warning(f"Updating existing tar {tfname} to proceed.")
                        cur.execute(
                            "UPDATE tars SET size = ?, md5 = ? WHERE name = ?",
                            (tarsize, tar_md5, tfname),
                        )
                    else:
                        # Tested by database_corruption.bash Cases 5,7
                        # Proceed as if we're in the typical case -- insert new
                        logger.warning(error_str)
                        logger.warning(f"Adding a new entry for {tfname}.")
                        cur.execute("INSERT INTO tars VALUES (NULL,?,?,?)", tar_tuple)
                elif force_database_corruption == "simulate_no_correct_size":
                    # Tested by database_corruption.bash Case 6
                    # For developers only! For debugging purposes only!
                    # Add this tar twice, with different sizes.
                    logger.info(
                        f"TESTING/DEBUGGING ONLY: Simulating no correct size for {tfname}."
                    )
                    cur.execute(
                        "INSERT INTO tars VALUES (NULL,?,?,?)",
                        (tfname, tarsize + 1000, tar_md5),
                    )
                    cur.execute(
                        "INSERT INTO tars VALUES (NULL,?,?,?)",
                        (tfname, tarsize + 2000, tar_md5),
                    )
                elif force_database_corruption == "simulate_bad_size_for_most_recent":
                    # Tested by database_corruption.bash Case 8
                    # For developers only! For debugging purposes only!
                    # Add this tar twice, second time with bad size.
                    logger.info(
                        f"TESTING/DEBUGGING ONLY: Simulating bad size for most recent entry for {tfname}."
                    )
                    cur.execute(
                        "INSERT INTO tars VALUES (NULL,?,?,?)",
                        (tfname, tarsize, tar_md5),
                    )
                    cur.execute(
                        "INSERT INTO tars VALUES (NULL,?,?,?)",
                        (tfname, tarsize + 2000, tar_md5),
                    )
                else:
                    # Tested by database_corruption.bash Cases 1,2
                    # Typical case
                    # Doesn't exist - insert new
                    logger.info(f"Adding {tfname} to the database.")
                    cur.execute("INSERT INTO tars VALUES (NULL,?,?,?)", tar_tuple)

                con.commit()

            # Update database with the individual files that have been archived
            # Add a row to the "files" table,
            # the last 6 columns matching the values of `archived`
            cur.executemany("insert into files values (NULL,?,?,?,?,?,?)", archived)
            con.commit()

            # Open new tar next time
            create_new_tar = True

    return failures


# Add file to tar archive while computing its hash
# Return file offset (in tar archive), size and md5 hash
def add_file(
    tar: tarfile.TarFile, file_name: str, follow_symlinks: bool
) -> Tuple[int, int, datetime, Optional[str]]:

    offset: int = tar.offset
    tarinfo: tarfile.TarInfo = tar.gettarinfo(file_name)
    # Change the size of any hardlinks from 0 to the size of the actual file
    if tarinfo.islnk():
        tarinfo.size = os.path.getsize(file_name)
    # Add the file to the tar
    tar.addfile(tarinfo)

    md5: Optional[str] = None
    # Only add files or hardlinks.
    # (So don't add directories or softlinks.)
    if tarinfo.isfile() or tarinfo.islnk():
        f: _io.TextIOWrapper = open(file_name, "rb")
        hash_md5: _hashlib.HASH = hashlib.md5()
        if tar.fileobj is not None:
            fileobj: _io.BufferedWriter = tar.fileobj
        else:
            raise TypeError("Invalid tar.fileobj={}".format(tar.fileobj))
        while True:
            s: str = f.read(BLOCK_SIZE)
            if len(s) > 0:
                # If the block read in is non-empty, write it to fileobj and update the hash
                fileobj.write(s)
                hash_md5.update(s)
            if len(s) < BLOCK_SIZE:
                # If the block read in is smaller than BLOCK_SIZE,
                # then we have reached the end of the file.
                # blocks = how many blocks of tarfile.BLOCKSIZE fit in tarinfo.size
                # remainder = how much more content is required to reach tarinfo.size
                blocks: int
                remainder: int
                blocks, remainder = divmod(tarinfo.size, tarfile.BLOCKSIZE)
                if remainder > 0:
                    null_bytes: bytes = tarfile.NUL
                    # Write null_bytes to get the last block to tarfile.BLOCKSIZE
                    fileobj.write(null_bytes * (tarfile.BLOCKSIZE - remainder))
                    blocks += 1
                # Increase the offset by the amount already saved to the tar
                tar.offset += blocks * tarfile.BLOCKSIZE
                break
        f.close()
        md5 = hash_md5.hexdigest()
    size: int = tarinfo.size
    mtime: datetime = datetime.utcfromtimestamp(tarinfo.mtime)
    return offset, size, mtime, md5
