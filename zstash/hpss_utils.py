from __future__ import absolute_import, print_function

import hashlib
import os
import os.path
import sqlite3
import tarfile
import traceback
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import _hashlib

from .hpss import hpss_put
from .settings import TupleFilesRowNoId, TupleTarsRowNoId, config, logger
from .transfer_tracking import TransferManager
from .utils import create_tars_table, tars_table_exists, ts_utc

# ---------------------------------------------------------------------------
# DevOptions
# ---------------------------------------------------------------------------


class DevOptions(object):
    """
    Parameters that activate deliberate misbehaviour for testing/debugging.
    None of these should ever be set in production.
    """

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


# ---------------------------------------------------------------------------
# TarWrapper
# ---------------------------------------------------------------------------


class TarWrapper(object):
    """
    Wraps a single tar archive being built during a create or update run.

    Lifecycle (driven by construct_tars):
      1. __init__: open a new tar file in the cache directory.
      2. process_file (× N): add each source file to the tar.
      3. finalize: close the tar, upload it to HPSS/Globus, and record it in
         the database.  Broken into three named steps below so that each
         concern is easy to find.
    """

    def __init__(self, tar_num: int, cache: str, do_hash: bool, follow_symlinks: bool):
        # Derive the tar filename from the sequential tar number (hex, min 6 digits).
        tname: str = "{0:0{1}x}".format(tar_num, 6)
        self.tfname: str = f"{tname}.tar"
        logger.info(f"{ts_utc()}: Creating new tar archive {self.tfname}")
        self.tarFileObject = HashIO(os.path.join(cache, self.tfname), "wb", do_hash)
        # FIXME: error: Argument "fileobj" to "open" has incompatible type "HashIO"; expected "Optional[IO[bytes]]"
        self.tar = tarfile.open(mode="w", fileobj=self.tarFileObject, dereference=follow_symlinks)  # type: ignore

    # ------------------------------------------------------------------
    # Step 1 of finalize: close the open tar file and capture its hash
    # ------------------------------------------------------------------

    def _close_tar(self) -> Tuple[int, Optional[str]]:
        """
        Close the tar archive and return (tar_size_bytes, tar_md5).
        Must be called before _upload_tar or _record_tar_in_database.
        """
        logger.debug(f"{ts_utc()}: Closing tar archive {self.tfname}")
        self.tar.close()
        tar_size: int = self.tarFileObject.tell()
        tar_md5: Optional[str] = self.tarFileObject.md5()
        self.tarFileObject.close()
        logger.info(f"{ts_utc()}: Closed archive {self.tfname} ({tar_size} bytes)")
        return tar_size, tar_md5

    # ------------------------------------------------------------------
    # Step 2 of finalize: upload the tar to HPSS / Globus
    # ------------------------------------------------------------------

    def _upload_tar(
        self,
        cache: str,
        keep: bool,
        non_blocking: bool,
        transfer_manager: TransferManager,
    ) -> None:
        """Submit the closed tar file to the transfer system."""
        if config.hpss is None:
            raise TypeError("Invalid config.hpss={}".format(config.hpss))
        hpss: str = config.hpss

        logger.debug(f"Cache contents before upload: {os.listdir(cache)}")
        logger.info(
            f"{ts_utc()}: Uploading {self.tfname} "
            f"[keep={keep}, non_blocking={non_blocking}]"
        )
        hpss_put(
            hpss,
            os.path.join(cache, self.tfname),
            cache,
            transfer_manager,
            keep,
            non_blocking,
            is_index=False,
        )
        logger.info(f"{ts_utc()}: Upload dispatched for {self.tfname}")

    # ------------------------------------------------------------------
    # Step 3 of finalize: record the tar and its files in the database
    # ------------------------------------------------------------------

    def _record_tar_in_database(
        self,
        tar_size: int,
        tar_md5: Optional[str],
        skip_tars_table: bool,
        cur: sqlite3.Cursor,
        con: sqlite3.Connection,
        dev_options: DevOptions,
        archived: List[TupleFilesRowNoId],
    ) -> None:
        """
        Insert the tar itself into the 'tars' table (unless skip_tars_table is
        set) and insert every file it contains into the 'files' table.
        """
        if not skip_tars_table:
            self._insert_tar_row(tar_size, tar_md5, cur, con, dev_options)

        # Record each individual file that was archived into this tar.
        cur.executemany("insert into files values (NULL,?,?,?,?,?,?)", archived)
        con.commit()

    def _insert_tar_row(
        self,
        tar_size: int,
        tar_md5: Optional[str],
        cur: sqlite3.Cursor,
        con: sqlite3.Connection,
        dev_options: DevOptions,
    ) -> None:
        """
        Insert (or update, depending on DevOptions) a row in the 'tars' table
        for this tar archive.  Handles duplicate-tar detection and the various
        developer-only corruption-simulation modes.
        """
        tar_tuple: TupleTarsRowNoId = (self.tfname, tar_size, tar_md5)
        logger.info("tar name={}, tar size={}, tar md5={}".format(*tar_tuple))

        if not tars_table_exists(cur):
            create_tars_table(cur, con)

        # Developer-only: optionally insert a duplicate row before the main logic.
        dev_options.simulate_row_existing(
            self.tfname, cur, tar_tuple, tar_size, tar_md5
        )

        cur.execute("SELECT COUNT(*) FROM tars WHERE name = ?", (self.tfname,))
        tar_count: int = cur.fetchone()[0]

        if tar_count != 0:
            self._handle_duplicate_tar(
                tar_size, tar_md5, tar_tuple, cur, con, dev_options
            )
        elif dev_options.force_database_corruption == "simulate_no_correct_size":
            # Tested by database_corruption.bash Case 6
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
            dev_options.force_database_corruption == "simulate_bad_size_for_most_recent"
        ):
            # Tested by database_corruption.bash Case 8
            logger.info(
                f"TESTING/DEBUGGING ONLY: Simulating bad size for most recent "
                f"entry for {self.tfname}."
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
            # Tested by database_corruption.bash Cases 1, 2 — normal path.
            logger.info(f"Adding {self.tfname} to the database.")
            cur.execute("INSERT INTO tars VALUES (NULL,?,?,?)", tar_tuple)

        con.commit()

    def _handle_duplicate_tar(
        self,
        tar_size: int,
        tar_md5: Optional[str],
        tar_tuple: TupleTarsRowNoId,
        cur: sqlite3.Cursor,
        con: sqlite3.Connection,
        dev_options: DevOptions,
    ) -> None:
        """
        React to finding that this tar's name is already present in the database.
        Behaviour is controlled by DevOptions flags.
        """
        error_str = (
            f"Database corruption detected! {self.tfname} is already in the database."
        )
        if dev_options.error_on_duplicate_tar:
            # Tested by database_corruption.bash Case 3
            logger.error(error_str)
            raise RuntimeError(error_str)
        elif dev_options.overwrite_duplicate_tars:
            # Tested by database_corruption.bash Case 4
            logger.warning(error_str)
            logger.warning(f"Overwriting existing tar entry for {self.tfname}.")
            cur.execute(
                "UPDATE tars SET size = ?, md5 = ? WHERE name = ?",
                (tar_size, tar_md5, self.tfname),
            )
        else:
            # Tested by database_corruption.bash Cases 5, 7
            logger.warning(error_str)
            logger.warning(f"Adding a new entry for {self.tfname}.")
            cur.execute("INSERT INTO tars VALUES (NULL,?,?,?)", tar_tuple)

    # ------------------------------------------------------------------
    # Public entry point: add a single source file to the open tar
    # ------------------------------------------------------------------

    def process_file(
        self,
        current_file: str,
        tar_info: tarfile.TarInfo,
        archived: List[TupleFilesRowNoId],
        failures: List[str],
    ) -> int:
        """
        Add *current_file* to the tar archive.

        Appends a row to *archived* on success, or to *failures* on error.
        Returns the current cumulative tar size (0 on failure).
        """
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
            archived.append((current_file, size, mtime, md5, self.tfname, offset))
            tar_size = self.tarFileObject.tell()
        except Exception:
            traceback.print_exc()
            logger.error(f"Archiving {current_file}")
            failures.append(current_file)
        return tar_size

    # ------------------------------------------------------------------
    # Public entry point: close, upload, and record this tar
    # ------------------------------------------------------------------

    def finalize(
        self,
        cache: str,
        keep: bool,
        non_blocking: bool,
        transfer_manager: TransferManager,
        skip_tars_table: bool,
        cur: sqlite3.Cursor,
        con: sqlite3.Connection,
        dev_options: DevOptions,
        archived: List[TupleFilesRowNoId],
    ) -> None:
        """
        Complete processing of this tar archive:
          1. Close the tar file and capture its size and MD5.
          2. Upload it to HPSS / Globus.
          3. Record the tar and its constituent files in the database.
        """
        tar_size, tar_md5 = self._close_tar()
        self._upload_tar(cache, keep, non_blocking, transfer_manager)
        self._record_tar_in_database(
            tar_size, tar_md5, skip_tars_table, cur, con, dev_options, archived
        )

    # Keep the old name as an alias so any external callers are not broken.
    # Prefer finalize() in new code.
    def process_tar(
        self,
        cache: str,
        keep: bool,
        non_blocking: bool,
        transfer_manager: TransferManager,
        skip_tars_table: bool,
        cur: sqlite3.Cursor,
        con: sqlite3.Connection,
        dev_options: DevOptions,
        archived: List[TupleFilesRowNoId],
    ) -> None:
        self.finalize(
            cache,
            keep,
            non_blocking,
            transfer_manager,
            skip_tars_table,
            cur,
            con,
            dev_options,
            archived,
        )


# ---------------------------------------------------------------------------
# HashIO — minimal file-like object that tracks position and MD5 as we write
# ---------------------------------------------------------------------------


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
        """
        Called implicitly by tarfile as it streams data into the archive.
        (tarfile.open requires the fileobj to have a write() method.)
        """
        self.f.write(s)
        if self.hash:
            self.hash.update(s)
        self.position += len(s)

    def md5(self) -> Optional[str]:
        if self.hash:
            return self.hash.hexdigest()
        return None

    def close(self):
        if self.closed:
            return
        self.f.close()
        self.closed = True


# ---------------------------------------------------------------------------
# Tar-size estimation
# ---------------------------------------------------------------------------


def estimate_tar_entry_size(file_size: int) -> int:
    """
    Estimate how much space a file of the given size will occupy in the tar
    archive, including the per-file header and block-alignment padding.
    """
    TAR_BLOCK_SIZE = 512
    TAR_HEADER_SIZE = 512
    data_blocks = (file_size + TAR_BLOCK_SIZE - 1) // TAR_BLOCK_SIZE
    return TAR_HEADER_SIZE + (data_blocks * TAR_BLOCK_SIZE)


# ---------------------------------------------------------------------------
# Adding a single file to a tar archive
# ---------------------------------------------------------------------------


def add_file_to_tar_archive(
    tar: tarfile.TarFile, file_name: str, tar_info: tarfile.TarInfo
) -> Tuple[int, int, datetime, Optional[str]]:
    """
    Add *file_name* to *tar* while computing its MD5 hash.

    Returns (offset_in_tar, file_size, mtime, md5).
    md5 is None for directories and symlinks.
    """
    offset = tar.offset
    md5: Optional[str] = None

    if tar_info.isfile() or tar_info.islnk():
        if tar_info.size > 0:
            hash_md5 = hashlib.md5()
            with open(file_name, "rb") as f:
                tar.addfile(tar_info, HashingFileWrapper(f, hash_md5))
            md5 = hash_md5.hexdigest()
        else:
            tar.addfile(tar_info)
            md5 = hashlib.md5(b"").hexdigest()
    else:
        # Directories, symlinks, etc. — no file data to hash.
        tar.addfile(tar_info)

    size = tar_info.size
    mtime = datetime.utcfromtimestamp(tar_info.mtime)
    return offset, size, mtime, md5


# ---------------------------------------------------------------------------
# Main loop: pack files into tars and dispatch each tar for upload
# ---------------------------------------------------------------------------


def construct_tars(
    cur: sqlite3.Cursor,
    con: sqlite3.Connection,
    itar: int,
    file_stats: Dict[str, Tuple[int, datetime]],
    cache: str,
    keep: bool,
    follow_symlinks: bool,
    dev_options: DevOptions,
    transfer_manager: TransferManager,
    skip_tars_table: bool = False,
    non_blocking: bool = False,
) -> List[str]:
    """
    Pack *file_stats* into a sequence of tar archives (each no larger than
    config.maxsize), upload each archive, and record everything in the database.

    *itar* is the index of the last existing tar (-1 for a fresh create, or the
    highest existing tar number for an update).  The first new tar will be
    itar+1.

    Returns a list of file paths that could not be archived.
    """
    failures: List[str] = []
    files: List[str] = list(file_stats.keys())
    nfiles: int = len(files)

    if config.maxsize is None:
        raise TypeError(f"Invalid config.maxsize={config.maxsize}")
    max_size: int = config.maxsize

    operation = "creation" if itar == -1 else "update"

    i_file: int = 0
    while i_file < nfiles:
        # Each iteration of this outer loop produces exactly one tar archive.
        itar += 1
        cumulative_tar_size: int = 0
        archived: List[TupleFilesRowNoId] = []

        tar_wrapper = TarWrapper(
            tar_num=itar,
            cache=cache,
            # We need the tar's hash iff we are writing it to the tars table.
            do_hash=not skip_tars_table,
            follow_symlinks=follow_symlinks,
        )

        # Add files until this tar would exceed max_size.
        while i_file < nfiles:
            current_file: str = files[i_file]
            current_file_size, _ = file_stats[current_file]
            estimated_entry_size: int = estimate_tar_entry_size(current_file_size)

            if cumulative_tar_size != 0 and (
                cumulative_tar_size + estimated_entry_size > max_size
            ):
                # This file would push us over the limit; start a new tar.
                break

            # Attempt to get the tarinfo for the current file.
            try:
                tar_info = tar_wrapper.tar.gettarinfo(current_file)
                if tar_info.islnk():
                    tar_info.size = os.path.getsize(current_file)
            except FileNotFoundError:
                logger.error(f"Archiving {current_file}")
                if follow_symlinks:
                    raise Exception(
                        f"Archive {operation} failed due to broken symlink."
                    )
                else:
                    raise

            new_size = tar_wrapper.process_file(
                current_file, tar_info, archived, failures
            )
            if new_size != 0:
                cumulative_tar_size = new_size
            i_file += 1

        # Close this tar, upload it, and record it in the database.
        tar_wrapper.finalize(
            cache,
            keep,
            non_blocking,
            transfer_manager,
            skip_tars_table,
            cur,
            con,
            dev_options,
            archived,
        )

    return failures


# ---------------------------------------------------------------------------
# HashingFileWrapper — streams data through a hasher as tarfile reads it
# ---------------------------------------------------------------------------


class HashingFileWrapper:
    def __init__(self, fileobj, hasher):
        self.fileobj = fileobj
        self.hasher = hasher

    def read(self, size=-1):
        data = self.fileobj.read(size)
        if data:
            self.hasher.update(data)
        return data
