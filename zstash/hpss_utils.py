from __future__ import absolute_import, print_function

import hashlib
import os.path
import sqlite3
import tarfile
import traceback
from datetime import datetime
from typing import List, Optional, Tuple

import _hashlib
import _io

from .hpss import hpss_put
from .settings import BLOCK_SIZE, FilesRowNoId, config, logger


def add_files(
    cur: sqlite3.Cursor,
    con: sqlite3.Connection,
    itar: int,
    files: List[str],
    cache: str,
) -> List[str]:

    # Now, perform the actual archiving
    failures: List[str] = []
    newtar: bool = True
    nfiles: int = len(files)
    archived: List[FilesRowNoId]
    tarsize: int
    tname: str
    tfname: str
    tar: tarfile.TarFile
    for i in range(nfiles):

        # New tar archive in the local cache
        if newtar:
            newtar = False
            archived = []
            tarsize = 0
            itar += 1
            # Create a hex value at least 6 digits long
            tname = "{0:0{1}x}".format(itar, 6)
            # Create the tar file name by adding ".tar"
            tfname = "{}.tar".format(tname)
            logger.info("Creating new tar archive {}".format(tfname))
            # Open that tar file in the cache
            tar = tarfile.open(os.path.join(cache, tfname), "w")

        # Add current file to tar archive
        current_file: str = files[i]
        logger.info("Archiving {}".format(current_file))
        try:
            offset: int
            size: int
            mtime: datetime
            md5: Optional[str]
            offset, size, mtime, md5 = add_file(tar, current_file)
            t: FilesRowNoId = (
                current_file,
                size,
                mtime,
                md5,
                tfname,
                offset,
            )
            archived.append(t)
            # Increase tarsize by the size of the current file
            tarsize += size
        except Exception:
            traceback.print_exc()
            logger.error("Archiving {}".format(current_file))
            failures.append(current_file)

        # Close tar archive if current file is the last one or
        # if adding one more would push us over the limit.
        next_file_size: int = tar.gettarinfo(current_file).size
        if config.maxsize is not None:
            maxsize: int = config.maxsize
        else:
            raise Exception("Invalid config.maxsize={}".format(config.maxsize))
        if i == nfiles - 1 or tarsize + next_file_size > maxsize:

            # Close current temporary file
            logger.debug("Closing tar archive {}".format(tfname))
            tar.close()

            # Transfer tar archive to HPSS
            if config.hpss is not None:
                hpss: str = config.hpss
            else:
                raise Exception("Invalid config.hpss={}".format(config.hpss))
            if config.keep is not None:
                keep: bool = config.keep
            else:
                raise Exception("Invalid config.keep={}".format(config.keep))
            hpss_put(hpss, os.path.join(cache, tfname), cache, keep)

            # Update database with files that have been archived
            # Add a row to the "files" table,
            # the last 6 columns matching the values of `archived`
            cur.executemany(u"insert into files values (NULL,?,?,?,?,?,?)", archived)
            con.commit()

            # Open new archive next time
            newtar = True

    return failures


# Add file to tar archive while computing its hash
# Return file offset (in tar archive), size and md5 hash
def add_file(
    tar: tarfile.TarFile, file_name: str
) -> Tuple[int, int, datetime, Optional[str]]:

    # FIXME: error: "TarFile" has no attribute "offset"
    offset: int = tar.offset  # type: ignore
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
            raise Exception("Invalid tar.fileobj={}".format(tar.fileobj))
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
                # FIXME: error: "TarFile" has no attribute "offset"
                tar.offset += blocks * tarfile.BLOCKSIZE  # type: ignore
                break
        f.close()
        md5 = hash_md5.hexdigest()
    size: int = tarinfo.size
    mtime: datetime = datetime.utcfromtimestamp(tarinfo.mtime)
    return offset, size, mtime, md5
