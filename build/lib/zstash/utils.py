from __future__ import print_function, absolute_import

import hashlib
import logging
import os.path
import tarfile
import traceback
from datetime import datetime
from fnmatch import fnmatch
from .hpss import hpss_put
from .settings import config, CACHE, BLOCK_SIZE, DB_FILENAME


def excludeFiles(exclude, files):

    # Construct lits of files to exclude, based on
    #  https://codereview.stackexchange.com/questions/33624/
    #  filtering-a-long-list-of-files-through-a-set-of-ignore-patterns-using-iterators
    exclude_patterns = exclude.split(',')

    # If exclude pattern ends with a trailing '/', the user intends to exclude
    # the entire subdirectory content, therefore replace '/' with '/*'
    for i in range(len(exclude_patterns)):
        if exclude_patterns[i][-1] == "/":
            exclude_patterns[i] += '*'

    # Actual files to exclude
    exclude_files = []
    for file in files:
        if any(fnmatch(file, pattern) for pattern in exclude_patterns):
            exclude_files.append(file)
            continue

    # Now, remove them
    new_files = [f for f in files if f not in exclude_files]

    return new_files


def addfiles(cur, con, itar, files):

    # Now, perform the actual archiving
    failures = []
    newtar = True
    nfiles = len(files)
    for i in range(nfiles):

        # New tar archive in the local cache
        if newtar:
            newtar = False
            archived = []
            tarsize = 0
            itar += 1
            tname = "{0:0{1}x}".format(itar, 6)
            tfname = "%s.tar" % (tname)
            logging.info('Creating new tar archive %s' % (tfname))
            tar = tarfile.open(os.path.join(CACHE, tfname), "w")

        # Add current file to tar archive
        file = files[i]
        logging.info('Archiving %s' % (file))
        try:
            offset, size, mtime, md5 = addfile(tar, file)
            archived.append((file, size, mtime, md5, tfname, offset))
            tarsize += size
        except:
            traceback.print_exc()
            logging.error('Archiving %s' % (file))
            failures.append(file)

        # Close tar archive if current file is the last one or adding one more
        # would push us over the limit.
        next_file_size = tar.gettarinfo(file).size
        if (i == nfiles-1 or tarsize+next_file_size > config.maxsize):

            # Close current temporary file
            logging.debug('Closing tar archive %s' % (tfname))
            tar.close()

            # Transfer tar archive to HPSS
            hpss_put(config.hpss, os.path.join(CACHE, tfname), config.keep)

            # Update database with files that have been archived
            cur.executemany(u"insert into files values (NULL,?,?,?,?,?,?)",
                            archived)
            con.commit()

            # Open new archive next time
            newtar = True

    return failures


# Add file to tar archive while computing its hash
# Return file offset (in tar archive), size and md5 hash
def addfile(tar, file):
    offset = tar.offset
    tarinfo = tar.gettarinfo(file)
    # Change the size of any hardlinks from 0 to the size of the actual file
    if tarinfo.islnk():
        tarinfo.size = os.path.getsize(file)
    tar.addfile(tarinfo)

    # Only add files or hardlinks.
    # So don't add directories or softlinks.
    if tarinfo.isfile() or tarinfo.islnk():
        f = open(file, "rb")
        hash_md5 = hashlib.md5()
        while True:
            s = f.read(BLOCK_SIZE)
            if len(s) > 0:
                tar.fileobj.write(s)
                hash_md5.update(s)
            if len(s) < BLOCK_SIZE:
                blocks, remainder = divmod(tarinfo.size, tarfile.BLOCKSIZE)
                if remainder > 0:
                    tar.fileobj.write(tarfile.NUL *
                                      (tarfile.BLOCKSIZE - remainder))
                    blocks += 1
                tar.offset += blocks * tarfile.BLOCKSIZE
                break
        f.close()
        md5 = hash_md5.hexdigest()
    else:
        md5 = None
    size = tarinfo.size
    mtime = datetime.utcfromtimestamp(tarinfo.mtime)
    return offset, size, mtime, md5
