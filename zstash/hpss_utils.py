from __future__ import print_function, absolute_import

import hashlib
import os.path
import tarfile
import traceback
from datetime import datetime
from .hpss import hpss_put
from .settings import config, logger, CACHE, BLOCK_SIZE, DB_FILENAME


def add_files(cur, con, itar, files):

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
            logger.info('Creating new tar archive %s' % (tfname))
            tar = tarfile.open(os.path.join(CACHE, tfname), "w")

        # Add current file to tar archive
        current_file = files[i]
        logger.info('Archiving %s' % (current_file))
        try:
            offset, size, mtime, md5 = add_file(tar, current_file)
            archived.append((current_file, size, mtime, md5, tfname, offset))
            tarsize += size
        except:
            traceback.print_exc()
            logger.error('Archiving %s' % (current_file))
            failures.append(current_file)

        # Close tar archive if current file is the last one or adding one more
        # would push us over the limit.
        next_file_size = tar.gettarinfo(current_file).size
        if (i == nfiles-1 or tarsize+next_file_size > config.maxsize):

            # Close current temporary file
            logger.debug('Closing tar archive %s' % (tfname))
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
def add_file(tar, file_name):
    offset = tar.offset
    tarinfo = tar.gettarinfo(file_name)
    # Change the size of any hardlinks from 0 to the size of the actual file
    if tarinfo.islnk():
        tarinfo.size = os.path.getsize(file_name)
    tar.addfile(tarinfo)

    # Only add files or hardlinks.
    # So don't add directories or softlinks.
    if tarinfo.isfile() or tarinfo.islnk():
        f = open(file_name, "rb")
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
