import hashlib
import logging
import os.path
import tarfile

from datetime import datetime
from fnmatch import fnmatch

from hpss import hpss_put
from settings import config, CACHE, BLOCK_SIZE, DB_FILENAME


def excludeFiles(exclude, files):

    # Construct lits of files to exclude, based on
    #  https://codereview.stackexchange.com/questions/33624/
    #  filtering-a-long-list-of-files-through-a-set-of-ignore-patterns-using-iterators
    exclude_patterns = exclude.split(',')
    exclude_files = []
    for file in files:
        if any(fnmatch(file, pattern) for pattern in exclude_patterns):
            exclude_files.append(file)
            continue

    # Now, remove them
    new_files = [f for f in files if f not in exclude_files]

    return new_files


def addfiles(cur, con, itar, files):

    # File size
    files = [(x, os.path.getsize(x)) for x in files]

    # Now, perform the actual archiving
    failures = []
    newtar = True
    nfiles = len(files)
    for i in range(nfiles):

        # New tar archive in the local cache
        if newtar:
            newtar = False
            tarsize = 0
            itar += 1
            tname = "{0:0{1}x}".format(itar, 6)
            tfname = "%s.tar" % (tname)
            logging.info('Creating new tar archive %s' % (tfname))
            tar = tarfile.open(os.path.join(CACHE, tfname), "w")

        # Add current file to tar archive
        file = files[i]
        logging.info('Archiving %s' % (file[0]))
        try:
            offset, size, mtime, md5 = addfile(tar, file[0])
            cur.execute(u"insert into files values (NULL,?,?,?,?,?,?)",
                        (file[0], size, mtime, md5, tfname, offset))
            con.commit()
            tarsize += file[1]
        except:
            logging.error('Archiving %s' % (file[0]))
            failures.append(file[0])

        # Close tar archive if current file is the last one or adding one more
        # would push us over the limit.
        if (i == nfiles-1 or tarsize+files[i+1][1] > config.maxsize):

            # Close current temporary file
            logging.debug('Closing tar archive %s' % (tfname))
            tar.close()

            # Transfer tar archive to HPSS
            hpss_put(config.hpss, os.path.join(CACHE, tfname), config.keep)

            # Open new archive next time
            newtar = True

    return failures


# Add file to tar archive while computing its hash
# Return file offset (in tar archive), size and md5 hash
def addfile(tar, file):
    offset = tar.offset
    tarinfo = tar.gettarinfo(file)
    tar.addfile(tarinfo)
    if tarinfo.isfile():
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
