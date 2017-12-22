"""
Utility functions for Zstash

Originally written by Chris Golaz
Updated by Sterling Baldwin
"""
import os
import tarfile
import logging
import hashlib
import shlex

from datetime import datetime
from subprocess import Popen, PIPE
from fnmatch import fnmatch

from lib.strings import extract_md5_mismatch

# -----------------------------------------------------------------------------


def excludeFiles(exclude, files):
    """
    Construct lits of files to exclude, based on
      https://codereview.stackexchange.com/questions/33624/
      filtering-a-long-list-of-files-through-a-set-of-ignore-patterns-using-iterators

    Parameters:
        exclude (list(str)): list of patterns to exclude
        files (list(str)): a list of file names
    """
    exclude_patterns = exclude.split(',')
    exclude_files = []
    for file in files:
        if any(fnmatch(file, pattern) for pattern in exclude_patterns):
            exclude_files.append(file)
            continue

    # Now, remove them
    new_files = [f for f in files if f not in exclude_files]

    return new_files


def addfiles(itar, files, config):
    """
    Adds files to an iterator

    Parameters:
        files (list(str)): A list of file paths to add
        itar (int): An iterator to keep track of the file indexes
    Returns:
        failures (list(str)): A list of filenames that weren't added
    """
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
            tar = tarfile.open(os.path.join(config.cache, tfname), "w")

        # Add current file to tar archive
        file = files[i]
        msg = 'Archiving {file}'.format(file=file[0])
        logging.info(msg)
        try:
            offset, size, mtime, md5 = addfile(tar, file[0], config.block_size)
            config.cursor.execute(u"insert into files values (NULL,?,?,?,?,?,?)",
                                  (file[0], size, mtime, md5, tfname, offset))
            config.connection.commit()
            tarsize += file[1]
        except:
            msg = 'Error during archiving of {file}'.format(file=file[0])
            logging.error(msg)
            failures.append(file[0])

        # Close tar archive if current file is the last one or adding one more
        # would push us over the limit.
        if i == nfiles - 1 or \
                tarsize + files[i + 1][1] > config.maxsize:

            # Close current temporary file
            msg = 'Closing tar archive {file}'.format(file=tfname)
            logging.debug(msg)
            tar.close()

            # Transfer tar archive to HPSS
            hpss_put(config.hpss, os.path.join(
                config.cache, tfname), config.keep)

            # Open new archive next time
            newtar = True

    return failures


def addfile(tar, file, block_size):
    """
    Add file to tar archive while computing its hash

    Parameters:
        tar (tarfile): the tarfile to add the file to,
        file (string): the path to the file to add
        block_size (int): the blocksize of the hpss system
    Returns:
        file offset (in tar archive),
        size (int) size of the tarfile,
        md5 hash (str): md5hash of the tarfile
        mtime (str): string of the timestamp from the tar info
    """
    offset = tar.offset
    tarinfo = tar.gettarinfo(file)
    tar.addfile(tarinfo)
    if tarinfo.isfile():
        with open(file, "rb") as fp:
            hash_md5 = hashlib.md5()
            while True:
                s = fp.read(block_size)
                slen = len(s)
                if slen > 0:
                    tar.fileobj.write(s)
                    hash_md5.update(s)
                if slen < block_size:
                    blocks, remainder = divmod(tarinfo.size, tarfile.BLOCKSIZE)
                    if remainder > 0:
                        tardata = tarfile.NUL * (tarfile.BLOCKSIZE - remainder)
                        tar.fileobj.write(tardata)
                        blocks += 1
                    tar.offset += blocks * tarfile.BLOCKSIZE
                    break
        md5 = hash_md5.hexdigest()
    else:
        md5 = None
    size = tarinfo.size
    mtime = datetime.utcfromtimestamp(tarinfo.mtime)
    return offset, size, mtime, md5


def hpss_get(hpss, file):
    """
    Gets a file from hpss

    Parameters:
        hpss (str): path to hpss system
        file (str): name of the file to get
    Returns:
        None
    """
    msg = 'Transferring from HPSS: {file}'.format(file=file)
    logging.info(msg)
    path, name = os.path.split(file)

    # Need to be in local directory for hsi get to work
    cwd = os.getcwd()
    if path != '':
        if not os.path.isdir(path):
            os.makedirs(path)
        os.chdir(path)

    # Transfer file using hsi get
    cmd = 'hsi -q "cd {path}; get {file}"'.format(
        path=hpss,
        file=name)
    proc = Popen(shlex.split(cmd), stdout=PIPE, stderr=PIPE)
    (stdout, stderr) = proc.communicate()
    status = proc.returncode
    if status != 0:
        msg = 'Transferring file from HPSS: {file}, non-zero exit code: {code}'.format(
            file=name, code=status)
        logging.error(msg)
        logging.debug('stdout:\n%s', stdout)
        logging.debug('stderr:\n%s', stderr)
        raise Exception

    # Back to original working directory
    if path != '':
        os.chdir(cwd)


def extractFiles(files, config):
    """
    Extract a list of files from a hpss tar

    Parameters:
        files (list(str)): a list of filenames
        config (Config): the master config object
    Returns:
        None
    """
    failures = []
    tfname = None
    newtar = True
    nfiles = len(files)
    for i in range(nfiles):

        file = files[i]

        # Open new tar archive
        if newtar:
            newtar = False
            tfname = os.path.join(config.cache, file[5])
            if not os.path.exists(tfname):
                # will need to retrieve from HPSS
                hpss_get(config.hpss, tfname)
            msg = 'Opening tar archive {file}'.format(file=tfname)
            logging.info(msg)
            tar = tarfile.open(tfname, "r")

        # Extract file
        msg = 'Extracting {file}'.format(file=file[1])
        logging.info(msg)
        try:

            # Seek file position
            tar.fileobj.seek(file[6])

            # Get next member
            tarinfo = tar.tarinfo.fromtarfile(tar)

            if tarinfo.isfile():
                # fileobj to extract
                fin = tar.extractfile(tarinfo)
                fname = tarinfo.name
                path, _ = os.path.split(fname)
                if path != '':
                    if not os.path.isdir(path):
                        os.makedirs(path)
                fout = open(fname, 'w')
                hash_md5 = hashlib.md5()
                while True:
                    s = fin.read(config.block_size)
                    slen = len(s)
                    if slen > 0:
                        fout.write(s)
                        hash_md5.update(s)
                    if slen < config.block_size:
                        break
                fin.close()
                fout.close()
                md5 = hash_md5.hexdigest()
                tar.chown(tarinfo, fname)
                tar.chmod(tarinfo, fname)
                tar.utime(tarinfo, fname)
                # Verify size
                if os.path.getsize(fname) != file[2]:
                    msg = 'size mismatch for: {file}'.format(file=fname)
                    logging.error(msg)
                # Verify md5 checksum
                if md5 != file[4]:
                    msg = extract_md5_mismatch.format(
                        file=fname,
                        extracted_md5=md5,
                        original_md5=file[4])
                    logging.error(msg)
                else:
                    msg = 'Valid md5: {hash} {file}'.format(
                        hash=md5, file=fname)
                    logging.debug(msg)

            else:
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
            msg = 'Error retrieving {file}'.format(file=file[1])
            logging.error(msg)

        # Close current archive?
        if i == nfiles - 1 or \
                files[i][5] != files[i + 1][5]:

            # Close current archive file
            msg = 'Closing tar archive {file}'.format(file=tfname)
            logging.debug(msg)
            tar.close()

            # Open new archive next time
            newtar = True


def hpss_put(hpss, file, keep=True):
    """
    Put file to hpss

    Parameters:
        hpss (str): path to hpss,
        file,
        keep (bool): if true keep the local file, otherwise remove
    Returns:
        None
    """
    msg = 'Transferring file to HPSS: {file}'.format(file=file)
    logging.info(msg)
    path, name = os.path.split(file)

    # Need to be in local directory for hsi put to work
    cwd = os.getcwd()
    if path != '':
        os.chdir(path)

    # Transfer file using hsi put
    cmd = 'hsi -q "cd {path}; put {file}"'.format(
        path=hpss,
        file=name)
    proc = Popen(shlex.split(cmd), stdout=PIPE, stderr=PIPE)
    (stdout, stderr) = proc.communicate()
    status = proc.returncode
    if status != 0:
        logging.error('Transferring file to HPSS: %s', name)
        logging.debug('stdout:\n%s', stdout)
        logging.debug('stderr:\n%s', stderr)
        raise Exception

    # Back to original working directory
    if path != '':
        os.chdir(cwd)

    # Remove local file if requested
    if not keep:
        os.remove(file)
