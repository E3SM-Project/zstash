********
Tutorial
********

.. note::
    This tutorial is specifically for ``zstash v0.4.2``.
    Some features have been added/changed since.
    Paths for E3SM Unified have also changed.

Some statements on this tutorial have been pulled from other pages of the
documentation. This is done to provide a comprehensive tutorial on a single page.
There is also an `accompanying video tutorial <https://youtu.be/kmdBdXa3rXo>`_.

What is Zstash?
===============

Zstash is an **HPSS long-term archiving** solution for E3SM.

Zstash is written entirely in Python using standard libraries.
Its design is intentionally minimalistic to provide an effective
long-term HPSS archiving solution without creating an overly complicated
(and hard to maintain) tool. For more information, see :ref:`index-label`,
`<https://e3sm.org/resources/tools/data-management/zstash-hpss-archive/>`_,
or `<https://e3sm.org/new-zstash-capabilities>`_.

Terminology
-----------
**zstash archive**:  A set of files {index.db, nnnnnn.tar where  nnnnnn=000000, 000001, …}

**HPSS**:  A long-term tape storage system.

**HPSS archive**: The zstash archive on HPSS.

**Local archive (or cache)**:  The zstash archive on our local file system. The default name is ``zstash``.

**Source directory**: the directory whose contents we are archiving.


Commands
--------

**Create**: create a new cache (local archive), create a tar file of the source directory's contents,
and if using HPSS, then store it on the HPSS archive.

**Check**: verify integrity of zstash archive (e.g., if using HPSS, check that files were uploaded on HPSS successfully.)

**Update**: add new or modified files to an existing zstash archive
(ignoring unmodified files).

**Extract**: extract files from an existing zstash archive into current directory.

**List (ls)**: view files in an existing zstash archive.

**Version**: show version number.


Options
-------
``--cache``, ``--hpss``, and ``--keep`` are covered in this tutorial.

A "x" indicates that the ``zstash`` command supports the given option.

.. list-table:: zstash options
    :header-rows: 1

    * - Option
      - Create
      - Check
      - Update
      - Extract
      - List

    * - ``--cache``
      - x
      - x
      - x
      - x
      - x

    * - ``--dry-run``
      -
      -
      - x
      -
      -

    * - ``--exclude``
      - x
      -
      - x
      -
      -

    * - ``--hpss``
      - x
      - x
      - x
      - x
      - x

    * - ``--keep``
      - x
      - x
      - x
      - x
      -

    * - ``-l``
      -
      -
      -
      -
      - x

    * - ``--maxsize``
      - x
      -
      -
      -
      -

    * - ``-v``
      - x
      - x
      - x
      - x
      - x

    * - ``--workers``
      -
      - x
      -
      - x
      -

Installation
============

Below is a basic guide to installation. For more information, see :ref:`getting-started`.

E3SM Unified Environment
------------------------

Zstash is available in the E3SM unified environment.
This is the recommended method of installation.

On Cori/NERSC: ::

    $ source /global/cfs/cdirs/e3sm/software/anaconda_envs/load_latest_e3sm_unified.sh

On Compy: ::

    $ source /share/apps/E3SM/conda_envs/load_latest_e3sm_unified.sh

For more information, see :ref:`getting-started`.

Installation with Conda
-----------------------
On Cori/NERSC: ::

    $ module load python/3.7-anaconda-2019.10
    $ source /global/common/cori_cle7/software/python/3.7-anaconda-2019.10/etc/profile.d/conda.sh
    $ conda create -n zstash_env -c e3sm -c conda-forge zstash
    $ conda activate zstash_env

On Compy: ::

    $ module load anaconda3/2019.03
    $ source /share/apps/anaconda3/2019.03/etc/profile.d/conda.sh
    $ conda create -n zstash_env -c e3sm -c conda-forge zstash
    $ conda activate zstash_env

On either machine, to install in an existing environment: ::

    $ conda install zstash -c e3sm -c conda-forge


HPSS
====

NERSC machines have HPSS access. We can choose to use HPSS by setting
``--hpss`` (or ``--hpss=none`` if we do not wish to archive to HPSS).

.. note::
    Before using zstash with HPSS for the first time, run ``hsi`` on NERSC
    and enter your credentials. Then, ``zstash`` will be able to access HPSS.
    Compy does not have HPSS access. Therefore, you’ll need to set
    ``--hpss=none`` when using it. For long term storage, zstash archives
    created locally on Compy should be transferred to an off-site HPSS storage using Globus
    (:ref:`globus-compy`)


Examples
========

We will use NERSC so that we have access to HPSS. The commands remain the same on Compy,
with the exception that ``--hpss`` will have to be set to ``none`` in all cases.


Open a NERSC terminal in two windows.

Setup
-----

We'll create a ``tutorial`` directory in ``$CSCRATCH`` on NERSC. We can create our tutorial
directory anywhere but ``CSCRATCH`` is a useful workspace. ::

    $ cd $CSCRATCH
    $ mkdir tutorial
    $ cd tutorial

Each example that follows is independent of the others. Therefore, we'll need to set up the
directory structure before each example. We'll call the following script ``setup.sh``: ::

    mkdir zstash_demo
    mkdir zstash_demo/empty_dir
    mkdir zstash_demo/dir
    echo 'file0 stuff' > zstash_demo/file0.txt
    echo '' > zstash_demo/file_empty.txt
    echo 'file1 stuff' > zstash_demo/dir/file1.txt


In one NERSC window, create this script and run it (``./setup.sh``).
That will create the following directory structure:

* zstash_demo/

    * dir/

        * file1.txt (contains 'file1 stuff')

    * empty_dir/
    * file0.txt (contains 'file0 stuff')
    * file_empty.txt (empty)

In some examples, we'll also want to add files after running some ``zstash`` commands.
We'll call the following script ``add_files.sh``: ::

    mkdir zstash_demo/dir2
    echo 'file2 stuff' > zstash_demo/dir2/file2.txt
    echo 'file1 stuff with changes' > zstash_demo/dir/file1.txt

If this is run (``./add_files.sh``) immediately after running ``setup.sh``, then we would have the
following directory structure:

* zstash_demo/

    * dir/

        * file1.txt (contains 'file1 stuff with changes')

    * dir2/

        * file2.txt (contains 'file2 stuff')

    * empty_dir/
    * file0.txt (contains 'file0 stuff')
    * file_empty.txt (empty)

In our second window, we'll log into HPSS with ``hsi``.

.. note::
    If this is your first time using HPSS, you'll have to enter your credentials.
    If you haven't used HPSS before, ``ls`` should print nothing.

After every example, we'll want to remove the directories we created both in our workspace
(``$CSCRATCH/tutorial``) and on HPSS.

Simple Case
-----------

This simple case will illustrate ``create``, ``update``, and ``extract``.

Set up the directory structure: ::

    $ ./setup.sh

**Create**:

Now, we will create the ``zstash`` archives -- both locally and on HPSS.
Note that the path passed to ``--hpss`` can be a **relative** path from ``/home/f/<username>`` or
an absolute path `on HPSS, not the local file system`.
``zstash_demo`` is the source directory whose contents we want to archive.
This is just a directory path; it can be a relative path and can contain ``..`` or
it can be an absolute path. ::

    $ zstash create --hpss=zstash_archive zstash_demo

This will output the following: ::

    INFO: Gathering list of files to archive
    INFO: Creating new tar archive 000000.tar
    INFO: Archiving file0.txt
    INFO: Archiving file_empty.txt
    INFO: Archiving dir/file1.txt
    INFO: Archiving empty_dir
    INFO: Transferring file to HPSS: zstash/000000.tar
    INFO: Transferring file to HPSS: zstash/index.db

The "cache" is our local archive. HPSS is our long-term archive.
Our cache is given the default name ``zstash`` and can be found in the source directory,
``zstash_demo``. We've also created an archive in HPSS named ``zstash_archive``.

The cache ``zstash_demo/zstash`` now contains ``index.db``,
which is the sqlite database used by ``zstash``.

In our HPSS window, ``ls zstash_archive`` will show ``000000.tar`` in addition to ``index.db``.
The former is the tar file created from the files and directories in ``zstash_demo``.

Note that the maximum size of tar files will default to 256 GB, unless ``--maxsize`` is passed in
with a different value to ``zstash create``.

**Update**:

Now, let's add some files to our source directory, ``zstash_demo``.
We want to update our HPSS archive with these
changes, so we will enter the directory ``zstash_demo`` and tell ``zstash`` to update it.
Note that we have to be inside the source directory to run ``zstash update``.
We also have to specify ``--hpss`` so that ``zstash`` will know which HPSS archive to update. ::

    $ ./add_files.sh
    $ cd zstash_demo
    $ zstash update --hpss=zstash_archive

This will output the following: ::

    INFO: Gathering list of files to archive
    INFO: Creating new tar archive 000001.tar
    INFO: Archiving dir/file1.txt
    INFO: Archiving dir2/file2.txt
    INFO: Transferring file to HPSS: zstash/000001.tar
    INFO: Transferring file to HPSS: zstash/index.db

In our HPSS window, notice that ``ls zstash_archive`` now includes a new tar file ``000001.tar``.
``zstash update`` produces a new tar file for each update.
Nothing will have changed in ``zstash_demo/zstash``, however.

**Extract**:

Now, we will extract the files from the HPSS archive. We will extract into a new directory
because ``zstash`` will not extract files that are already present. So, we will exit the
``zstash_demo`` directory and create a new directory. Extraction will always occur
in the current directory. ::

    $ cd ..
    $ mkdir zstash_extraction
    $ cd zstash_extraction
    $ zstash extract --hpss=zstash_archive

This will output the following: ::

    INFO: Transferring file from HPSS: zstash/index.db
    INFO: Transferring file from HPSS: zstash/000000.tar
    INFO: Opening tar archive zstash/000000.tar
    INFO: Extracting file0.txt
    INFO: Extracting file_empty.txt
    INFO: Extracting empty_dir
    INFO: Transferring file from HPSS: zstash/000001.tar
    INFO: Opening tar archive zstash/000001.tar
    INFO: Extracting dir/file1.txt
    INFO: Extracting dir2/file2.txt
    INFO: No failures detected when extracting the files.

The contents of ``zstash_extraction`` will be identical to those of ``zstash_demo``.

**Cleanup**:

Now, we will clean up for the next example.

In our NERSC window: ::

    $ cd ..
    $ rm -r zstash_demo/ zstash_extraction/

In our HPSS window (``rm -r`` doesn't work on HPSS): ::

    $ rm zstash_archive/000000.tar zstash_archive/000001.tar zstash_archive/index.db
    $ rmdir zstash_archive

No HPSS
-------

This example will again illustrate ``create``, ``update``, and ``extract``,
but this time without using HPSS.

Set up the directory structure: ::

    $ ./setup.sh

**Create**:

Now, we will create the local ``zstash`` archive, but skip creating a HPSS archive. ::

    $ zstash create --hpss=none zstash_demo

This will output the following: ::

    INFO: Gathering list of files to archive
    INFO: Creating new tar archive 000000.tar
    INFO: Archiving file0.txt
    INFO: Archiving file_empty.txt
    INFO: Archiving dir/file1.txt
    INFO: Archiving empty_dir
    INFO: put: HPSS is unavailable
    INFO: put: Keeping tar files locally and removing write permissions
    INFO: zstash/000000.tar original mode=b"'660'"
    INFO: zstash/000000.tar new mode=b"'440'"
    INFO: put: HPSS is unavailable

``zstash_demo/zstash`` (the cache, located in the source directory) is our only archive now.
It contains both ``index.db`` and ``000000.tar``.
In our HPSS window, ``ls`` will not show ``zstash_archive``.
Our cache has completely replaced the HPSS archive we used in the simple case.

**Update**:

Now, we can add some more files and update this local archive. ::

    $ ./add_files.sh
    $ cd zstash_demo
    $ zstash update --hpss=none

This will output the following: ::

    INFO: Gathering list of files to archive
    INFO: Creating new tar archive 000001.tar
    INFO: Archiving dir/file1.txt
    INFO: Archiving dir2/file2.txt
    INFO: put: HPSS is unavailable
    INFO: put: Keeping tar files locally and removing write permissions
    INFO: zstash/000001.tar original mode=b"'660'"
    INFO: zstash/000001.tar new mode=b"'440'"
    INFO: put: HPSS is unavailable

``000001.tar`` has been added to ``zstash_demo/zstash``.
In our HPSS window, ``ls`` still shows no archive created from this example.

**Extract**:

Now, we will extract the files from this local archive. Again, we will extract into a new directory
because ``zstash`` will not extract files that are already present. So, we will exit the
``zstash_demo`` directory and create a new directory. Unlike in the simple case however,
we will need to copy the cache ``zstash_demo/zstash`` into this new directory. We have
to do this because with ``--hpss=none``, ``zstash`` simply extracts from the cache
in the current directory, rather than extracting from an HPSS archive. We could also
pass in ``--cache=../zstash_demo/zstash``; ``--cache`` will be discussed in the next section. ::

    $ cd ..
    $ mkdir zstash_extraction
    $ cd zstash_extraction
    $ cp -r ../zstash_demo/zstash/ zstash
    $ zstash extract --hpss=none

This will output the following: ::

    INFO: Opening tar archive zstash/000000.tar
    INFO: Extracting file0.txt
    INFO: Extracting file_empty.txt
    INFO: Extracting empty_dir
    INFO: Opening tar archive zstash/000001.tar
    INFO: Extracting dir/file1.txt
    INFO: Extracting dir2/file2.txt
    INFO: No failures detected when extracting the files.

As with the simple case, the contents of ``zstash_extraction``
will be identical to those of ``zstash_demo``. We still had to specify ``--hpss=none`` because
if we had kept ``--hpss=hpss_archive``, then ``zstash`` would have tried to extract from a HPSS
archive that doesn't exist.

**Cleanup**:

Now, we will clean up for the next example.

In our NERSC window (use the ``-f`` option to force deletion of tar files): ::

    $ cd ..
    $ rm -rf zstash_demo/ zstash_extraction/

There's nothing to clean up in our HPSS window.

Change Cache Name
-----------------

This example will again illustrate ``create``, ``update``, and ``extract``,
but this time using a different cache.

Set up the directory structure: ::

    $ ./setup.sh

**Create**:

Now, we will create the ``zstash`` archives -- both locally and on HPSS.
This time, however, our local archive will be named ``my_cache`` instead ``zstash``.
``--cache`` just takes a directory path; it can
be a relative path and can contain ``..`` or it can be an absolute path. `However,` it
is recommended that however the path is written, it places the cache in the source
directory (in this case, ``zstash_demo``). ::

    $ zstash create --hpss=zstash_archive --cache=my_cache zstash_demo
    zstash create --hpss=zstash_archive --cache=/global/cscratch1/sd/forsyth/tutorial/zstash_demo/my_cache zstash_demo

This will output the following: ::

    INFO: Gathering list of files to archive
    INFO: Creating new tar archive 000000.tar
    INFO: Archiving file0.txt
    INFO: Archiving file_empty.txt
    INFO: Archiving dir/file1.txt
    INFO: Archiving empty_dir
    INFO: Transferring file to HPSS: my_cache/000000.tar
    INFO: Transferring file to HPSS: my_cache/index.db

``zstash_demo/my_cache`` will now contain ``index.db`` just as ``zstash_demo/zstash``
did in the simple case.

As in the simple case, in our HPSS window, ``ls zstash_archive`` shows both
``000000.tar`` and ``index.db``.

**Update**:

We will now add more files and update the archives. ::

    $ ./add_files.sh
    $ cd zstash_demo
    $ zstash update --hpss=zstash_archive --cache=my_cache

This will output the following: ::

    INFO: Gathering list of files to archive
    INFO: Creating new tar archive 000001.tar
    INFO: Archiving dir/file1.txt
    INFO: Archiving dir2/file2.txt
    INFO: Transferring file to HPSS: my_cache/000001.tar
    INFO: Transferring file to HPSS: my_cache/index.db

In our HPSS window, notice that ``ls zstash_archive`` now includes a new tar file ``000001.tar``.
As in the simple case, nothing will have changed in the cache
(named ``zstash_demo/my_cache`` here).

**Extract**:

Now we will extract the files: ::

    $ cd ..
    $ mkdir zstash_extraction
    $ cd zstash_extraction
    $ zstash extract --hpss=zstash_archive --cache=my_cache

This will output the following: ::

    INFO: Transferring file from HPSS: my_cache/index.db
    INFO: Transferring file from HPSS: my_cache/000000.tar
    INFO: Opening tar archive my_cache/000000.tar
    INFO: Extracting file0.txt
    INFO: Extracting file_empty.txt
    INFO: Extracting empty_dir
    INFO: Transferring file from HPSS: my_cache/000001.tar
    INFO: Opening tar archive my_cache/000001.tar
    INFO: Extracting dir/file1.txt
    INFO: Extracting dir2/file2.txt
    INFO: No failures detected when extracting the files.

As in the simple case, the contents of ``zstash_extraction`` will be identical to those of
``zstash_demo``. The difference is that this time the cache will be named ``my_cache`` in both
``zstash_demo`` and ``zstash_extraction``.

**Cleanup**:

Now, we will clean up for the next example.

In our NERSC window: ::

    $ cd ..
    $ rm -r zstash_demo/ zstash_extraction/

In our HPSS window (``rm -r`` doesn't work on HPSS): ::

    $ rm zstash_archive/000000.tar zstash_archive/000001.tar zstash_archive/index.db
    $ rmdir zstash_archive

Keep Files
----------

This example will again illustrate ``create``, ``update``, and ``extract``,
but this time storing the tar files in the local archive (the cache) in addition to the
HPSS archive. In the ``--hpss=none`` example, we saw that when there was no HPSS archive
being used, the cache contained tar files. The ``--keep`` option allows us to store these files
locally even if we're using HPSS. If ``--keep`` is used with ``--hpss=none``, there won't be a
noticeable effect since the latter keeps tar files by default.

Set up the directory structure: ::

    $ ./setup.sh

**Create**:

Now, we will create the ``zstash`` archives -- both locally and on HPSS.
This time, however, we will specify that the cache should keep the tar file created
by ``zstash create``. ::

    $ zstash create --hpss=zstash_archive --keep zstash_demo

This will output the following: ::

    INFO: Gathering list of files to archive
    INFO: Creating new tar archive 000000.tar
    INFO: Archiving file0.txt
    INFO: Archiving file_empty.txt
    INFO: Archiving dir/file1.txt
    INFO: Archiving empty_dir
    INFO: Transferring file to HPSS: zstash/000000.tar
    INFO: Transferring file to HPSS: zstash/index.db

``zstash_demo/zstash`` and our HPSS archive ``zstash_archive`` now have identical contents:
``000000.tar`` and ``index.db``. In the simple case, the former would not have contained
the tar file.

**Update**:

Now, we will add more files and update the archives. ::

    $ ./add_files.sh
    $ cd zstash_demo
    $ zstash update --hpss=zstash_archive --keep

This will output the following: ::

    INFO: Gathering list of files to archive
    INFO: Creating new tar archive 000001.tar
    INFO: Archiving dir/file1.txt
    INFO: Archiving dir2/file2.txt
    INFO: Transferring file to HPSS: zstash/000001.tar
    INFO: Transferring file to HPSS: zstash/index.db

Again, ``zstash_demo/zstash`` and our HPSS archive ``zstash_archive``
will have identical contents: ``000001.tar`` in addition to ``000000.tar`` and ``index.db``.
In the simple case, the former would not have the tar file added.

**Extract**:

Now, we will extract the files from the HPSS archive. ::

    $ cd ..
    $ mkdir zstash_extraction
    $ cd zstash_extraction
    $ zstash extract --hpss=zstash_archive --keep

This will output the following: ::

    INFO: Transferring file from HPSS: zstash/index.db
    INFO: Transferring file from HPSS: zstash/000000.tar
    INFO: Opening tar archive zstash/000000.tar
    INFO: Extracting file0.txt
    INFO: Extracting file_empty.txt
    INFO: Extracting empty_dir
    INFO: Transferring file from HPSS: zstash/000001.tar
    INFO: Opening tar archive zstash/000001.tar
    INFO: Extracting dir/file1.txt
    INFO: Extracting dir2/file2.txt
    INFO: No failures detected when extracting the files.

As in the simple case, the contents of ``zstash_extraction`` will be identical to those of
``zstash_demo``. The difference is that this time ``zstash_demo/zstash`` contains tar files
and thus so too does ``zstash_extraction/zstash``, whereas this was not the case in
the simple case.

**Cleanup**:

Now, we will clean up for the next example.

In our NERSC window: ::

    $ cd ..
    $ rm -r zstash_demo/ zstash_extraction/

In our HPSS window (``rm -r`` doesn't work on HPSS): ::

    $ rm zstash_archive/000000.tar zstash_archive/000001.tar zstash_archive/index.db
    $ rmdir zstash_archive

Check File Integrity
--------------------

This example will demonstrate how to check that files haven't been damaged during archive or transfer.

**Create**:

First, let's create local and HPSS archives as in the simple case: ::

    $ ./setup.sh
    $ zstash create --hpss=zstash_archive zstash_demo

**Check**:

Now, we will check the file integrity. Note that we'll want to enter the directory where our
local cache (``zstash``) is. If we run ``zstash check`` in another directory,
a local cache (named ``zstash``) will be created in that directory. ::

    $ cd zstash_demo
    $ zstash check --hpss=zstash_archive

This will output the following: ::

    INFO: Transferring file from HPSS: zstash/000000.tar
    INFO: Opening tar archive zstash/000000.tar
    INFO: Checking file0.txt
    INFO: Checking file_empty.txt
    INFO: Checking dir/file1.txt
    INFO: Checking empty_dir
    INFO: No failures detected when checking the files.

No failures were detected, so the file integrity has been confirmed. This was done by verifying their md5 checksums
against the original ones stored in the database.

``zstash_demo/zstash`` will still only contain ``index.db`` and no files will have been added to
``zstash_demo``.

In our HPSS window, ``ls zstash archive`` will show ``000000.tar`` and ``index.db``.

**Cleanup**:

Now, we will clean up for the next example.

In our NERSC window: ::

    $ cd ..
    $ rm -r zstash_demo/

In our HPSS window (``rm -r`` doesn't work on HPSS): ::

    $ rm zstash_archive/000000.tar zstash_archive/index.db
    $ rmdir zstash_archive


List Files in HPSS Archive
---------------------------

This example will demonstrate how to list the files contained in the tar files in the HPSS archive.

**Create**:

First, let's create local and HPSS archives as in the simple case: ::

    $ ./setup.sh
    $ zstash create --hpss=zstash_archive zstash_demo

Recall from the simple case that the cache ``zstash_demo/zstash`` now contains ``index.db`` and
that ``zstash_archive`` will contain ``000000.tar`` in addition to ``index.db``.

**List**:

We can check the contents of the tar file by running the following.
Note that it's good to be in the directory with the cache
(in this case ``zstash_demo`` contains the cache ``zstash``).
If we run ``zstash ls`` in another directory, a local cache (named ``zstash``) will be created
in that directory. ::

    $ cd zstash_demo
    $ zstash ls --hpss=zstash_archive

This will output the following: ::

    file0.txt
    file_empty.txt
    dir/file1.txt
    empty_dir

Compare to the output of ``ls -R .`` (i.e., list all files in ``zstash_demo``): ::

    dir  empty_dir  file0.txt  file_empty.txt  zstash

    ./dir:
    file1.txt

    ./empty_dir:

    ./zstash:
    index.db

``zstash ls`` lists the same files but excludes the cache (in this case, named ``zstash``).

**Extract**:

Now, let's extract the files. ::

    $ cd ..
    $ mkdir zstash_extraction
    $ cd zstash_extraction
    $ zstash extract --hpss=zstash_archive

This will output the following: ::

    INFO: Transferring file from HPSS: zstash/index.db
    INFO: Transferring file from HPSS: zstash/000000.tar
    INFO: Opening tar archive zstash/000000.tar
    INFO: Extracting file0.txt
    INFO: Extracting file_empty.txt
    INFO: Extracting dir/file1.txt
    INFO: Extracting empty_dir
    INFO: No failures detected when extracting the files.

**List**:

Now that we have extracted files, let's list the files in the archive again. ::

    $ cd ..
    $ cd zstash_demo
    $ zstash ls --hpss=zstash_archive

This will output the following: ::

    file0.txt
    file_empty.txt
    dir/file1.txt
    empty_dir

We can see that extraction did not alter what ``zstash ls`` displays, since extraction does not
change the contents of the HPSS archive.

**Cleanup**:

Now, we will clean up for the next example.

In our NERSC window: ::

    $ cd ..
    $ rm -r zstash_demo/ zstash_extraction/

In our HPSS window (``rm -r`` doesn't work on HPSS): ::

    $ rm zstash_archive/000000.tar zstash_archive/index.db
    $ rmdir zstash_archive

Archiving E3SM Data Output
--------------------------

Now, instead of using placeholder data, let's archive actual data from E3SM. Archiving large amounts
of data can take several days, so we'll use a data transfer node to improve performance and
``screen`` so that we can detatch and reattach without stopping ``zstash``. For this example,
however, we'll use a small amount of data so ``zstash`` finishes in a reasonable amount of time.

We'll call our directory ``e3sm_output`` this time instead of ``zstash_demo``. We'll enter this
directory and copy over a couple ``nc`` files from the data repository on NERSC. ::

    $ mkdir e3sm_output
    $ cd e3sm_output
    $ cp /global/cfs/cdirs/e3smpub/E3SM_simulations/20180215.DECKv1b_H1.ne30_oEC.edison/archive/atm/hist/20180215.DECKv1b_H1.ne30_oEC.edison.cam.h0.1850-01.nc .
    $ cp /global/cfs/cdirs/e3smpub/E3SM_simulations/20180215.DECKv1b_H1.ne30_oEC.edison/archive/atm/hist/20180215.DECKv1b_H1.ne30_oEC.edison.cam.h0.1850-02.nc .

**Create**:

Now, let's enter the data transfer node, use ``screen``, install ``zstash``, and begin archiving. ::

    $ ssh dtn01.nersc.gov
    $ screen
    $ bash
    $ source /global/cfs/cdirs/e3sm/software/anaconda_envs/load_latest_e3sm_unified.sh
    $ cd $CSCRATCH/tutorial
    $ zstash create --hpss=zstash_archive e3sm_output

If the archiving is taking a long time, we could enter the keys ``CTRL-A`` and then ``CTRL-D`` to
detach and ``exit`` to get out of the data transfer node. We could reattach with: ::

    $ ssh dtn01.nersc.gov
    $ screen -r

For more information on archiving E3SM output, see :ref:`best-practices`.

**Cleanup**:

Now, we will clean up.

In our NERSC window (not on the data transfer node): ::

    $ cd ..
    $ rm -r e3sm_output

In our HPSS window (``rm -r`` doesn't work on HPSS): ::

    $ rm zstash_archive/000000.tar zstash_archive/index.db
    $ rmdir zstash_archive
