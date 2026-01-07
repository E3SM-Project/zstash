*****
Usage
*****

.. highlight:: none

If running on Cori, it is preferable to run from ``$CSCRATCH`` rather than
``/global/homes``. Running from the latter may result in a
'Resource temporarily unavailable' error.

.. note::
    If you have not logged into HSI before, you will have to do so before running ``zstash`` with HPSS.
    On NERSC machines, just run ``hsi`` on the command line and enter your credentials.
    Note that ``hsi`` is only available on the log-in nodes, not the compute nodes.

.. note::
   If you set ``--hpss=none``, there is no need to set ``--keep``, as the former option includes the effects of the latter.

.. warning::
    When specifying files, wildcards should be enclosed in double quotes (e.g., ``"a*"``).

.. warning::
    Specifying a high number for ``--workers`` will result in slow downloads for each of the tars since your bandwidth
    is limited. User discretion is advised.

Create
======

To create a new zstash archive: ::

   $ zstash create --hpss=<path to HPSS> <local path>

where

* ``--hpss=<path to HPSS>`` specifies the destination path on the HPSS file 
  system where the archive files will be stored. This directory should be **unique** for each 
  zstash archive. If ``--hpss=none``, then files will be archived locally instead of being
  transferred to HPSS. The ``none`` option should be used when running Zstash on a machine
  without HPSS. The option also accepts a Globus URL, ``globus://<Globus endpoint UUID/<path to archive>``.
  Then zstash will use `Globus <https://globus.org/>`_ to store a new zstash archive on a Globus endpoint.
  Names ``alcf`` and ``nersc`` are recognized as referring to the ALCF HPSS and NERSC HPSS endpoints,
  e.g. ``globus://nersc/~/my_archive``.
* ``<local path>`` specifies the path to the local directory that should be archived.

Additional optional arguments:

* ``--cache`` to use a cache other than the default of ``zstash``. If hpss is ``--hpss=none``, then this will be the archive.
* ``--exclude`` comma separated list of file patterns to exclude
* ``--follow-symlinks`` Hard copy symlinks. This is useful for preventing broken links. Note that a broken link will result in a failed create.
* ``--include`` comma separated list of file patterns to include
* ``--keep`` to keep a copy of the tar files on the local file system after 
  they have been transferred to HPSS. Normally, they are deleted after 
  successful transfer.
* ``--maxsize MAXSIZE`` specifies the maximum size (in GB) for tar files. 
  The default is 256 GB. Zstash will create tar files that are smaller 
  than MAXSIZE except when individual input files exceed MAXSIZE (as 
  individual files are never split up between different tar files).
* ``--non-blocking`` Zstash will submit a Globus transfer and immediately create a subsequent tarball. That is, Zstash will not wait until the transfer completes to start creating a subsequent tarball. On machines where it takes more time to create a tarball than transfer it, each Globus transfer will have one file. On machines where it takes less time to create a tarball than transfer it, the first transfer will have one file, but the number of tarballs in subsequent transfers will grow finding dynamically the most optimal number of tarballs per transfer. NOTE: zstash is currently always non-blocking.
* ``--error-on-duplicate-tar`` FOR ADVANCED USERS ONLY: Raise an error if a tar file with the same name already exists in the database. If this flag is set, zstash will exit if it sees a duplicate tar. If it is not set, zstash's behavior will depend on whether or not the --overwrite-duplicate-tar flag is set.
* ``--overwrite-duplicate-tars`` FOR ADVANCED USERS ONLY: If a duplicate tar is encountered, overwrite the existing database record with the new one (i.e., it will assume the latest tar is the correct one). If this flag is not set, zstash will permit multiple entries for the same tar in its database.
* ``-v`` increases output verbosity.

Local tar files as well as the sqlite3 index database (index.db) will be stored
under ``<local path>/zstash``.

**After you run** ``zstash create`` **it's highly recommended that you
run** ``zstash check``, **detailed in the section below. This will allow you to check that your archival completed successfully. Do not delete any data until you've run** ``zstash check``.

What Happens If: Create Scenarios
----------------------------------

**Q: What happens to tar files after they're created?**

A: It depends on your ``--hpss`` and ``--keep`` settings:

* **With HPSS transfer (default)**: Tar files are created in the local cache, transferred to HPSS, then deleted from the local cache
* **With HPSS transfer + ``--keep``**: Tar files remain in the local cache after transfer
* **With ``--hpss=none``**: Tar files always remain in the local cache (``--keep`` is automatically enabled)

Example showing the difference: ::

  $ zstash create --hpss=my_archive source_dir
  # After completion: source_dir/zstash/ contains only index.db
  
  $ zstash create --hpss=my_archive --keep source_dir
  # After completion: source_dir/zstash/ contains index.db and all tar files

**Q: What happens if I use ``--follow-symlinks``?**

A: The behavior of symlinks depends on this flag:

* **Without ``--follow-symlinks`` (default)**: Symlinks are preserved as symlinks in the archive
* **With ``--follow-symlinks``**: Symlinks are dereferenced and the actual file content is archived
* **Broken symlinks without ``--follow-symlinks``**: The broken symlink is preserved in the archive
* **Broken symlinks with ``--follow-symlinks``**: Archive creation fails with a ``FileNotFoundError``

Example: ::

  $ ln -s /path/to/original.txt source_dir/link.txt
  $ zstash create --hpss=none source_dir
  # link.txt remains a symlink in the archive
  
  $ zstash create --hpss=none --follow-symlinks source_dir
  # Contents of original.txt are archived, link.txt becomes a regular file
  
  $ rm /path/to/original.txt  # Break the symlink
  $ zstash create --hpss=none --follow-symlinks source_dir
  # ERROR: FileNotFoundError - archive creation fails

**Q: What happens if I specify a custom cache location?**

A: Using ``--cache=my_cache`` changes where zstash stores its working files:

* Default: ``source_dir/zstash/`` contains ``index.db`` and temporary tar files
* Custom: ``source_dir/my_cache/`` contains ``index.db`` and temporary tar files
* You must use the same ``--cache`` value for all operations on that archive

Example: ::

  $ zstash create --hpss=none --cache=my_custom_cache source_dir
  $ ls source_dir/my_custom_cache/
  index.db  000000.tar

Basic example
-------------

To **archive** output from an E3SM simulation located
under `$CSCRATCH/ACME_simulations/20170731.F20TR.ne30_ne30.edison`::

  $ cd $CSCRATCH/ACME_simulations/20170731.F20TR.ne30_ne30.edison
  $ zstash create --hpss=test/E3SM_simulations/20170731.F20TR.ne30_ne30.edison .

Once done, you should see the archive files on hsi: ::

  $ hsi
  > cd test/E3SM_simulations/20170731.F20TR.ne30_ne30.edison
  > ls 
  000000.tar   index.db

The data from this test simulation is small, so in this case there is only a single tar 
file (000000.tar) and the index database (index.db).

Examples excluding some files
-----------------------------

You may decide that certain files do not need to be archived.
For example, if you want to **exclude \*.o and \*.mod files** under the build
subdirectory: ::

  $ cd $CSCRATCH/ACME_simulations/20170731.F20TR.ne30_ne30.edison
  $ zstash create --hpss=test/ACME_simulations/20170731.F20TR.ne30_ne30.edison \
    --exclude="build/*/*.o","build/*/*.mod" .

Or you may decide that you only want to **archive restart files every 5 years**
to conserve storage space: ::

  $ cd $CSCRATCH/ACME_simulations/20170731.F20TR.ne30_ne30.edison
  $ zstash create --hpss=test/ACME_simulations/20170731.F20TR.ne30_ne30.edison \
    --exclude="archive/rest/???[!05]-*/" .

This exclude pattern will skip all restart subdirectories under the short-term archive,
except for those with years ending in '0' or '5'.

Example with Globus
-------------------
If you run zstash on the system without the HPSS file system, but has a `Globus <https://app.globus.org/endpoints>`_ endpoint set up,
you can use a Globus URL: ::

  $ cd $CSCRATCH/ACME_simulations/20170731.F20TR.ne30_ne30.anvil
  $ zstash create --hpss=globus://9cd89cfd-6d04-11e5-ba46-22000b92c6ec/~/test/E3SM_simulations/20170731.F20TR.ne30_ne30.anvil .

9cd89cfd-6d04-11e5-ba46-22000b92c6ec is the NERSC HPSS Globus endpoint UUID. Two names ``nersc`` and ``alcf``
are recognized by zstash and substituted internally with a corresponding Globus UUID
for the NERSC HPSS Globus endpoint (9cd89cfd-6d04-11e5-ba46-22000b92c6ec) and
the ALCF HPSS Globus endpoint (de463ec4-6d04-11e5-ba46-22000b92c6ec) endpoint.
If you want to store zstash archive on these two remote HPSS file systems, you can use the names instead of UUIDs: ::

  $ zstash create --hpss=globus://nersc/~/test/E3SM_simulations/20170731.F20TR.ne30_ne30.anvil .

.. note::
    If you are a new Globus user, you should first do a small transfer to test functionality.

.. note::
    Always activate Globus endpoints via the Globus web interface before running ``zstash``.

.. note::
    Globus authentication is required once per endpoint. After the first authentication, tokens are saved locally and reused for subsequent operations.

Check
=====

Note: Most of the commands for this are the same for ``zstash extract`` and ``zstash ls``.

To verify that your files were uploaded on HPSS successfully,
go to a **new, empty directory** and run: ::

   $ zstash check --hpss=<path to HPSS> [--workers=<num of processes>] [--cache=<cache>] [--keep] [-v] [files]

where

* ``--hpss=<path to HPSS>`` specifies the destination path on the HPSS file system. If ``--hpss=none``,
  then ``zstash check`` will check the cache.
* ``--workers=<num of processes>`` an optional argument which specifies the number of
  processes to use, resulting in checking being done in parallel.
  **Using a high number will result in slow downloads for each of the tars since your bandwidth is limited.**
  **User discretion is advised.**
* ``--cache`` to use a cache other than the default of ``zstash``.
* ``--keep`` to keep a copy of the tar files on the local file system after
  they have been extracted from the archive. Normally, they are deleted after
  successful transfer.
* ``--retries`` to set the number of times to retry ``hsi get`` if it is unsuccessful.
  The default is 1 retry (2 tries total). Note: for a retry to occur automatically because of
  an incomplete tar file, then the archive you're checking
  must have been created using ``zstash >= v1.1.0``.
* ``--tars`` to specify specific tars to check. See below for example usage.
* ``--error-on-duplicate-tar`` FOR ADVANCED USERS ONLY: Raise an error if a tar file with the same name already exists in the database. If this flag is set, zstash will exit if it sees a duplicate tar. If it is not set, zstash will check if the size matches the *most recent* entry.
* ``-v`` increases output verbosity.
* ``[files]`` is a list of files to check (standard wildcards supported).

  * Leave empty to check all the files.
  * List of files with support for wildcards. Please note that any expression
    containing **wildcards should be enclosed in double quotes ("...")** 
    to avoid shell substitution.
  * Names of specific tar archives to check all files within these tar archives.


``zstash check`` will download the tar archives to the local disk cache (under 
the `zstash/` subdirectory) and verify the md5 checksum against the checksum 
stored in the index database (`index.db`).

After the check is complete, a list of all corrupted files in the HPSS archive,
along with the tar archive they belong is listed. Below is an example:  ::

    INFO: Opening tar archive zstash/000000.tar
    INFO: Checking archive/atm/hist/20180129.DECKv1b_piControl.ne30_oEC.edison.cam.h0.0001-01.nc
    DEBUG: Valid md5: cfb388d9c4ffe3bf45985fa470855801 archive/atm/hist/20180129.DECKv1b_piControl.ne30_oEC.edison.cam.h0.0001-01.nc
    INFO: Checking archive/atm/hist/20180129.DECKv1b_piControl.ne30_oEC.edison.cam.h0.0001-02.nc
    DEBUG: Valid md5: ce9bb79fb60fdef2ca4c2c29afc54776 archive/atm/hist/20180129.DECKv1b_piControl.ne30_oEC.edison.cam.h0.0001-02.nc
    ...
    ERROR: Encountered an error for files:
    ERROR: archive/atm/hist/20180129.DECKv1b_piControl.ne30_oEC.edison.cam.h0.0214-06.nc in 00000a.tar
    ERROR: archive/atm/hist/20180129.DECKv1b_piControl.ne30_oEC.edison.cam.h0.0214-07.nc in 00000a.tar
    ERROR: archive/atm/hist/20180129.DECKv1b_piControl.ne30_oEC.edison.cam.h0.0214-08.nc in 00000a.tar
    ...
    ERROR: archive/ocn/hist/mpaso.hist.am.timeSeriesStatsMonthly.0085-08-01.nc in 000029.tar
    ERROR: archive/ocn/hist/mpaso.hist.am.timeSeriesStatsMonthly.0085-09-01.nc in 000029.tar
    ERROR: The following tar archives had errors:
    ERROR: 00000a.tar
    ERROR: 000029.tar

If you encounter an error, **save your original data**.
You may need to reupload it via ``zstash create``.
Please contact the zstash development team, we're working on
identifying what causes these issues.

What Happens If: Check Scenarios
---------------------------------

**Q: What happens if I run check with ``--hpss=none``?**

A: ``zstash check --hpss=none`` checks files against the local cache without accessing HPSS. This requires:

* The local cache must contain both ``index.db`` and the tar files
* Useful for verifying local archives or testing without HPSS access
* Tar files will be deleted after checking unless ``--keep`` is specified

Example: ::

  $ zstash create --hpss=none --keep source_dir
  $ cd source_dir
  $ zstash check --hpss=none
  # Checks files against local tar files in source_dir/zstash/

**Q: What happens with ``--keep`` during check?**

A: The ``--keep`` flag determines whether tar files remain in the cache after checking:

* **Without ``--keep``**: Tar files are downloaded, checked, then deleted
* **With ``--keep``**: Tar files remain in the local cache after checking
* **With ``--hpss=none``**: Tar files are never deleted (``--keep`` is implicit)

Example: ::

  $ zstash check --hpss=my_archive --keep
  # After completion: zstash/ directory contains index.db and all checked tar files

**Q: What happens with parallel checking (``--workers``)?**

A: Using ``--workers=N`` processes multiple tar files concurrently:

* Faster overall checking time
* Each worker processes complete tar files (tars are not split)
* Tars are processed in sorted order even with parallel execution
* High worker counts may slow individual downloads due to bandwidth limits

Example: ::

  $ zstash check --hpss=my_archive --workers=3
  # Three tar files are checked concurrently

**Q: What does ``--tars`` do?**

A: The ``--tars`` option limits checking to specific tar archives:

* Cannot be combined with the ``[files]`` argument
* Supports ranges, individual files, and combinations
* Useful for checking specific subsets of large archives

See "Example usage of ``--tars``" below for syntax examples.

**Q: What happens if duplicate tar entries exist in the database?**

A: This indicates database corruption. Behavior depends on your flags:

* **With ``--error-on-duplicate-tar``**: Check exits immediately with an error
* **Without ``--error-on-duplicate-tar`` (default)**: Check validates against the most recent entry
* If most recent entry's size matches the actual file: Check passes
* If most recent entry's size doesn't match: Check fails and reports the mismatch

Example: ::

  $ zstash check --hpss=my_archive
  WARNING: Database corruption detected! Found 2 database entries for 000001.tar
  INFO: 000001.tar: The most recent database entry has the same size as the actual file
  # Check continues
  
  $ zstash check --hpss=my_archive --error-on-duplicate-tar
  ERROR: Database corruption detected! Found 2 database entries for 000001.tar
  # Check exits immediately

Example using ``--hpss=none``::

  $ mkdir zstash_demo
  $ echo 'file0 stuff' > zstash_demo/file0.txt
  $ zstash create --hpss=none zstash_demo
  $ ls zstash_demo/
  file0.txt  zstash
  $ ls zstash_demo/zstash/
  000000.tar  index.db
  $ cd zstash_demo
  $ zstash check --hpss=none
  INFO: Opening tar archive zstash/000000.tar
  INFO: Checking file0.txt
  INFO: No failures detected when checking the files. If you have a log file, run "grep -i Exception <log-file>" to double check.

Example usage of ``--tars``::

  # Starting at 00005a until the end
  zstash check --tars=00005a-
  # Starting from the beginning to 00005a (included)
  zstash check --tars=-00005a
  # Specific range
  zstash check --tars=00005a-00005c
  # Selected tar files
  zstash check --tars=00003e,00004e,000059
  # Mix and match
  zstash check --tars=000030-00003e,00004e,00005a-

Update
======

An existing zstash archive can be updated to add new or modified files: ::

   $ cd <mydir>
   $ zstash update --hpss=<path to HPSS> [--cache=<cache>] [--dry-run] [--exclude] [--keep] [-v]

where

* ``--hpss=<path to HPSS>`` specifies the destination path on the HPSS file system,
* ``--cache`` to use a cache other than the default of ``zstash``.
* ``--dry-run`` an optional argument to specify a dry run, only lists files to be updated in archive.
* ``--exclude`` an optional argument of comma separated list of file patterns to exclude
* ``--follow-symlinks`` Hard copy symlinks. This is useful for preventing broken links. Note that a broken link will result in a failed update.
* ``--include`` an optional argument of comma separated list of file patterns to include
* ``--keep`` to keep a copy of the tar files on the local file system after
  they have been extracted from the archive. Normally, they are deleted after
  successful transfer.
* ``--non-blocking`` Zstash will submit a Globus transfer and immediately create a subsequent tarball. That is, Zstash will not wait until the transfer completes to start creating a subsequent tarball. On machines where it takes more time to create a tarball than transfer it, each Globus transfer will have one file. On machines where it takes less time to create a tarball than transfer it, the first transfer will have one file, but the number of tarballs in subsequent transfers will grow finding dynamically the most optimal number of tarballs per transfer. NOTE: zstash is currently always non-blocking.
* ``--error-on-duplicate-tar`` FOR ADVANCED USERS ONLY: Raise an error if a tar file with the same name already exists in the database. If this flag is set, zstash will exit if it sees a duplicate tar. If it is not set, zstash's behavior will depend on whether or not the --overwrite-duplicate-tar flag is set.
* ``--overwrite-duplicate-tars`` FOR ADVANCED USERS ONLY: If a duplicate tar is encountered, overwrite the existing database record with the new one (i.e., it will assume the latest tar is the correct one). If this flag is not set, zstash will permit multiple entries for the same tar in its database.
* ``-v`` increases output verbosity.

Note: in the event that an update includes revisions to files previously archived, ``zstash update``
will archive the new revisions. ``zstah extract`` will only extract the latest revision, but all
file versions will still be listed with the ``zstash ls`` and ``zstash ls -l`` commands.

Starting with ``zstash v1.1.0`` the md5 hash for the tars will be computed on ``zstash create``.
If you're using an existing database, then ``zstash update`` will begin keeping track
of the tars automatically.

What Happens If: Update Scenarios
----------------------------------

**Q: What happens if I run update with no changes?**

A: If no files have been added, modified, or deleted since the last archive operation: ::

  $ zstash update --hpss=my_archive
  INFO: Nothing to update

**Q: What happens to modified files?**

A: Modified files are archived again with their new content:

* The new version is added to a new tar file
* The old version remains in its original tar file
* ``zstash extract`` will extract only the latest version
* ``zstash ls`` will show all versions of the file

Example: ::

  $ echo "original content" > source_dir/file.txt
  $ zstash create --hpss=my_archive source_dir
  # file.txt is in 000000.tar
  
  $ echo "modified content" > source_dir/file.txt
  $ cd source_dir && zstash update --hpss=my_archive
  # New version of file.txt is in 000001.tar
  
  $ zstash ls --hpss=my_archive -l
  # Both versions are listed

**Q: Why are new tar files created instead of appending to existing ones?**

A: This is a design choice for data safety:

* New and modified files always go into new tar archives
* Existing tar files are never modified, reducing risk of corruption
* Even if an existing tar has space available, new files create a new tar

Example: ::

  $ zstash create --hpss=my_archive --maxsize 1.0 source_dir
  # Creates 000000.tar (10 MB, well under 1 GB limit)
  
  $ echo "new file" > source_dir/new.txt
  $ cd source_dir && zstash update --hpss=my_archive
  # Creates 000001.tar for new.txt (doesn't append to 000000.tar)

**Q: What happens with ``--dry-run``?**

A: The ``--dry-run`` flag shows what would be updated without making changes: ::

  $ cd source_dir && zstash update --hpss=my_archive --dry-run
  INFO: List of files to be updated
  INFO: new_file.txt
  INFO: modified_file.txt
  # No actual changes are made

Example
-------

Following the '**zstash create**' example above, we now run zstash again with the 
'**update**' functionality: ::

  $ cd $CSCRATCH/ACME_simulations/20170731.F20TR.ne30_ne30.edison
  $ zstash update --hpss=test/ACME_simulations/20170731.F20TR.ne30_ne30.edison

Since nothing has changed, zstash simply returns ::

  INFO: Nothing to update

Now, let's add a new file ::

  $ mkdir new
  $ echo "This is a new file..." > new/file.txt

and rerun zstash update ::

  $ zstash update --hpss=test/ACME_simulations/20170731.F20TR.ne30_ne30.edison

Zstash recognizes the presence of a new file and adds it to the archive: ::

  INFO: Gathering list of files to archive
  INFO: Creating new tar archive 000001.tar
  INFO: Archiving new/file.txt
  DEBUG: Closing tar archive 000001.tar
  INFO: Transferring file to HPSS: zstash/000001.tar
  INFO: Transferring file to HPSS: zstash/index.db

Note that the new file is added into a new archive tar file (000001.tar) even 
though the first archive tar file (000000.tar) is smaller than the target size 
and therefore could potentially hold more data. This is a design choice that 
was made out of caution to avoid the risk of damaging an existing tar file by 
appending to it.


Extract
=======

Note: Most of the commands for this are the same for ``zstash check`` and ``zstash ls``.

To extract files from an existing zstash archive into current <mydir>: ::

   $ cd <mydir>
   $ zstash extract --hpss=<path to HPSS> [--workers=<num of processes>] [--cache=<cache>] [--keep] [-v] [files]

where

* ``--hpss=<path to HPSS>`` specifies the destination path on the HPSS file system.
  Note that if ``--hpss=none``, then ``--keep`` is automatically set to ``True``.
  The option also accepts a Globus URL, ``globus://<Globus endpoint UUID/<path to archive>``.
  Then zstash will use `Globus <https://globus.org/>`_ to extract from a zstash archive on a Globus endpoint.
  Names ``alcf`` and ``nersc`` are recognized as referring to the ALCF HPSS and NERSC HPSS endpoints,
  e.g. ``globus://nersc/~/my_archive``.
* ``--workers=<num of processes>`` an optional argument which specifies the number of
  processes to use, resulting in extracting being done in parallel.
  **Using a high number will result in slow downloads for each of the tars since your bandwidth is limited.**
  **User discretion is advised.**
* ``--cache`` to use a cache other than the default of ``zstash``.
* ``--keep`` to keep a copy of the tar files on the local file system after
  they have been extracted from the archive. Normally, they are deleted after
  successful transfer.
* ``--retries`` to set the number of times to retry ``hsi get`` if it is unsuccessful.
  The default is 1 retry (2 tries total). Note: for a retry to occur automatically because of
  an incomplete tar file, then the archive you're extracting from
  must have been created using ``zstash >= v1.1.0``.
* ``--tars`` to	specify	specific tars to extract. See "Check" above for example usage.
* ``--error-on-duplicate-tar`` FOR ADVANCED USERS ONLY: Raise an error if a tar file with the same name already exists in the database. If this flag is set, zstash will exit if it sees a duplicate tar. If it is not set, zstash will check if the size matches the *most recent* entry.
* ``-v`` increases output verbosity.
* ``[files]`` is a list of files to be extracted (standard wildcards supported).

  * Leave empty to extract all the files.
  * List of files with support for wildcards. Please note that any expression
    containing **wildcards should be enclosed in double quotes ("...")** 
    to avoid shell substitution.
  * Names of specific tar archives to extract all files within these tar archives.

You must pass in the **path relative to the top level** for the file(s). For help 
finding path names, you can use ``zstash ls`` as documented below.

A few words about performance. All of the files are grouped into 256GB tar archives by default.
(See the ``--maxsize`` argument for ``zstash create`` for more information).
If the tar file is not already present in the local disk cache (under 
the ``zstash/`` sub-directory), it must first be downloaded from HPSS before
the desired file can be extracted.

  * Downloading a 256GB file on Cori/Edison takes about 30 mins (or more depending on load).
  * Using NERSC data transfer nodes (DTN) may be about 3x faster, according to some users.
  * Again, to see which of your files are in what tar archives, use ``zstash ls -l``.

    * Note the ``-l`` argument.
    * The sixth column is the tar archive that the file is in.
    * Please see the List documentation below for more information.

What Happens If: Extract Scenarios
-----------------------------------

**Q: What happens if I extract files that already exist locally?**

A: Files that already exist and match the archived version are skipped: ::

  $ zstash extract --hpss=my_archive
  INFO: Extracting file1.txt
  INFO: Extracting file2.txt
  
  $ zstash extract --hpss=my_archive
  INFO: Not extracting file1.txt
  INFO: Not extracting file2.txt
  # Files are skipped because they already exist

**Q: What happens with ``--keep`` during extract?**

A: The ``--keep`` flag determines whether tar files remain in the cache after extraction:

* **Without ``--keep``**: Tar files are downloaded, extracted, then deleted
* **With ``--keep``**: Tar files remain in the local cache for potential reuse
* **With ``--hpss=none``**: Requires existing local cache with tar files

Example: ::

  $ zstash extract --hpss=my_archive --keep
  # After completion: zstash/ directory contains index.db and all extracted tar files
  
  $ zstash extract --hpss=my_archive
  # After completion: zstash/ directory contains only index.db

**Q: How do I extract from a local cache without HPSS?**

A: Use ``--hpss=none`` and specify the cache location: ::

  $ zstash extract --hpss=none --cache=/path/to/cache
  # Extracts from local cache without accessing HPSS

**Q: What happens with parallel extraction (``--workers``)?**

A: Using ``--workers=N`` processes multiple tar files concurrently:

* Faster overall extraction time
* Each worker processes complete tar files
* Tars are processed in sorted order even with parallel execution
* Files within each tar are still extracted sequentially

Example: ::

  $ zstash extract --hpss=my_archive --workers=3
  # Three tar files are extracted concurrently

**Q: What does ``--tars`` do during extraction?**

A: The ``--tars`` option limits extraction to files within specific tar archives:

* Only files from the specified tars are extracted
* Cannot be combined with the ``[files]`` argument
* Useful for partial restoration of large archives

Example: ::

  $ zstash extract --hpss=my_archive --tars="000001,000003"
  # Only extracts files from 000001.tar and 000003.tar

Examples
--------

Extracting a single file by its full path ``archive/logs/atm.log.8229335.180130-143234.gz`` ::

      $ zstash extract --hpss=/home/g/golaz/2018/E3SM_simulations/20180129.DECKv1b_piControl.ne30_oEC.edison archive/logs/atm.log.8229335.180130-143234.gz
      DEBUG: Opening index database
      DEBUG: Running zstash extract
      DEBUG: Local path : /global/cscratch1/sd/golaz/ACME_simulations/20180129.DECKv1b_piControl.ne30_oEC.edison
      DEBUG: HPSS path  : /home/g/golaz/2018/E3SM_simulations/20180129.DECKv1b_piControl.ne30_oEC.edison
      DEBUG: Max size  : 274877906944
      DEBUG: Keep local tar files  : False
      INFO: Opening tar archive zstash/000018.tar
      INFO: Extracting archive/logs/atm.log.8229335.180130-143234.gz
      DEBUG: Valid md5: e8161bba53500848dc917258d1d8f56a archive/logs/atm.log.8229335.180130-143234.gz
      DEBUG: Closing tar archive zstash/000018.tar
      DEBUG: Closing index database

If the index database is already in the local disk cache (zstash/index.db), you can leave out the ``--hpss``
path. For example: ::

      $ zstash extract archive/logs/atm.log.8229335.180130-143234.gz

However, recall that wildcards are supported, so this full path isn't needed when using them.
Instead, you could download files matching ``"*atm.log.8229335.180130-143234.gz*"``. Note
the use of double quotes (") to avoid shell level substitution. ::
  
      $ zstash extract --hpss=/home/g/golaz/2018/E3SM_simulations/20180129.DECKv1b_piControl.ne30_oEC.edison "*atm.log.8229335.180130-143234.gz*"
      DEBUG: Opening index database
      DEBUG: Running zstash extract
      DEBUG: Local path : /global/cscratch1/sd/golaz/ACME_simulations/20180129.DECKv1b_piControl.ne30_oEC.edison
      DEBUG: HPSS path  : /home/g/golaz/2018/E3SM_simulations/20180129.DECKv1b_piControl.ne30_oEC.edison
      DEBUG: Max size  : 274877906944
      DEBUG: Keep local tar files  : False
      INFO: Opening tar archive zstash/000018.tar
      INFO: Extracting archive/logs/atm.log.8229335.180130-143234.gz
      DEBUG: Valid md5: e8161bba53500848dc917258d1d8f56a archive/logs/atm.log.8229335.180130-143234.gz
      DEBUG: Closing tar archive zstash/000018.tar
      INFO: Opening tar archive zstash/000047.tar
      INFO: Extracting case_scripts/logs/atm.log.8229335.180130-143234.gz
      DEBUG: Valid md5: e8161bba53500848dc917258d1d8f56a case_scripts/logs/atm.log.8229335.180130-143234.gz
      DEBUG: Closing tar archive zstash/000047.tar
      DEBUG: Closing index database

In this particular example, the pattern matches two specific files, one under `archive/logs/`
and another one under `case_scripts/logs/`. If you didn't intend to retrieve both of them, a
more efficient approach would have been to first identify the desired files with 'zstash ls'.

Another example of wildcards would be to retrieve all **cam.h0** (monthly atmosphere output files) 
between **years 0030 and 0069** for the DECKv1 piControl simulation. The zstash command would be: ::

   $ zstash extract --hpss=/home/g/golaz/2018/E3SM_simulations/20180129.DECKv1b_piControl.ne30_oEC.edison \
            "*.cam.h0.00[3-6]?-??.nc"


You may specify the cache with the ``--cache`` option. Notice that there is no need to include
``--keep`` when not using HPSS. ::

  $ zstash extract --hpss=none \
  --cache=/p/user_pub/e3sm/archive/1_1/BGC-v1/20181217.BCRC_CNPCTC20TR_OIBGC.ne30_oECv3.edison \
  "*cam.h3.1906-01-*-*.nc"

Example with Globus
-------------------

To extract from the archive created with Globus in the ``zstash create`` example, you would run: ::

  $ zstash extract --hpss=globus://9cd89cfd-6d04-11e5-ba46-22000b92c6ec/~/test/E3SM_simulations/20170731.F20TR.ne30_ne30.anvil

.. _zstash-list:

List
====

Note: Most of the commands for this are the same for ``zstash extract`` and ``zstash check``.

You can view the files in an existing zstash archive:  ::

   $ zstash ls --hpss=<path to HPSS> [-l] [--cache=<cache>] [--tars] [-v] [files]

where

* ``--hpss=<path to HPSS>`` specifies the destination path on the HPSS file system,
* ``-l`` an optional argument to display more information.
* ``--cache`` to use a cache other than the default of ``zstash``.
* ``--tars`` to list the tars in addition to the files.
* ``-v`` increases output verbosity.
* ``[files]`` is a list of files to be listed (standard wildcards supported).

  * Leave empty to list all the files.
  * List of files with support for wildcards. Please note that any expression
    containing **wildcards should be enclosed in double quotes ("...")** 
    to avoid shell substitution.
  * Names of specific tar archives to list all files within these tar archives.

Below is an example. Note the names of the columns:  ::

   $ zstash ls -l --hpss=/home/g/golaz/2018/E3SM_simulations/20180129.DECKv1b_piControl.ne30_oEC.edison "*atm.log.8229335.180130-143234.gz*"
   DEBUG: Opening index database
   DEBUG: Running zstash ls
   DEBUG: HPSS path  : /home/g/golaz/2018/E3SM_simulations/20180129.DECKv1b_piControl.ne30_oEC.edison
   id	name	size	mtime	md5	tar	offset
   30482	archive/logs/atm.log.8229335.180130-143234.gz	20156521	2018-02-01 10:02:35	e8161bba53500848dc917258d1d8f56a	000018.tar	131697281536	
   51608	case_scripts/logs/atm.log.8229335.180130-143234.gz	20156521	2018-02-01 10:02:52	e8161bba53500848dc917258d1d8f56a	000047.tar	202381473280	

Below is an example of using ``ls`` to look at the tars in addition to the files: ::

    $ mkdir source_directory
    $ touch source_directory/file0.txt
    $ zstash create --hpss=hpss_archive source_directory
    INFO: Gathering list of files to archive
    INFO: Creating new tar archive 000000.tar
    INFO: Archiving file0.txt
    INFO: tar name=000000.tar, tar size=10240, tar md5=97d3e0ffaff4880251c77699d7438fe2
    INFO: Transferring file to HPSS: zstash/000000.tar
    INFO: Transferring file to HPSS: zstash/index.db

    $ zstash ls --hpss=hpss_archive --tars
    INFO: Transferring file from HPSS: zstash/index.db
    file0.txt

    Tars:
    000000.tar

What Happens If: List Scenarios
--------------------------------

**Q: What happens if I run ls from outside the source directory?**

A: ``zstash ls`` uses the local cache if available, otherwise downloads ``index.db`` from HPSS:

* **If local cache exists**: Uses the local ``index.db`` (may be outdated)
* **If no local cache**: Downloads ``index.db`` from HPSS to a new ``zstash/`` directory
* **Warning**: The local cache may not reflect recent updates if you've switched directories

Example: ::

  $ zstash create --hpss=my_archive source_dir
  # Creates source_dir/zstash/index.db
  
  $ cd /different/directory
  $ zstash ls --hpss=my_archive
  # Creates /different/directory/zstash/ and downloads index.db there

**Q: What happens with ``--tars`` option?**

A: The ``--tars`` flag shows which tar files are in the archive:

* Lists all tar files in the archive
* Can be combined with file patterns to show which tars contain matching files
* Useful for understanding archive structure

Example: ::

  $ zstash ls --hpss=my_archive --tars
  file0.txt
  file1.txt
  
  Tars:
  000000.tar
  
  $ zstash ls --hpss=my_archive --tars "*.txt"
  file0.txt
  file1.txt
  
  Tars:
  000000.tar

**Q: What happens if I update the archive and then run ls?**

A: The behavior depends on your location and cache:

* **From source directory after update**: ``ls`` shows the updated files immediately
* **From different directory with old cache**: ``ls`` shows outdated information
* **Solution**: Delete the old cache or run from the source directory

Example: ::

  $ zstash create --hpss=my_archive source_dir
  $ cd /different/directory
  $ zstash ls --hpss=my_archive  # Downloads index.db to ./zstash/
  
  # Add files and update
  $ cd source_dir
  $ echo "new content" > new_file.txt
  $ zstash update --hpss=my_archive  # Updates source_dir/zstash/index.db
  
  $ cd /different/directory
  $ zstash ls --hpss=my_archive  # Still uses old cache!
  # new_file.txt is NOT shown
  
  $ rm -rf zstash  # Delete old cache
  $ zstash ls --hpss=my_archive  # Downloads fresh index.db
  # new_file.txt is now shown

.. warning::
    Running ``zstash ls`` outside the source directory (the directory you're archiving)
    is not advised. ``zstash`` will only retrieve ``index.db`` from the HPSS archive
    if a local archive (cache) is not present.

Example 1 -- changing the HPSS archive: ::

    $ zstash create --hpss=hpss_archive source_directory           # Creates an HPSS archive named `hpss_archive` and a local archive (cache) `source_directory/zstash`.
    $ zstash ls --hpss=hpss_archive                                # List the contents of `hpss_archive` and creates a cache `zstash` at the same level of `source_directory`.
    # Add `source_directory/new_file.txt`
    $ zstash create --hpss=different_hpss_archive source_directory # Create a different HPSS archive of the source directory. This overwrites the local archive (cache) `source_directory/zstash`.
    $ zstash ls --hpss=different_hpss_archive                      # `new_file.txt` will NOT be shown. The existing cache `zstash` (same level as `source_directory`) is being used.
    $ rm -rf zstash                                                # Delete the cache. (You could instead change to another directory).
    $ zstash ls --hpss=different_hpss_archive                      # `new_file.txt` will be shown.

Example 2 -- updating the HPSS archive: ::

    $ zstash create --hpss=hpss_archive source_directory # Creates an HPSS archive named `hpss_archive` and a local archive (cache) `source_directory/zstash`.
    $ zstash ls --hpss=hpss_archive                      # List the contents of `hpss_archive` and creates a cache `zstash` at the same level of `source_directory`.
    # Add `source_directory/new_file.txt`
    $ cd source_directory
    $ zstash update --hpss=hpss_archive                  # Add `new_file.txt` to the HPSS archive. This updates the cache `zstash` (in `source_directory`).
    $ cd ..
    $ zstash ls --hpss=hpss_archive                      # `new_file.txt` will NOT be shown. The existing cache `zstash` (same level as `source_directory`) is being used.
    $ rm -rf zstash                                      # Delete the cache. (You could instead change to another directory).
    $ zstash ls --hpss=hpss_archive                      # `new_file.txt` will be shown.

Example 3 -- changing the HPSS archive, running ``zstash_ls`` from the source directory: ::

    $ zstash create --hpss=hpss_archive source_directory           # Creates an HPSS archive named `hpss_archive` and a local archive (cache) `source_directory/zstash`.
    $ cd source_directory                                          # This is the directory we are archiving.
    $ zstash ls --hpss=hpss_archive                                # List the contents of `hpss_archive` and uses the existing cache `zstash` (in `source_directory`).
    # Add `new_file.txt`
    $ cd ..
    $ zstash create --hpss=different_hpss_archive source_directory # Create a different HPSS archive of the source directory. This overwrites the local archive (cache) `source_directory/zstash`.
    $ cd source_directory
    $ zstash ls --hpss=different_archive                           # `new_file.txt` will be shown.

Example 4 -- updating the HPSS archive, running ``zstash_ls`` from the source directory: ::

    $ zstash create --hpss=hpss_archive source_directory # Creates an HPSS archive named `hpss_archive` and a local archive (cache) `source_directory/zstash`.
    $ cd source_directory                                # This is the directory we are archiving.
    $ zstash ls --hpss=hpss_archive                      # List the contents of `hpss_archive` and uses the existing cache `zstash` (in `source_directory`).
    # Add new_file.txt
    $ zstash update --hpss=hpss_archive                  # Add `new_file.txt` to the HPSS archive. This updates the cache `zstash` (in `source_directory`).
    $ zstash ls --hpss=hpss_archive                      # `new_file.txt` will be shown.

Version
=======

Starting with version 0.3, you can check the version of zstash from the command line: ::

   $ zstash version
   v0.3.0

Change Group
============

To change the HPSS group ownership of the tar archives and index.db: ::

   $ zstash chgrp -R <group> --hpss=<path to HPSS>

where

* ``-R`` recursively changes the group ownership
* ``<group>`` specifies the new group name
* ``--hpss=<path to HPSS>`` specifies the destination path on the HPSS file system

Note: This command is only available for HPSS archives. It uses the ``hsi chgrp`` command internally.

What Happens If: Change Group Scenarios
----------------------------------------

**Q: What happens if HPSS is unavailable?**

A: The command will report that HPSS is unavailable: ::

  $ zstash chgrp -R my_group --hpss=none
  chgrp: HPSS is unavailable

**Q: What happens with verbose output?**

A: Use the ``-v`` flag to see detailed progress: ::

  $ zstash chgrp -R my_group -v --hpss=my_archive
  Running zstash chgrp
  # Shows detailed output of group changes
