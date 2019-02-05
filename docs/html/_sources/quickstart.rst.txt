****************
Quickstart guide
****************

.. highlight:: none

Installation
============

First, make sure that you're using ``bash``. ::

   $ bash

You must have Anaconda installed as well. On NERSC Edison and Cori machines,
you can load the Anaconda module instead of installing it yourself. ::

   $ module load python/2.7-anaconda-4.4

Create a new Anaconda environment with zstash installed and activate it. ::

   $ conda create -n zstash_env -c e3sm -c conda-forge zstash
   $ source activate zstash_env

Or you can install zstash in an existing environment. ::

   $ conda install zstash -c e3sm -c conda-forge 

On NERSC, after installing on Edison or Cori, you may see improved performance 
running zstash on the data transfer nodes (dtn{01..15}.nersc.gov). However, modules are
not directly available there, so you will need to manually activate Anaconda: ::

   $ . /global/common/edison/software/python/2.7-anaconda-4.4/etc/profile.d/conda.sh
   $ export PATH="/global/common/edison/software/python/2.7-anaconda-4.4/bin:$PATH"

Installation from source
========================

If you want to get the latest code of zstash from the master branch, do the following.

First, follow the instructions in the previous section ("Installation") to create an
Anaconda environment with zstash.
Make sure you're in the zstash environment before executing the below instructions.

Then, use the command below to remove just zstash, keeping all of the dependencies
in the environment.
We'll be manually installing the latest zstash from master soon. ::

   $ conda remove zstash --force

Clone the zstash repository. ::

   $ git clone https://github.com/E3SM-Project/zstash.git

Install the latest zstash. ::

   $ cd zstash/
   $ python setup.py install


Archive
=======

To create a new zstash archive: ::

   $ zstash create --hpss=<path to HPSS> <local path>

where

* ``--hpss=<path to HPSS>`` specifies the destination path on the HPSS file system,
* and ``<local path>`` specifies the path to the local directory that should be archived.

Additional optional arguments:

* ``--exclude`` comma separated list of file patterns to exclude
* ``--keep`` to keep a copy of the tar files on the local file system after 
  they have been transferred to HPSS. Normally, they are deleted after 
  successful transfer.
* ``--maxsize MAXSIZE`` specifies the maximum size (in GB) for tar files. 
  The default is 256 GB. Zstash will create tar files that are smaller 
  than MAXSIZE except when individual input files exceed MAXSIZE (as 
  individual files are never split up between different tar files).

Local tar files as well as the sqlite3 index database (index.db) will be stored
under ``<local path>/zstash``.

**After you run** ``zstash create`` **it's highly recommended that you
run** ``zstash check``, **detailed in the section below.**

Check
=====

Note: Most of the commands for this are the same for ``zstash extract`` and ``zstash ls``.

To verify that your files were uploaded on HPSS successfully,
go to a **new, empty directory** and run: ::

   $ zstash check --hpss=<path to HPSS> [files]

where

* ``--hpss=<path to HPSS>`` specifies the destination path on the HPSS file system.
* ``[files]`` is a list of files to check (standard wildcards supported).

  * Leave empty to check all the files.
  * You can even pass in the name of a specific tar archive to check
    all files from that tar archive.

The tar archives are downloaded and each file in it is checked.

After the checking is done, a list of all corrupted files in the HPSS archive,
along with what tar archive they belong in is listed. Below is an example:  ::

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

Extract
=======

Note: Most of the commands for this are the same for ``zstash check`` and ``zstash ls``.

To extract files from an existing zstash archive into current <mydir>: ::

   $ cd <mydir>
   $ zstash extract --hpss=<path to HPSS> [files]

where

* ``[files]`` is a list of files to extract (standard wildcards supported).

  * Leave empty to extract all the files.
  * You can even pass in the name of a specific tar archive to extract
    all files from that tar archive.

You must pass in the **full path** for the file(s).

  * For help finding the full paths, you can use ``zstash ls``.
    Please see below for the documentation on this.
  * Ex: Downloading ``archive/logs/atm.log.8229335.180130-143234.gz`` ::

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

However, recall that wildcards are supported, so this full path isn't needed when using them.

  * As in the example below, when compared to the example above, you might also
    inadvertently download more files than you wanted.
  * Ex: Downloading ``*atm.log.8229335.180130-143234.gz*`` ::
  
      $ zstash extract --hpss=/home/g/golaz/2018/E3SM_simulations/20180129.DECKv1b_piControl.ne30_oEC.edison *atm.log.8229335.180130-143234.gz*
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

All of the files are grouped into 256GB tar archives by default.
Look at the ``--maxsize`` argument for ``zstash create`` for more information.
So even if your file is a few MB in size, the entire tar archive your file is in needs to be downloaded.

  * Downloading a 256GB file on Cori/Edison takes about 30 mins.
  * Using the data transfer nodes (DTN) will be about 3x faster, according to some users.
  * Again, to see which of your files are in what tar archives, use ``zstash ls -l``.

    * Note the ``-l`` argument.
    * The sixth column is the tar archive that the file is in.
    * Please see the List documentation below for more information.

Update
======

An existing archive can be updated to add new or modified files into an existing zstash 
archive: ::

   $ cd <mydir>
   $ zstash update --hpss=<path to HPSS> [files]

where

* ``--hpss=<path to HPSS>`` specifies the destination path on the HPSS file system,
* ``--exclude`` comma separated list of file patterns to exclude,
* ``--dry-run`` dry run, only list files to be updated in archive.

List content
============

Note: Most of the commands for this are the same for ``zstash extract`` and ``zstash check``.

You can view the files in an existing zstash archive:  ::

   $ zstash ls --hpss=<path to HPSS> [files]

where

* ``--hpss=<path to HPSS>`` specifies the destination path on the HPSS file system,
* ``-l`` an optional argument to display more information.

Below is an example. Note the names of the columns:  ::

   $ zstash ls -l --hpss=/home/g/golaz/2018/E3SM_simulations/20180129.DECKv1b_piControl.ne30_oEC.edison *atm.log.8229335.180130-143234.gz*
   DEBUG: Opening index database
   DEBUG: Running zstash ls
   DEBUG: HPSS path  : /home/g/golaz/2018/E3SM_simulations/20180129.DECKv1b_piControl.ne30_oEC.edison
   id	name	size	mtime	md5	tar	offset
   30482	archive/logs/atm.log.8229335.180130-143234.gz	20156521	2018-02-01 10:02:35	e8161bba53500848dc917258d1d8f56a	000018.tar	131697281536	
   51608	case_scripts/logs/atm.log.8229335.180130-143234.gz	20156521	2018-02-01 10:02:52	e8161bba53500848dc917258d1d8f56a	000047.tar	202381473280	
