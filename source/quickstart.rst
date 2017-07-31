****************
Quickstart guide
****************

.. highlight:: none

Installation
============

First, make sure that Python 2.7 is loaded in your environment. On NERSC 
machines: ::

   $ module load python/2.7-anaconda-4.4

Clone the github repository in your preferred location ::

   $ cd <myInstallDir>
   $ git clone git@github.com:ACME-Climate/zstash.git zstash

Optionally, create a symbolic link to the executable in your
`~/bin` directory: ::

   $ cd ~/bin
   $ ln -s <myInstallDir>/zstash/zstash.py zstash

If `~/bin` is not already in you PATH, you can add it using ::

   $ export PATH=$HOME/bin:$PATH


Archive
=======

To create a new zstash archive: ::

   $ zstash create --hpss=<path to HPSS> <local path>

where

* ``--hpss=<path to HPSS>`` specifies the destination path on the HPSS file system,
* and ``<local path>`` specifies the path to the local directory that should be archived.

Additional optional arguments:

* ``--keep`` to keep a copy of the tar files on the local file system after 
  they have been transferred to HPSS. Normally, they are deleted after 
  successful transfer.
* ``--maxsize MAXSIZE`` specifies the maximum size (in GB) for tar files. Zstash
  will create tar files that are smaller than MAXSIZE except when individual
  input files exceed MAXSIZE (as individual files are never split up between 
  different tar files).

Local tar files as well as the sqlite3 index database (index.db) will be stored
under ``<local path>/zstash``.

Extract
=======

To extract files from an existing zstash archive into current <mydir>: ::

   $ cd <mydir>
   $ zstash extract --hpss=<path to HPSS> [files]

where

* ``--hpss=<path to HPSS>`` specifies the destination path on the HPSS file system,
* and ``[files]`` is a list of files to extract (standard wildcards supported). Leave empty 
  to extract all the files.

List content
============

Zstash does not yet include a built-in functionality to list content of archives.
However, the content can be obtained by directly querying the index database.

To list **all the files** in an archive: ::

   $ cd <mydir>
   $ sqlite3 zstash/index.db "select * from files;"

For each file, the following information will be printed ::

   file # | path | size | modification time |md5 checksum |tar archive | offset (within tar)

To list **files matching a specified pattern** (for example \*/run/\*.nc): ::

   $ sqlite3 zstash/index.db "select * from files where name glob '*/run/*.nc';"

To list **all the files in a specific tar fole** (for example 000000.tar): ::

   $ sqlite3 zstash/index.db "select * from files where tar is '000000.tar';"

Example
=======

Simple illustration how to **archive** output from an ACME simulation located
under `$CSCRATCH/ACME_simulations/20170731.F20TR.ne30_ne30.edison`::

  $ cd $CSCRATCH/ACME_simulations/20170731.F20TR.ne30_ne30.edison
  $ zstash create --hpss=test/ACME_simulations/20170731.F20TR.ne30_ne30.edison .

Once dome, you should see the archive files on hsi: ::

  $ hsi
  > cd test/ACME_simulations/20170731.F20TR.ne30_ne30.edison
  > ls 
  000000.tar   index.db

The data from this test simulation is small, so in this case there is only a single tar 
file (000000.tar) and the index database (index.db).

To **extract all** the files ::

  $ cd $CSCRATCH/ACME_simulations/test
  $ zstash extract --hpss=test/ACME_simulations/20170731.F20TR.ne30_ne30.edison

or to **extract selected files** ::

  $ cd $CSCRATCH/ACME_simulations/test
  $ zstash extract --hpss=test/ACME_simulations/20170731.F20TR.ne30_ne30.edison \
    *.cam.h0*.nc *.clm2.h0*.nc
