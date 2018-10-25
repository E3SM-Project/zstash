****************
Quickstart guide
****************

.. highlight:: none

Installation
============

First, make sure that you're using ``bash``. ::

   $ bash

You must have Anaconda installed as well. On NERSC machines,
you can load the Anaconda module instead of installing it yourself. ::

   $ module load python/2.7-anaconda-4.4

Create a new Anaconda environment with zstash installed and activate it. ::

   $ conda create -n zstash_env -c e3sm -c conda-forge zstash
   $ source activate zstash_env

Or you can install zstash in an existing environment. ::

   $ conda install zstash -c e3sm -c conda-forge 


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

Extract
=======

To extract files from an existing zstash archive into current <mydir>: ::

   $ cd <mydir>
   $ zstash extract --hpss=<path to HPSS> [files]

where

* ``--hpss=<path to HPSS>`` specifies the destination path on the HPSS file system,
* and ``[files]`` is a list of files to extract (standard wildcards supported). Leave empty 
  to extract all the files.

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

