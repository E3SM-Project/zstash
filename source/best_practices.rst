***********************
Best practices for E3SM
***********************

This page summarizes the recommended best practices for archiving
E3SM simulation output using zstash.

NERSC
=====

For best performance on NERSC, you should login to one 
of the data transfer nodes (dtn<01..15>.nersc.gov). Also, because
archiving large amount of data with zstash can take several days,
it is recommended to invoke zstash within a UNIX `screen` session
to which you can detach and re-attach without killing zstash. ::

   $ ssh dtn01.nersc.gov
   $ screen

To detach from the screen session, use CTRL-A followed by D (for detach).
You can then safely close your window. To re-attach to an existing session
later: ::

   $ ssh dtn01.nersc.gov
   $ screen -r

Archiving
=========

Typically, you should consider archiving the entire directory structure
of a simulation. The first time, this is accomplished with ``zstash create``.
For example: ::

   $ ssh dtn01.nersc.gov
   $ screen -r
   $ cd /global/cscratch1/sd/golaz/E3SM/simulations/20180129.DECKv1b_piControl.ne30_oEC.edison
   $ zstash create --hpss=2018/E3SM_simulations/20180129.DECKv1b_piControl.ne30_oEC.edison \
     --maxsize 128 . 2>&1 | tee zstash_create_20190226.log

The command above will archive the entire directory structure under
`/global/cscratch1/sd/golaz/E3SM/simulations/20180129.DECKv1b_piControl.ne30_oEC.edison`.
The archive files will be stored on HPSS under `2018/E3SM_simulations/20180129.DECKv1b_piControl.ne30_oEC.edison`.
Individual tar files in the zstash archive will have a maximum size of 128 GB. NERSC typically
recommends file size between 100 and 500 GB for best performance.

If your model output has been reorganized using the CIME short-term archive utility, you can easily
archive only a subset of the restart files to conserve space. For example, to **archive
restart files every 5 years** only: ::

   $ ssh dtn01.nersc.gov
   $ screen -r
   $ cd /global/cscratch1/sd/golaz/E3SM/simulations/20180129.DECKv1b_piControl.ne30_oEC.edison
   $ zstash create --hpss=2018/E3SM_simulations/20180129.DECKv1b_piControl.ne30_oEC.edison \
     --exclude="archive/rest/???[!05]-*/" \
     --maxsize 128 . 2>&1 | tee zstash_create_20190226.log

Updating
========

You can also add newly created files to an existing archive, or restart archiving after a 
failure using the ``zstash update`` functionality: ::

   $ ssh dtn01.nersc.gov
   $ screen -r
   $ cd /global/cscratch1/sd/golaz/E3SM/simulations/20180129.DECKv1b_piControl.ne30_oEC.edison
   $ zstash update --hpss=2018/E3SM_simulations/20180129.DECKv1b_piControl.ne30_oEC.edison \
     --exclude="archive/rest/???[!05]-*/" \
     --maxsize 128 2>&1 | tee zstash_update_20190226.log

Checking
========

After archiving or updating, it is **highly recommended** that you verify the integrity
of the tar files. The safest way to do so is go to a new, empty directory and run: ::

  $ zstash check --hpss=2018/E3SM_simulations/20180129.DECKv1b_piControl.ne30_oEC.edison

``zstash check`` will download the tar archives to the local disk cache (under 
the zstash/ subdirectory) and verify the md5 checksum of every file against the 
checksum stored in the index database (index.db).

``zstash check`` can also be run in the original location. But if tar files
are present in the local cache (zstash/ sub-directory), the check will only
be performed agains the local disk copy, not the one on HPSS.

If any corrupted file is found during the check, zstash will print a list of corrupted 
files in the archive, along with the tar archive they belong to.

