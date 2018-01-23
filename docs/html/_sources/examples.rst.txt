********
Examples
********

Basic
=====

Simple illustration how to **archive** output from an ACME simulation located
under `$CSCRATCH/ACME_simulations/20170731.F20TR.ne30_ne30.edison`::

  $ cd $CSCRATCH/ACME_simulations/20170731.F20TR.ne30_ne30.edison
  $ zstash create --hpss=test/ACME_simulations/20170731.F20TR.ne30_ne30.edison .

Once done, you should see the archive files on hsi: ::

  $ hsi
  > cd test/ACME_simulations/20170731.F20TR.ne30_ne30.edison
  > ls 
  000000.tar   index.db

The data from this test simulation is small, so in this case there is only a single tar 
file (000000.tar) and the index database (index.db).

We now run again with the '**update**' functionality: ::

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

To **extract all** the files ::

  $ cd $CSCRATCH/ACME_simulations/test
  $ zstash extract --hpss=test/ACME_simulations/20170731.F20TR.ne30_ne30.edison

or to **extract selected files** ::

  $ cd $CSCRATCH/ACME_simulations/test
  $ zstash extract --hpss=test/ACME_simulations/20170731.F20TR.ne30_ne30.edison \
    *.cam.h0*.nc *.clm2.h0*.nc

Excluding some files
====================

You may decide that there are certain type of files that do not need archiving.
For example, if you want to skip archiving \*.o and \*.mod files under the build
subdirectory: ::

  $ cd $CSCRATCH/ACME_simulations/20170731.F20TR.ne30_ne30.edison
  $ zstash create --hpss=test/ACME_simulations/20170731.F20TR.ne30_ne30.edison \
    --exclude="build/*/*.o","build/*/*.mod" .

Running update with the same exclude patterns will also ignore these files
rather than r=treating them as new: ::

  $ zstash update --hpss=test/ACME_simulations/20170731.F20TR.ne30_ne30.edison \
    --exclude="build/*/*.o","build/*/*.mod"

  [...]

  INFO: Nothing to update

