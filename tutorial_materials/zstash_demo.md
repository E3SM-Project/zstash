# zstash demo

# Load the E3SM Unified environment

This environment has all the important post-processing packages,
including zppy and the packages it calls.
```
# This will load E3SM Unified 1.10.0
$ source /global/common/software/e3sm/anaconda_envs/load_latest_e3sm_unified_pm-cpu.sh
```

# Running zstash
Note: to access HPSS, you will need to log into `hsi`, by running `hsi` on the command line, first.

We're going to create an archive and then
verify it, list the contents, and extract the contents.
We'll do each of these in a different subdirectory to simulate being different users.

## zstash create

```
$ mkdir /pscratch/sd/f/forsyth/e3sm_tutorial
$ cd /pscratch/sd/f/forsyth/e3sm_tutorial
$ mkdir workdir
```

We can create an archive on NERSC HPSS by running:
```
$ zstash create --hpss=tutorial_archive_20240507 --cache=/pscratch/sd/f/forsyth/e3sm_tutorial/workdir/zstash_v3.LR.historical_0101 --include=archive/atm/hist/extendedOutput.v3.LR.historical_0101.eam.h0.2000-1*.nc /global/cfs/cdirs/e3sm/www/Tutorials/2024/simulations/extendedOutput.v3.LR.historical_0101/
# hpss -- what HPSS archive should we use? Note: you can also use Globus, or set to "None" to archive locally
# cache -- what cache should we use? This is important to set here because we don't have write permissions to write the cache in the directory we're archiving
# include -- what files should we include in the archive? Often you'd want to include the entire directory. For the purposes of the tutorial however, it speeds things up to only archive a little bit.

# It may also be helpful to pipe output to a log
# (e.g., with `2>&1 | tee <path-to-log>`)
# Warning -- It is important to not pipe the `zstash` command line output to the directory being archived. 
# That is, the log should *not* be in the directory being archived.
# Trying to archive a file currently being updated will created a corrupted file,
# causing `zstash check` and `zstash extract` to error.
# For more information, see the discussion at https://github.com/E3SM-Project/zstash/issues/332#issuecomment-2050190511
```

<details>
<summary>Output</summary>

```
For help, please see https://e3sm-project.github.io/zstash. Ask questions at https://github.com/E3SM-Project/zstash/discussions/categories/q-a.
INFO: Gathering list of files to archive
INFO: Creating new tar archive 000000.tar
INFO: Archiving archive/atm/hist/extendedOutput.v3.LR.historical_0101.eam.h0.2000-10.nc
INFO: Archiving archive/atm/hist/extendedOutput.v3.LR.historical_0101.eam.h0.2000-11.nc
INFO: Archiving archive/atm/hist/extendedOutput.v3.LR.historical_0101.eam.h0.2000-12.nc
INFO: tar name=000000.tar, tar size=7728998400, tar md5=16d1afe9cba36a663a784482486a1b9d
INFO: Transferring file to HPSS: /pscratch/sd/f/forsyth/e3sm_tutorial/workdir/zstash_v3.LR.historical_0101/000000.tar
INFO: Transferring file to HPSS: /pscratch/sd/f/forsyth/e3sm_tutorial/workdir/zstash_v3.LR.historical_0101/index.db
```

The directory structure now looks like this:
```
$ pwd
/pscratch/sd/f/forsyth/e3sm_tutorial
$ ls
workdir
$ ls workdir/
zstash_v3.LR.historical_0101
$ ls workdir/zstash_v3.LR.historical_0101/ # This is our cache!
index.db

$ hsi
$ ls tutorial_archive_20240507

tutorial_archive_20240507:
000000.tar   index.db
$ exit
```

</details>

## zstash check

```
$ mkdir check_output
$ cd check_output
```

We can verify zstash was successful by running:
```
$ zstash check --hpss=tutorial_archive_20240507
```

<details>
<summary>Output</summary>

```
For help, please see https://e3sm-project.github.io/zstash. Ask questions at https://github.com/E3SM-Project/zstash/discussions/categories/q-a.
INFO: Transferring file from HPSS: zstash/index.db
INFO: Transferring file from HPSS: zstash/000000.tar
INFO: zstash/000000.tar exists. Checking expected size matches actual size.
INFO: Opening tar archive zstash/000000.tar
INFO: Checking archive/atm/hist/extendedOutput.v3.LR.historical_0101.eam.h0.2000-10.nc
INFO: Checking archive/atm/hist/extendedOutput.v3.LR.historical_0101.eam.h0.2000-11.nc
INFO: Checking archive/atm/hist/extendedOutput.v3.LR.historical_0101.eam.h0.2000-12.nc
INFO: No failures detected when checking the files. If you have a log file, run "grep -i Exception <log-file>" to double check.
```

The directory structure now looks like this:
```
$ pwd
/pscratch/sd/f/forsyth/e3sm_tutorial/check_output
$ ls # The default name of the cache is "zstash"
zstash
$ ls zstash/
index.db
$ ls ..
check_output  workdir

# From this point on, no changes since `create`:

$ ls ../workdir/
zstash_v3.LR.historical_0101
$ ls ../workdir/zstash_v3.LR.historical_0101/ # This is our cache from the `create` command!
index.db

$ hsi
$ ls tutorial_archive_20240507

tutorial_archive_20240507:
000000.tar   index.db 
$ exit
```

</details>

## zstash ls

```
$ cd ..
$ mkdir ls_output
$ cd ls_output
```

We can check the actual contents on HPSS by running:
```
$ zstash ls --hpss=tutorial_archive_20240507
```

<details>
<summary>Output</summary>

```
For help, please see https://e3sm-project.github.io/zstash. Ask questions at https://github.com/E3SM-Project/zstash/discussions/categories/q-a.
INFO: Transferring file from HPSS: zstash/index.db
archive/atm/hist/extendedOutput.v3.LR.historical_0101.eam.h0.2000-10.nc
archive/atm/hist/extendedOutput.v3.LR.historical_0101.eam.h0.2000-11.nc
archive/atm/hist/extendedOutput.v3.LR.historical_0101.eam.h0.2000-12.nc
```

The directory structure now looks like this:
```
$ pwd
/pscratch/sd/f/forsyth/e3sm_tutorial/ls_output
$ ls # The default name of the cache is "zstash"
zstash
$ ls zstash/
index.db
$ ls ..
check_output  ls_output  workdir

# From this point on, no changes since `create`:

$ ls ../workdir/
zstash_v3.LR.historical_0101
$ ls ../workdir/zstash_v3.LR.historical_0101/ # This is our cache from the `create` command!
index.db

$ hsi
$ ls tutorial_archive_20240507

tutorial_archive_20240507:
000000.tar   index.db 
$ exit
```

</details>

## zstash extract

```
$ cd ..
$ mkdir extract_output
$ cd extract_output
```

We can also extract the archive's contents into our own directory:
```
$ zstash extract --hpss=tutorial_archive_20240507
```

<details>
<summary>Output</summary>

```
For help, please see https://e3sm-project.github.io/zstash. Ask questions at https://github.com/E3SM-Project/zstash/discussions/categories/q-a.
INFO: Transferring file from HPSS: zstash/index.db
INFO: Transferring file from HPSS: zstash/000000.tar
INFO: zstash/000000.tar exists. Checking expected size matches actual size.
INFO: Opening tar archive zstash/000000.tar
INFO: Extracting archive/atm/hist/extendedOutput.v3.LR.historical_0101.eam.h0.2000-10.nc
INFO: Extracting archive/atm/hist/extendedOutput.v3.LR.historical_0101.eam.h0.2000-11.nc
INFO: Extracting archive/atm/hist/extendedOutput.v3.LR.historical_0101.eam.h0.2000-12.nc
INFO: No failures detected when extracting the files. If you have a log file, run "grep -i Exception <log-file>" to double check.
```

The directory structure now looks like this:
```
$ pwd
/pscratch/sd/f/forsyth/e3sm_tutorial/extract_output
$ ls # The default name of the cache is "zstash"
archive  zstash
$ ls zstash/
index.db
$ ls archive # This is the extracted output!
atm
$ ls archive/atm/hist/
extendedOutput.v3.LR.historical_0101.eam.h0.2000-10.nc
extendedOutput.v3.LR.historical_0101.eam.h0.2000-11.nc
extendedOutput.v3.LR.historical_0101.eam.h0.2000-12.nc
$ ls ..
check_output  extract_output  ls_output  workdir

# From this point on, no changes since `create`:

$ ls ../workdir/
zstash_v3.LR.historical_0101
$ ls ../workdir/zstash_v3.LR.historical_0101/ # This is our cache from the `create` command!
index.db

$ hsi
$ ls tutorial_archive_20240507

tutorial_archive_20240507:
000000.tar   index.db 
$ exit
```

</details>
