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

```
$ cd /pscratch/sd/f/forsyth/e3sm_tutorial_output

# We can create an archive on NERSC HPSS by running:
$ zstash create --hpss=tutorial_archive_20240504 --cache=/pscratch/sd/f/forsyth/e3sm_tutorial_output/zstash_v3.LR.historical_0101 --include=archive/atm/hist/extendedOutput.v3.LR.historical_0101.eam.h0.2000-1*.nc /global/cfs/cdirs/e3sm/www/Tutorials/2024/simulations/extendedOutput.v3.LR.historical_0101/
# hpss -- what HPSS archive should we use? Note: you can also use Globus, or set to "None" to archive locally
# cache -- what cache should we use? This is important to set here because we don't have write permissions to write the cache in the directory we're archiving
# include -- what files should we include in the archive? Often you'd want to include the entire directory. For the purposes of the tutorial however, it speeds things up to only archive a little bit.

# Warning -- It is important to not pipe the `zstash` command line output to the directory being archived.
# (e.g., with `2>&1 | tee my_zstash_output.log`)
# The log file will be corrupted, causing `zstash check` and `zstash extract` to error.
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
INFO: Transferring file to HPSS: /pscratch/sd/f/forsyth/e3sm_tutorial_output/zstash_v3.LR.historical_0101/000000.tar
INFO: Transferring file to HPSS: /pscratch/sd/f/forsyth/e3sm_tutorial_output/zstash_v3.LR.historical_0101/index.db
```

</details>

```
# We can verify zstash was successful by running:
$ zstash check --hpss=tutorial_archive_20240504 --cache=/pscratch/sd/f/forsyth/e3sm_tutorial_output/zstash_v3.LR.historical_0101
```

<details>
<summary>Output</summary>

```
For help, please see https://e3sm-project.github.io/zstash. Ask questions at https://github.com/E3SM-Project/zstash/discussions/categories/q-a.
INFO: Transferring file from HPSS: /pscratch/sd/f/forsyth/e3sm_tutorial_output/zstash_v3.LR.historical_0101/000000.tar
INFO: /pscratch/sd/f/forsyth/e3sm_tutorial_output/zstash_v3.LR.historical_0101/000000.tar exists. Checking expected size matches actual size.
INFO: Opening tar archive /pscratch/sd/f/forsyth/e3sm_tutorial_output/zstash_v3.LR.historical_0101/000000.tar
INFO: Checking archive/atm/hist/extendedOutput.v3.LR.historical_0101.eam.h0.2000-10.nc
INFO: Checking archive/atm/hist/extendedOutput.v3.LR.historical_0101.eam.h0.2000-11.nc
INFO: Checking archive/atm/hist/extendedOutput.v3.LR.historical_0101.eam.h0.2000-12.nc
INFO: No failures detected when checking the files. If you have a log file, run "grep -i Exception <log-file>" to double check.
```

</details>

```
# We can check the actual contents on HPSS by running:
$ zstash ls --hpss=tutorial_archive_20240504 --cache=/pscratch/sd/f/forsyth/e3sm_tutorial_output/zstash_v3.LR.historical_0101
```

<details>
<summary>Output</summary>

```
For help, please see https://e3sm-project.github.io/zstash. Ask questions at https://github.com/E3SM-Project/zstash/discussions/categories/q-a.
archive/atm/hist/extendedOutput.v3.LR.historical_0101.eam.h0.2000-10.nc
archive/atm/hist/extendedOutput.v3.LR.historical_0101.eam.h0.2000-11.nc
archive/atm/hist/extendedOutput.v3.LR.historical_0101.eam.h0.2000-12.nc
```

</details>

```
# We can also extract the archive's contents into our own directory:
$ zstash extract --hpss=tutorial_archive_20240504 --cache=/pscratch/sd/f/forsyth/e3sm_tutorial_output/zstash_v3.LR.historical_0101
```

<details>
<summary>Output</summary>

```
For help, please see https://e3sm-project.github.io/zstash. Ask questions at https://github.com/E3SM-Project/zstash/discussions/categories/q-a.
INFO: Transferring file from HPSS: /pscratch/sd/f/forsyth/e3sm_tutorial_output/zstash_v3.LR.historical_0101/000000.tar
INFO: /pscratch/sd/f/forsyth/e3sm_tutorial_output/zstash_v3.LR.historical_0101/000000.tar exists. Checking expected size matches actual size.
INFO: Opening tar archive /pscratch/sd/f/forsyth/e3sm_tutorial_output/zstash_v3.LR.historical_0101/000000.tar
INFO: Extracting archive/atm/hist/extendedOutput.v3.LR.historical_0101.eam.h0.2000-10.nc
INFO: Extracting archive/atm/hist/extendedOutput.v3.LR.historical_0101.eam.h0.2000-11.nc
INFO: Extracting archive/atm/hist/extendedOutput.v3.LR.historical_0101.eam.h0.2000-12.nc
INFO: No failures detected when extracting the files. If you have a log file, run "grep -i Exception <log-file>" to double check.
```

</details>

In our directory `/pscratch/sd/f/forsyth/e3sm_tutorial_output`, we have:
```
$ ls zstash_v3.LR.historical_0101/
index.db

$ ls archive/atm/hist/
extendedOutput.v3.LR.historical_0101.eam.h0.2000-10.nc  extendedOutput.v3.LR.historical_0101.eam.h0.2000-12.nc
extendedOutput.v3.LR.historical_0101.eam.h0.2000-11.nc
```
