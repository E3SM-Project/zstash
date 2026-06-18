# How to profile zstash's performance

Performance profiling should be done on Perlmutter. We're keeping the performance records in a long-term directory specified by `performance_archive_dir` in your config file (see below). (NOTE: this is currently user-specific. If we start having many other developers running performance profiling, we may try to find a more centralized location.)

## Setup

All parameters for both scripts live in a single config file (`perf.cfg`) so you never need to edit the scripts themselves. Copy the provided template and fill in your values:

```bash
cd tests/performance/
cp perf.cfg my_run.cfg  # or just edit perf.cfg in place
```

The config file uses a simple `key=value` format (lines starting with `#` are comments). It is shared between the bash script and the Python visualizer.

> **Perlmutter path convention:** home and scratch directories follow the pattern
> `/global/homes/u/username/...` and `/pscratch/sd/u/username/...`
> where `u` is the first letter of your username.
> The placeholder `u/username` in the template should be replaced accordingly.

## Generate performance data

Edit the run metadata section of your cfg file:

```ini
# Use /pscratch since a lot of data will be transferred.
# The results csv alone will be copied to a long-term directory at the end.
work_dir=/pscratch/sd/u/username/zstash_performance/
unique_id=performance_20260603

# The environment that zstash will be run in.
# Using Unified environment:
environment_commands=source /global/common/software/e3sm/anaconda_envs/load_latest_e3sm_unified_pm-cpu.sh
# Example dev environment:
# environment_commands=source /global/homes/u/username/miniforge3/etc/profile.d/conda.sh ; conda activate zstash-pr427-20260603

# Long-term directory where the results CSV is archived after the run.
performance_archive_dir=/global/homes/u/username/zstash_performance_records
```

These parameters you probably won't have a need to change:

```ini
# Directories to run zstash create/update/extract on.
dir_to_copy_from=/global/cfs/cdirs/e3sm/forsyth/E3SMv2/v2.LR.historical_0201/
subdir0=build/
subdir1=run/
subdir2=init/

# Which --hpss settings to run (space-separated; comment out any to skip):
HPSS_OPTIONS=none hpss globus

# Used for the "hpss" option:
dst_hpss_path=/home/u/username/zstash_performance

# Used for the "globus" option:
fresh_globus=true  # prompts a fresh Globus authentication
dst_endpoint_uuid=15288284-7006-4041-ba1a-6b52501e49f1  # LCRC's endpoint
dst_endpoint_archive_dir=/lcrc/group/e3sm/username/zstash_performance_dst_dir/
```

Once you have the parameters set up, run:

```bash
cd tests/performance/
./generate_performance_data.bash my_run.cfg
```

If no cfg file argument is given, the script looks for `perf.cfg` in the same directory.

Results will be saved to `${work_dir}${unique_id}/results.csv`. To keep all records together in a non-scratch space, the results csv is also copied to `${performance_archive_dir}/${unique_id}_results.csv`.

## Visualize performance

Edit the visualizer section of your cfg file:

```ini
# Path to the results CSV to show in Figure 1.
# This should be the results.csv you just generated in the step above.
results_csv=/pscratch/sd/u/username/zstash_performance/performance_20260603/results.csv

# Path to a baseline results CSV to compare against in Figure 2.
# Leave blank to skip Figure 2.
# This will typically be the second-to-latest results.csv in the records space.
baseline_results_csv=/pscratch/sd/u/username/zstash_performance/performance_20260414/results.csv

# Output path for the saved figures.
# Leave blank to display interactively instead of saving.
# Make sure to use the web server path, i.e., /global/cfs/cdirs/e3sm/www/...
output_path=/global/cfs/cdirs/e3sm/www/username/zstash_performance/performance_pr427_20260603.png
```

Once you have the parameters set up, run:

```bash
cd tests/performance/
python visualize_performance.py --cfg my_run.cfg
```

If `--cfg` is omitted, the script looks for `perf.cfg` in the same directory.

The script will print both the file path and the URL to access the plots.

## For reference

Records made before the long-term record space was made have been copied to it via:
```bash
SCRATCH_SPACE=/pscratch/sd/f/forsyth/zstash_performance
RECORDS_SPACE=/global/homes/f/forsyth/zstash_performance_records

for unique_id in \
  performance_20260225 \
  performance_20260226_pr402 \
  performance_20260226_pr424 \
  performance_20260226_pr428 \
  performance_20260402 \
  performance_20260414 \
  performance_pr416_20260403 \
  performance_pr416_20260406
do
  cp "${SCRATCH_SPACE}/${unique_id}/results.csv" "${RECORDS_SPACE}/${unique_id}_results.csv"
done
```
