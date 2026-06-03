# How to profile zstash's performance

Performance profiling should be done on Perlmutter. We're keeping the performance records in `/global/homes/f/forsyth/zstash_performance_records`. (NOTE: this is currently user-specific. If we start having many other developers running performance profiling, we may try to find a more centralized location.)

## Generate performance data.bash

In `zstash/tests/performance/generate_performance_data.bash`, edit the run metadata:

```bash
# The performance data will end up in `results_csv="${work_dir}${unique_id}/results.csv"`
# Use `/pscratch` since a lot of data will be transferred.
# The results csv alone will be copied to a long-term (i.e., non-scratch) directory at the end.
work_dir=/pscratch/sd/f/forsyth/zstash_performance/
unique_id=performance_20260603

# This is the environment that zstash will be run in.
# Using Unified environment:
environment_commands="source /global/common/software/e3sm/anaconda_envs/load_latest_e3sm_unified_pm-cpu.sh"
# Example dev environment:
environment_commands="source /global/homes/f/forsyth/miniforge3/etc/profile.d/conda.sh; conda activate zstash-pr427-20260603"
```

These parameters you probably won't have a need to change:
```bash
# These are the directories to run `zstash create`, `zstash update`, and `zstash extract` on.
dir_to_copy_from=/global/cfs/cdirs/e3sm/forsyth/E3SMv2/v2.LR.historical_0201/
subdir0=build/
subdir1=run/
subdir2=init/

# This specifies which `--hpss` settings will be run:
HPSS_OPTIONS=("none" "hpss" "globus")

# This is what will be used for the "hpss" option:
dst_hpss_path=/home/f/forsyth/zstash_performance

# This is what will be used the "globus" option:
fresh_globus=true # This will prompt a fresh Globus authentication
dst_endpoint_uuid=15288284-7006-4041-ba1a-6b52501e49f1 # This is LCRC's endpoint
dst_endpoint_archive_dir=/lcrc/group/e3sm/ac.forsyth2/zstash_performance_dst_dir/
```

Results will be saved to `${results_csv}` (recall `results_csv="${work_dir}${unique_id}/results.csv`). To keep all records together in a non-scratch space, the results csv is also copied to: `/global/homes/f/forsyth/zstash_performance_records/${unique_id}_results.csv`.

## Visualize performance

In `zstash/tests/performance/visualize_performance.py`, edit the run metadata:

```python
# The results to show in Fig. 1
# This should be the results.csv you just generated in the step above.
RESULTS_CSV: str = "/pscratch/sd/f/forsyth/zstash_performance/performance_20260414/results.csv"

# The results to compare against in Fig. 2.
# Set to None to skip Fig. 2.
# This will typically be the second-to-oldest results.csv in the records space
BASELINE_RESULTS_CSV: Optional[str] = "/pscratch/sd/f/forsyth/zstash_performance/performance_20260402/results.csv"

# Output path for the saved figures.
# Set to None to display interactively instead of saving.
# Make sure to put this on the web server path,
# i.e., /global/cfs/cdirs/e3sm/www/...
OUTPUT_PATH: Optional[str] = "/global/cfs/cdirs/e3sm/www/forsyth/zstash_performance/performance_pr427_20260603.png"
```

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
