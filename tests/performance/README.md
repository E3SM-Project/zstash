# How to profile zstash's performance

Performance profiling should be done on Perlmutter. We're keeping the performance records in a long-term directory specified by `performance_archive_dir` in your config file (see below). (NOTE: this is currently user-specific. If we start having many other developers running performance profiling, we may try to find a more centralized location.)

## Setup

To run the visualizer (`visualize_performance.py`), you need `matplotlib`, `numpy`, and `pandas`. The repo provides a minimal conda environment for this in `conda/perf.yml`:

```bash
conda env create -f conda/perf.yml -n zstash_perf
conda activate zstash_perf
python -m pip install .
```

All parameters for both scripts can live in a single config file (`key=value`) so you never need to edit the scripts themselves. Start by copying the generator template and fill in your values:

```bash
cd tests/performance/generate
cp perf.cfg my_run.cfg  # or just edit perf.cfg in place
```

The config file uses a simple `key=value` format (lines starting with `#` are comments). It is shared between the bash script and the Python visualizer.

To visualize, add the visualizer keys from `visualize/perf.cfg` into the same file.

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
gen_run_id=performance_20260603

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
cd tests/performance/generate/
./generate_performance_data.bash ../my_run.cfg
```

If no cfg file argument is given, the script looks for `perf.cfg` in the same directory.

Results will be saved to `${work_dir}${gen_run_id}/results.csv`. To keep all records together in a non-scratch space, the results csv is also copied to `${performance_archive_dir}/${gen_run_id}_results.csv`.

## Visualize performance

The visualizer lives in `tests/performance/visualize/`. Edit the visualizer section of your cfg file:

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
output_path=/global/cfs/cdirs/e3sm/www/username/zstash_performance/performance_20260603.png
```

The following options are available for finer control over the visualizer output:

```ini
# Subset of HPSS modes to include in every figure (comma-separated).
# Valid values: none, hpss, globus. Leave blank to include all three.
# Example: hpss_filter=none,hpss
hpss_filter=

# Top-level subdirectory for all output files.
# When set (together with most_recent_gen_run_id), figures are placed under
# <output_dir>/<viz_run_id>/.
# When blank, the existing behaviour (stem of output_path as filename stem) is used.
viz_run_id=pr427_20260603

# Identifier for the most recent generate run; used as the filename stem for
# Figures 1 & 2 and as a subdirectory under viz_run_id/.
# Requires viz_run_id to also be set.
most_recent_gen_run_id=performance_20260603

# Which figures to produce (comma-separated). Valid values: 1, 2, 3, 4.
# Leave blank to produce all applicable figures.
# Note: Figure 2 still requires baseline_results_csv; Figures 3/4 still require
# performance_archive_dir, regardless of this setting.
figures=1,2,3,4
```

When both `viz_run_id` and `most_recent_gen_run_id` are set, output files are laid out as:

```
<output_dir>/<viz_run_id>/<most_recent_gen_run_id>.png
<output_dir>/<viz_run_id>/<most_recent_gen_run_id>_vs_baseline.png
<output_dir>/<viz_run_id>/record_create_and_update.png
<output_dir>/<viz_run_id>/record_extract.png
```

Once you have the parameters set up, run:

```bash
cd tests/performance/visualize/
python visualize_performance.py --cfg ../my_run.cfg
```

If `--cfg` is omitted, the script looks for `perf.cfg` in the same directory.

The script will print both the file path and the URL to access the plots.

### Figures produced

**Figure 1 – Performance overview** (`results_csv` required): A 2×2 grid of subplots (one per operation: create, update, extract_seq, extract_par) plus a 5th subplot comparing sequential vs parallel extract side-by-side. Bars represent HPSS mode (none / hpss / globus); individual data points are overlaid as dots when multiple test configs share the same directory.

**Figure 2 – Baseline comparison** (`baseline_results_csv` required): Same layout as Figure 1, but each cell shows two bars (current = solid, baseline = hatched) with a current/baseline ratio annotation. Ratio > 1 indicates a regression (slower); ratio < 1 indicates an improvement (faster).

**Figure 3 – Historical archive for create & update** (`performance_archive_dir` required): A 2×2 grid of time-series and box plots for create and update operations across all historical CSVs in the archive directory.

**Figure 4 – Historical archive for extract** (`performance_archive_dir` required): Same layout as Figure 3, for extract_seq and extract_par operations.

## For reference

Records made before the long-term record space was made have been copied to it via:
```bash
SCRATCH_SPACE=/pscratch/sd/u/username/zstash_performance
RECORDS_SPACE=/global/homes/u/username/zstash_performance_records

# Example:
for gen_run_id in \
  performance_20260225 \
  performance_20260226_pr402 \
  performance_20260226_pr424 \
  performance_20260226_pr428 \
  performance_20260402 \
  performance_20260414 \
  performance_pr416_20260403 \
  performance_pr416_20260406
do
  cp "${SCRATCH_SPACE}/${gen_run_id}/results.csv" "${RECORDS_SPACE}/${gen_run_id}_results.csv"
done
```
