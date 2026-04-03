#!/bin/bash
set -e

# Analogous to CI/CD matrix testing of Python versions,
# here we will do a matrix performance profiling
# by comparing runtimes for create/update/extract:
# - On multiple directories
# - With `--hpss=none`, with HPSS path, with Globus

# We will also compare `zstash extract` in sequential-mode and parallel-mode

###############################################################################
# Manually edit parameters here:

# Run from Perlmutter, so that we can do both
# a direct transfer to HPSS & a Globus transfer to Chrysalis
work_dir=/pscratch/sd/f/forsyth/zstash_performance/
unique_id=performance_20260402

dir_to_copy_from=/global/cfs/cdirs/e3sm/forsyth/E3SMv2/v2.LR.historical_0201/
subdir0=build/
subdir1=run/
subdir2=init/
###
# For reference, these files have these sizes and number of files
# (Paths are from Chrysalis, but the data is identical on Perlmutter)

# Analyzing: /lcrc/group/e3sm/ac.forsyth2/E3SMv2/v2.LR.historical_0201/build/
# Total size: 1.2GiB
# Number of files: 7046
# => Lots of small files

# Analyzing: /lcrc/group/e3sm/ac.forsyth2/E3SMv2/v2.LR.historical_0201/run/
# Total size: 11GiB
# Number of files: 111

# Analyzing: /lcrc/group/e3sm/ac.forsyth2/E3SMv2/v2.LR.historical_0201/init/
# Total size: 6.9GiB
# Number of files: 14
# => A few large files
###


# For `--hpss=...`
# Which HPSS options to run. Comment out any you want to skip.
# Options: "none"  "hpss"  "globus"
HPSS_OPTIONS=("none" "hpss" "globus")

dst_hpss_path=/home/f/forsyth/zstash_performance

# For `--hpss=globus...`
fresh_globus=true
# ENDPOINT UUIDS:
# LCRC_IMPROV_DTN_ENDPOINT=15288284-7006-4041-ba1a-6b52501e49f1
# NERSC_PERLMUTTER_ENDPOINT=6bdc7956-fc0f-4ad2-989c-7aa5ee643a79
# NERSC_HPSS_ENDPOINT=9cd89cfd-6d04-11e5-ba46-22000b92c6ec
# PIC_COMPY_DTN_ENDPOINT=68fbd2fa-83d7-11e9-8e63-029d279f7e24
# GLOBUS_TUTORIAL_COLLECTION_1_ENDPOINT=6c54cade-bde5-45c1-bdea-f4bd71dba2cc
dst_endpoint_uuid=15288284-7006-4041-ba1a-6b52501e49f1
dst_endpoint_archive_dir=/lcrc/group/e3sm/ac.forsyth2/zstash_performance_dst_dir/

###############################################################################
# Utility functions

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Functions to print colored messages
print_step() {
    echo -e "${CYAN}[STEP]${NC} $1"
}

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

confirm()
{
    read -p "$1 (y/n): " -n 1 -r
    echo
    [[ $REPLY =~ ^[Yy]$ ]]
}

validate_configuration()
{
    local dir_to_copy_from="${1}"
    local subdir0="${2}"
    local subdir1="${3}"
    local subdir2="${4}"

    print_step "Validating configuration..."

    if [ ! -d "$dir_to_copy_from" ]; then
        print_error "Source directory does not exist: $dir_to_copy_from"
        exit 1
    fi

    if [ "$subdir0" != "none" ] && [ -n "$subdir0" ]; then
        if [ ! -d "${dir_to_copy_from}${subdir0}" ]; then
            print_error "subdir0 does not exist: ${dir_to_copy_from}${subdir0}"
            exit 1
        fi
    fi

    if [ "$subdir1" != "none" ] && [ -n "$subdir1" ]; then
        if [ ! -d "${dir_to_copy_from}${subdir1}" ]; then
            print_error "subdir1 does not exist: ${dir_to_copy_from}${subdir1}"
            exit 1
        fi
    fi

    if [ "$subdir2" != "none" ] && [ -n "$subdir2" ]; then
        if [ ! -d "${dir_to_copy_from}${subdir2}" ]; then
            print_error "subdir2 does not exist: ${dir_to_copy_from}${subdir2}"
            exit 1
        fi
    fi

    print_success "Configuration validated"
}

refresh_globus()
{
    print_step "Setting up fresh Globus authentication..."

    # 1. Activate endpoints
    echo "Go to https://app.globus.org/file-manager?two_pane=true > For 'Collection', choose the endpoints you're using, and authenticate if needed:"
    echo "LCRC Improv DTN, NERSC Perlmutter, NERSC HPSS, pic#compy-dtn"
    if ! confirm "Have you authenticated into the correct endpoints?"; then
        exit 1
    fi

    # 2. Reset authentication token files
    INI_PATH=${HOME}/.zstash.ini
    TOKEN_FILE=${HOME}/.zstash_globus_tokens.json

    if [ -f "${INI_PATH}" ]; then
        rm -f "${INI_PATH}"
        print_info "Removed ${INI_PATH}"
    fi

    if [ -f "${TOKEN_FILE}" ]; then
        rm -f "${TOKEN_FILE}"
        print_info "Removed ${TOKEN_FILE}"
    fi

    # 3. Reset Globus consents
    echo "https://auth.globus.org/v2/web/consents > Globus Endpoint Performance Monitoring > rescind all"
    if ! confirm "Have you revoked Globus consents?"; then
        exit 1
    fi

    print_success "Globus authentication reset complete"
}

# Parse the real-time (wall clock) seconds from the output of `time`.
# `time` writes to stderr a block like:
#   real    1m23.456s
#   user    0m12.345s
#   sys     0m 1.234s
# We capture both stdout+stderr into the log, then grep for the real line.
parse_elapsed_seconds()
{
    local log_file="${1}"
    # Extract "Xm Y.ZZZs" and convert to total seconds
    awk '/^real/ {
        split($2, a, "m");
        mins = a[1];
        secs = substr(a[2], 1, length(a[2])-1);
        printf "%.3f\n", mins*60 + secs
    }' "${log_file}"
}

###############################################################################
# Core functions

run_create()
{
    local dir_to_copy_from="${1}"
    local subdir="${2}"
    local archive_dir="${3}"
    local hpss_path="${4}"
    local cache_dir="${5}"
    local create_log="${6}"

    print_step "Starting CREATE operation..."

    print_info "Copying data from ${dir_to_copy_from}${subdir}"
    cp -r "${dir_to_copy_from}${subdir}" "${archive_dir}${subdir}"

    print_info "Running zstash create..."
    print_info "Command: zstash create --hpss=${hpss_path} --cache=${cache_dir} -v ${archive_dir}"

    # We must be outside archive_dir when running create
    if { time zstash create --hpss="${hpss_path}" --cache="${cache_dir}" -v "${archive_dir}" ; } 2>&1 | tee "${create_log}"; then
        print_success "zstash create completed successfully"
    else
        print_error "zstash create failed with exit code $?"
        exit 1
    fi
}

run_update()
{
    local dir_to_copy_from="${1}"
    local subdir="${2}"
    local archive_dir="${3}"
    local hpss_path="${4}"
    local cache_dir="${5}"
    local update_log="${6}"

    print_step "Starting UPDATE operation..."

    print_info "Copying additional data from ${dir_to_copy_from}${subdir}"
    cp -r "${dir_to_copy_from}${subdir}" "${archive_dir}${subdir}"

    print_info "Running zstash update..."
    print_info "Command: zstash update --hpss=${hpss_path} --cache=${cache_dir} -v"

    # zstash update must be run from within the archive directory
    pushd "${archive_dir}" > /dev/null
    if { time zstash update --hpss="${hpss_path}" --cache="${cache_dir}" -v ; } 2>&1 | tee "${update_log}"; then
        print_success "zstash update completed successfully"
    else
        print_error "zstash update failed with exit code $?"
        popd > /dev/null
        exit 1
    fi
    popd > /dev/null
}

run_extract()
{
    local extract_dir="${1}"
    local hpss_path="${2}"
    local num_workers="${3}"
    local cache_dir="${4}"
    local extract_log="${5}"

    print_step "Starting EXTRACT operation (workers=${num_workers})..."

    print_info "Running zstash extract..."
    print_info "Command: zstash extract --hpss=${hpss_path} --workers=${num_workers} --cache=${cache_dir} -v"

    # zstash extract must be run from within the extraction directory
    pushd "${extract_dir}" > /dev/null
    if { time zstash extract --hpss="${hpss_path}" --workers="${num_workers}" --cache="${cache_dir}" -v ; } 2>&1 | tee "${extract_log}"; then
        print_success "zstash extract completed successfully"
    else
        print_error "zstash extract failed with exit code $?"
        popd > /dev/null
        exit 1
    fi
    popd > /dev/null
}

###############################################################################
# Results tracking

# CSV file to collect all runtimes for later visualization
results_csv="${work_dir}${unique_id}/results.csv"

record_result()
{
    local test_label="${1}"   # e.g. "01"
    local create_subdir="${2}"
    local update_subdir="${3}"
    local hpss_label="${4}"   # "none", "hpss", "globus"
    local operation="${5}"    # "create", "update", "extract_seq", "extract_par"
    local log_file="${6}"

    local elapsed
    elapsed=$(parse_elapsed_seconds "${log_file}")
    echo "${test_label},${create_subdir},${update_subdir},${hpss_label},${operation},${elapsed}" >> "${results_csv}"
    print_info "Recorded: test=${test_label} op=${operation} hpss=${hpss_label} elapsed=${elapsed}s"
}

###############################################################################
# Main script:

validate_configuration "$dir_to_copy_from" "$subdir0" "$subdir1" "$subdir2"

if [ "${fresh_globus}" == "true" ] && [[ " ${HPSS_OPTIONS[*]} " == *" globus "* ]]; then
    refresh_globus
fi

# Create the top-level results directory and CSV header
mkdir -p "${work_dir}${unique_id}"
echo "test_label,create_subdir,update_subdir,hpss_label,operation,elapsed_seconds" > "${results_csv}"
print_info "Results CSV: ${results_csv}"

# Array of subdirectories
subdirs=("$subdir0" "$subdir1" "$subdir2")

# Define the 6 possible permutations as test configurations.
# Each string contains two space-separated indices into the subdirs array:
#   first index  = subdir used for create
#   second index = subdir used for update
declare -a test_configs=(
    "0 1"
    "0 2"
    "1 0"
    "1 2"
    "2 0"
    "2 1"
)
# FIX: removed erroneous commas that were present in the original array
declare -a test_labels=("01" "02" "10" "12" "20" "21")

# Loop through the 6 test configurations
for test_idx in 0 1 2 3 4 5; do
    # Parse the configuration
    read -r -a config <<< "${test_configs[$test_idx]}"
    i=${config[0]} # index for create subdir
    j=${config[1]} # index for update subdir

    # Get the subdirectories for this test
    create_subdir="${subdirs[$i]}"
    update_subdir="${subdirs[$j]}"

    test_label="${test_labels[$test_idx]}"

    print_step "=========================================="
    print_step "Running Test ${test_label}"
    print_step "  Create subdir: $create_subdir"
    print_step "  Update subdir: $update_subdir"
    print_step "=========================================="

    # Create unique work directories for this test
    work_subdir="${work_dir}${unique_id}/test${test_label}/"
    mkdir -p "${work_subdir}"

    log_dir="${work_subdir}logs/"
    mkdir -p "${log_dir}"

    # FIX: renamed `dst_hpss` local var to `dst_globus_path` so it does not
    # shadow/overwrite the top-level `dst_hpss_path` parameter.
    dst_globus_path="globus://${dst_endpoint_uuid}/${dst_endpoint_archive_dir}${unique_id}/test${test_label}/"

    # Iterate over the three HPSS modes
    declare -A hpss_path_map=(
        ["none"]="none"
        ["hpss"]="${dst_hpss_path}"
        ["globus"]="${dst_globus_path}"
    )

    for hpss_label in "${HPSS_OPTIONS[@]}"; do
        hpss_path="${hpss_path_map[$hpss_label]}"
        print_step "--- HPSS mode: ${hpss_label} (${hpss_path}) ---"

        # Each hpss mode gets its own subdirectories to avoid cross-contamination
        mode_dir="${work_subdir}${hpss_label}/"
        archive_dir="${mode_dir}archive_dir/"
        cache_dir="${mode_dir}cache/"
        mkdir -p "${archive_dir}" "${cache_dir}"

        create_log="${log_dir}create_${hpss_label}.log"
        update_log="${log_dir}update_${hpss_label}.log"

        # --- CREATE ---
        run_create "$dir_to_copy_from" "$create_subdir" "$archive_dir" "$hpss_path" "$cache_dir" "$create_log"
        record_result "$test_label" "$create_subdir" "$update_subdir" "$hpss_label" "create" "$create_log"

        # --- UPDATE ---
        run_update "$dir_to_copy_from" "$update_subdir" "$archive_dir" "$hpss_path" "$cache_dir" "$update_log"
        record_result "$test_label" "$create_subdir" "$update_subdir" "$hpss_label" "update" "$update_log"

        # --- EXTRACT (sequential=1 worker, parallel=2 workers) ---
        for num_workers in 1 2; do
            extract_log="${log_dir}extract_${hpss_label}_${num_workers}workers.log"
            extract_dir="${mode_dir}extract_${num_workers}workers/"
            mkdir -p "${extract_dir}"

            # FIX: pass num_workers argument (was missing in original)
            run_extract "$extract_dir" "$hpss_path" "$num_workers" "$cache_dir" "$extract_log"

            if [ "$num_workers" -eq 1 ]; then
                op_label="extract_seq"
            else
                op_label="extract_par"
            fi
            record_result "$test_label" "$create_subdir" "$update_subdir" "$hpss_label" "$op_label" "$extract_log"
        done
    done

    print_success "Test ${test_label} completed"
    echo ""
done

print_success "All tests completed. Results saved to: ${results_csv}"
print_info "Now edit IO paths and run: python visualize_performance.py"
