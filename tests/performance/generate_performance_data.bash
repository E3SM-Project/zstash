#!/bin/bash
set -e

# Analagous to CI/CD matrix testing of Python versions,
# here we will do a matrix performance profiling
# by comparing runtimes for create/update/extract:
# - On multiple directories
# - With `--hpss=none` with HPSS path, with Globus

# We will also compare `zstash extract` in sequential-mode and parallel-mode

###############################################################################
# Manually edit parameters here:

# Run from Perlmutter, so that we can do both
# a direct transfer to HPSS & a Globus transfer to Chrysalis
work_dir=/global/cfs/cdirs/e3sm/forsyth/zstash_performance/
unique_id=performance_20260225

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
dst_hpss=/home/f/forsyth/zstash_performance

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

valiate_configuration()
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

###############################################################################
# Core functions

run_create()
{
    local dir_to_copy_from="${1}"
    local subdir="${2}"
    local archive_dir="${3}"
    local dst_hpss="${4}"
    local cache_dir="${5}"
    local create_log="${6}"
    print_step "Starting CREATE operation..."

    print_info "Copying data from ${dir_to_copy_from}${subdir}"
    cp -r "${dir_to_copy_from}${subdir}" "${archive_dir}${subdir}"

    print_info "Running zstash create..."
    print_info "Command: zstash create --hpss=${dst_hpss} --cache=${cache_dir} -v ${archive_dir}"

    if { time zstash create --hpss="${dst_hpss}" --cache="${cache_dir}" -v "${archive_dir}" ; } 2>&1 | tee "${create_log}"; then
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
    local dst_hpss="${4}"
    local cache_dir="${5}"
    local update_log="${6}"

    print_step "Starting UPDATE operation..."

    print_info "Copying additional data from ${dir_to_copy_from}${subdir}"
    cp -r "${dir_to_copy_from}${subdir}" "${archive_dir}${subdir}"

    print_info "Running zstash update..."
    print_info "Command: zstash update --hpss=${dst_hpss} --cache=${cache_dir} -v"

    if { time zstash update --hpss="${dst_hpss}" --cache="${cache_dir}" -v ; } 2>&1 | tee "${update_log}"; then
        print_success "zstash update completed successfully"
    else
        print_error "zstash update failed with exit code $?"
        exit 1
    fi
}

run_extract()
{
    local archive_dir="${1}"
    local src_hpss="${2}"
    local num_workers="${3}"
    local cache_dir="${4}"
    local extract_log="${5}"

    print_step "Starting EXTRACT operation..."

    print_info "Running zstash extract..."
    print_info "Command: zstash extract --hpss=${src_hpss} --workers=${num_workers} --cache=${cache_dir} -v"

    if { time zstash extract --hpss="${src_hpss}" --workers="${num_workers}" --cache="${cache_dir}" -v ; } 2>&1 | tee "${extract_log}"; then
        print_success "zstash extract completed successfully"
    else
        print_error "zstash extract failed with exit code $?"
        exit 1
    fi
}

###############################################################################
# Main script:

valiate_configuration $dir_to_copy_from $subdir0 $subdir1 $subdir2

if [ "${fresh_globus}" == "true" ]; then
    refresh_globus
fi

# Array of subdirectories
subdirs=("$subdir0" "$subdir1" "$subdir2")
# Define the 6 possible permutations as test configurations
# Each array contains indices into the subdirs array
declare -a test_configs=(
    "0 1"
    "0 2"
    "1 0"
    "1 2"
    "2 0"
    "2 1"
)
declare -a test_labels=("01" "02" "10", "12", "20", "21")
declare -a test_names

# Loop through the 6 test configurations
for test_idx in 0 1 2 3 4 5; do
    # Parse the configuration
    config=(${test_configs[$test_idx]})
    i=${config[0]} # 1st element (create-subdir)
    j=${config[1]} # 2nd element (update-subdir)

    # Get the subdirectories for this test
    create_subdir="${subdirs[$i]}"
    update_subdir="${subdirs[$j]}"

    # Create a label for this test
    test_label="${test_labels[$test_idx]}"
    test_names+=("Test_${test_label}")

    print_step "=========================================="
    print_step "Running Test ${test_label}"
    print_step "  Create: $create_subdir"
    print_step "  Update: $update_subdir"
    print_step "=========================================="

    # Create unique work directories for this test
    dst_endpoint_archive_subdir="${dst_endpoint_archive_dir}${unique_id}/test${test_label}/"
    work_subdir="${work_dir}${unique_id}/test${test_label}/"
    mkdir -p "${work_subdir}"
    archive_dir="${work_subdir}archive_dir/"
    cache_dir="${work_subdir}cache/"
    log_dir="${work_subdir}logs/"
    mkdir -p "${archive_dir}"
    mkdir -p "${cache_dir}"
    mkdir -p "${log_dir}"

    # Define log file paths
    create_log="${log_dir}create.log"
    update_log="${log_dir}update.log"

    print_success "Work directories created at ${work_subdir}"

    dst_globus="globus://${dst_endpoint_uuid}/${dst_endpoint_archive_subdir}"
    dst_hpss=""


    for hpss_path in "none" "${dst_hpss}" "${dst_globus}"; do
        cd "${work_subdir}"
        run_create "$dir_to_copy_from" "$create_subdir" "$archive_dir" "$hpss_path" "$cache_dir" "$create_log"
        # For update, we need to be in archive_dir:
        cd "${archive_dir}"
        run_update "$dir_to_copy_from" "$update_subdir" "$archive_dir" "$hpss_path" "$cache_dir" "$update1_log"

        cd "${work_subdir}"

        # For extraction, dst should really be thought of as the src
        for num_workers in 1 2; do
            # For extraction, we must NOT be in archive_dir
            extraction_subdir=${work_subdir}extract_subdir_${num_workers}_workers
            mkdir ${extract_subdir}
            cd ${extract_subdir}
            run_extract "$archive_dir" "$hpss_path" "$cache_dir" "$extract_log"
            cd "${work_subdir}"
        done
    done

    print_success "Test ${test_label} completed"
    echo ""
done
