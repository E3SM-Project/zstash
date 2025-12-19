#!/bin/bash
set -e

# Analagous to CI/CD matrix testing of Python 3.11, 3.12, 3.13,
# here we will do a matrix performance profiling by comparing performance numbers
# for every combination of create/update/update on 3 different directories.

###############################################################################
# Manually edit parameters here:

work_dir=/lcrc/group/e3sm/ac.forsyth2/zstash_performance/
unique_id=profile_update_20251219

dir_to_copy_from=/lcrc/group/e3sm/ac.forsyth2/E3SMv2/v2.LR.historical_0201/
subdir0=build/
subdir1=run/
subdir2=init/

fresh_globus=true
# ENDPOINT UUIDS:
# LCRC_IMPROV_DTN_ENDPOINT=15288284-7006-4041-ba1a-6b52501e49f1
# NERSC_PERLMUTTER_ENDPOINT=6bdc7956-fc0f-4ad2-989c-7aa5ee643a79
# NERSC_HPSS_ENDPOINT=9cd89cfd-6d04-11e5-ba46-22000b92c6ec
# PIC_COMPY_DTN_ENDPOINT=68fbd2fa-83d7-11e9-8e63-029d279f7e24
# GLOBUS_TUTORIAL_COLLECTION_1_ENDPOINT=6c54cade-bde5-45c1-bdea-f4bd71dba2cc
dst_endpoint_uuid=9cd89cfd-6d04-11e5-ba46-22000b92c6ec
dst_archive_dir=/home/f/forsyth/zstash_performance/

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

analyze_directories() {
    # Array of directories to analyze
    local dirs=("$@")
    local cumulative_size=0

    for dir in "${dirs[@]}"; do
        if [[ ! -d "$dir" ]]; then
            echo "Warning: '$dir' is not a directory, skipping..."
            continue
        fi

        echo "========================================"
        echo "Analyzing: $dir"
        echo "========================================"

        # Get total size in bytes for calculation
        local total_size_bytes=$(du -sb "$dir" 2>/dev/null | cut -f1)

        # Add to cumulative total
        cumulative_size=$((cumulative_size + total_size_bytes))

        # Convert to human-readable format
        local total_size_human=$(numfmt --to=iec-i --suffix=B "$total_size_bytes" 2>/dev/null || echo "${total_size_bytes}B")
        echo "Total size: $total_size_human"

        # 2. Total number of files (excluding directories)
        local file_count=$(find "$dir" -type f 2>/dev/null | wc -l)
        echo "Number of files: $file_count"

        # 3. Average file size
        if [[ $file_count -gt 0 ]]; then
            local avg_bytes=$((total_size_bytes / file_count))

            # Convert to human-readable format
            if [[ $avg_bytes -lt 1024 ]]; then
                echo "Average file size: ${avg_bytes}B"
            elif [[ $avg_bytes -lt 1048576 ]]; then
                echo "Average file size: $((avg_bytes / 1024))KB"
            elif [[ $avg_bytes -lt 1073741824 ]]; then
                echo "Average file size: $((avg_bytes / 1048576))MB"
            else
                echo "Average file size: $((avg_bytes / 1073741824))GB"
            fi
        else
            echo "Average file size: N/A (no files found)"
        fi

        echo ""
    done

    # Summary statistics
    echo "========================================"
    echo "SUMMARY"
    echo "========================================"

    local num_dirs=${#dirs[@]}
    local cumulative_human=$(numfmt --to=iec-i --suffix=B "$cumulative_size" 2>/dev/null || echo "${cumulative_size}B")
    echo "Cumulative size of all directories: $cumulative_human"

    if [[ $num_dirs -gt 1 ]]; then
        local permutation_count=3 # We're going to run 3 permutations of these directories
        local permutation_size=$((cumulative_size * permutation_count))
        local permutation_human=$(numfmt --to=iec-i --suffix=B "$permutation_size" 2>/dev/null || echo "${permutation_size}B")

        echo "Number of directories: $num_dirs"
        echo "Number of permutations we will test: $permutation_count"
        echo "Space needed: $permutation_human"
    fi
}

run_create()
{
    local dir_to_copy_from="${1}"
    local subdir="${2}"
    local archive_dir="${3}"
    local dst_endpoint_uuid="${4}"
    local dst_archive_subdir="${5}"
    local cache_dir="${6}"
    local create_log="${7}"
    print_step "Starting CREATE operation..."

    print_info "Copying data from ${dir_to_copy_from}${subdir}"
    cp -r "${dir_to_copy_from}${subdir}" "${archive_dir}${subdir}"

    print_info "Running zstash create..."
    print_info "Command: zstash create --hpss=globus://${dst_endpoint_uuid}/${dst_archive_subdir} --cache=${cache_dir} -v ${archive_dir}"

    if { time zstash create --hpss="globus://${dst_endpoint_uuid}/${dst_archive_subdir}" --cache="${cache_dir}" -v "${archive_dir}" ; } 2>&1 | tee "${create_log}"; then
        print_success "zstash create completed successfully"
    else
        print_error "zstash create failed with exit code $?"
        exit 1
    fi

    echo ""
    print_warning "Verification suggestion"
    echo "Go to your destination machine and run:"
    echo "  ls ${dst_archive_subdir}"
    echo "Expected output: 000000.tar   index.db"
    echo ""
}

run_update()
{
    local dir_to_copy_from="${1}"
    local subdir="${2}"
    local archive_dir="${3}"
    local dst_endpoint_uuid="${4}"
    local dst_archive_subdir="${5}"
    local cache_dir="${6}"
    local update_log="${7}"

    print_step "Starting UPDATE operation..."

    print_info "Copying additional data from ${dir_to_copy_from}${subdir}"
    cp -r "${dir_to_copy_from}${subdir}" "${archive_dir}${subdir}"

    print_info "Running zstash update..."
    print_info "Command: zstash update --hpss=globus://${dst_endpoint_uuid}/${dst_archive_subdir} --cache=${cache_dir} -v"

    if { time zstash update --hpss="globus://${dst_endpoint_uuid}/${dst_archive_subdir}" --cache="${cache_dir}" -v ; } 2>&1 | tee "${update_log}"; then
        print_success "zstash update completed successfully"
    else
        print_error "zstash update failed with exit code $?"
        exit 1
    fi

    echo ""
    print_warning "Verification suggestion:"
    echo "Go to your destination machine and run:"
    echo "  ls ${dst_archive_subdir}"
    echo "Expected output (after update1): 000000.tar   000001.tar   index.db"
    echo "Expected output (after update2): 000000.tar   000001.tar   000002.tar   index.db"
    echo ""
}

###############################################################################
# Main script:

valiate_configuration $dir_to_copy_from $subdir0 $subdir1 $subdir2

if [ "${fresh_globus}" == "true" ]; then
    refresh_globus
fi

analyze_directories "${dir_to_copy_from}${subdir0}" "${dir_to_copy_from}${subdir1}" "${dir_to_copy_from}${subdir2}"
# Array of subdirectories
subdirs=("$subdir0" "$subdir1" "$subdir2")
# Define the 3 specific test configurations
# Each array contains indices into the subdirs array
declare -a test_configs=(
    "0 1 2"  # 123: subdir0, subdir1, subdir2
    "1 2 0"  # 231: subdir1, subdir2, subdir0
    "2 0 1"  # 312: subdir2, subdir0, subdir1
)
declare -a test_labels=("123" "231" "312")
# Store all log files for combined analysis
declare -a all_update1_logs
declare -a all_update2_logs
declare -a test_names

# Loop through the 3 test configurations
for test_idx in 0 1 2; do
    # Parse the configuration
    config=(${test_configs[$test_idx]})
    i=${config[0]}
    j=${config[1]}
    k=${config[2]}

    # Get the subdirectories for this test
    create_subdir="${subdirs[$i]}"
    update1_subdir="${subdirs[$j]}"
    update2_subdir="${subdirs[$k]}"

    # Create a label for this test
    test_label="${test_labels[$test_idx]}"
    test_names+=("Test_${test_label}")

    print_step "=========================================="
    print_step "Running Test ${test_label}"
    print_step "  Create:  $create_subdir"
    print_step "  Update1: $update1_subdir"
    print_step "  Update2: $update2_subdir"
    print_step "=========================================="

    # Create unique work directories for this test
    dst_archive_subdir="${dst_archive_dir}${unique_id}/test${test_label}/"
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
    update1_log="${log_dir}update1.log"
    update2_log="${log_dir}update2.log"

    # Store logs for combined analysis
    all_update1_logs+=("$update1_log")
    all_update2_logs+=("$update2_log")

    print_success "Work directories created at ${work_subdir}"

    cd "${work_subdir}"
    run_create "$dir_to_copy_from" "$create_subdir" "$archive_dir" "$dst_endpoint_uuid" "$dst_archive_subdir" "$cache_dir" "$create_log"

    # For update, we need to be in archive_dir:
    cd "${archive_dir}"
    run_update "$dir_to_copy_from" "$update1_subdir" "$archive_dir" "$dst_endpoint_uuid" "$dst_archive_subdir" "$cache_dir" "$update1_log"
    run_update "$dir_to_copy_from" "$update2_subdir" "$archive_dir" "$dst_endpoint_uuid" "$dst_archive_subdir" "$cache_dir" "$update2_log"

    cd "${work_subdir}"

    print_success "Test ${test_label} completed"
    echo ""
done

# Combined analysis of all tests
print_step "=========================================="
print_step "COMBINED PERFORMANCE ANALYSIS"
print_step "=========================================="

combined_analysis_report="${work_dir}${unique_id}/combined_analysis.txt"

{
    echo "=========================================="
    echo "Combined Performance Analysis"
    echo "Generated: $(date)"
    echo "=========================================="
    echo ""
    echo "Test configurations:"
    echo "  Test 123: create=$subdir0, update1=$subdir1, update2=$subdir2"
    echo "  Test 231: create=$subdir1, update1=$subdir2, update2=$subdir0"
    echo "  Test 312: create=$subdir2, update1=$subdir1, update2=$subdir0"
    echo ""
    echo "=========================================="
    echo ""

    # Create markdown table header
    echo "| Test/Update | File Gathering | Database Comparison | Add Files |"
    echo "| --- | --- | --- | --- |"

    # Extract metrics for each test
    for idx in 0 1 2; do
        test_name="${test_names[$idx]}"
        u1_log="${all_update1_logs[$idx]}"
        u2_log="${all_update2_logs[$idx]}"

        # Extract Update 1 metrics
        u1_file_gathering=$(grep "File gathering:" "$u1_log" 2>/dev/null | grep -oP '\d+\.\d+s \(\d+\.\d+%\)' | head -1)
        u1_db_comparison=$(grep "Database comparison:" "$u1_log" 2>/dev/null | grep -oP '\d+\.\d+s \(\d+\.\d+%\)' | head -1)
        u1_add_files=$(grep "Add files:" "$u1_log" 2>/dev/null | grep -oP '\d+\.\d+s \(\d+\.\d+%\)' | head -1)

        # Extract Update 2 metrics
        u2_file_gathering=$(grep "File gathering:" "$u2_log" 2>/dev/null | grep -oP '\d+\.\d+s \(\d+\.\d+%\)' | head -1)
        u2_db_comparison=$(grep "Database comparison:" "$u2_log" 2>/dev/null | grep -oP '\d+\.\d+s \(\d+\.\d+%\)' | head -1)
        u2_add_files=$(grep "Add files:" "$u2_log" 2>/dev/null | grep -oP '\d+\.\d+s \(\d+\.\d+%\)' | head -1)

        # Default to N/A if not found
        u1_file_gathering=${u1_file_gathering:-"N/A"}
        u1_db_comparison=${u1_db_comparison:-"N/A"}
        u1_add_files=${u1_add_files:-"N/A"}
        u2_file_gathering=${u2_file_gathering:-"N/A"}
        u2_db_comparison=${u2_db_comparison:-"N/A"}
        u2_add_files=${u2_add_files:-"N/A"}

        # Print markdown table rows for both updates
        echo "| ${test_name}_Update1 | $u1_file_gathering | $u1_db_comparison | $u1_add_files |"
        echo "| ${test_name}_Update2 | $u2_file_gathering | $u2_db_comparison | $u2_add_files |"
    done

    echo ""
    echo "=========================================="
    echo "Detailed Metrics by Test"
    echo "=========================================="
    echo ""

    for idx in 0 1 2; do
        test_name="${test_names[$idx]}"
        u1_log="${all_update1_logs[$idx]}"
        u2_log="${all_update2_logs[$idx]}"

        echo "----------------------------------------"
        echo "$test_name"
        echo "----------------------------------------"
        echo ""
        echo "UPDATE 1:"
        echo "--------"
        grep "File gathering:" "$u1_log" 2>/dev/null || echo "  File gathering: N/A"
        grep "Database comparison:" "$u1_log" 2>/dev/null || echo "  Database comparison: N/A"
        grep "Add files:" "$u1_log" 2>/dev/null || echo "  Add files: N/A"
        grep "TOTAL TIME:" "$u1_log" 2>/dev/null | tail -1 || echo "  TOTAL TIME: N/A"
        echo ""
        echo "UPDATE 2:"
        echo "--------"
        grep "File gathering:" "$u2_log" 2>/dev/null || echo "  File gathering: N/A"
        grep "Database comparison:" "$u2_log" 2>/dev/null || echo "  Database comparison: N/A"
        grep "Add files:" "$u2_log" 2>/dev/null || echo "  Add files: N/A"
        grep "TOTAL TIME:" "$u2_log" 2>/dev/null | tail -1 || echo "  TOTAL TIME: N/A"
        echo ""
    done

    echo "=========================================="
    echo "Individual log files:"
    echo "=========================================="
    for idx in 0 1 2; do
        test_name="${test_names[$idx]}"
        echo "${test_name}:"
        echo "  Update1: ${all_update1_logs[$idx]}"
        echo "  Update2: ${all_update2_logs[$idx]}"
    done

} | tee "$combined_analysis_report"

print_success "Combined analysis saved to: $combined_analysis_report"
print_info "All log files listed above for detailed review"
