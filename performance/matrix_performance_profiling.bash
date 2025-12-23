#!/bin/bash
set -e

# Analagous to CI/CD matrix testing of Python 3.11, 3.12, 3.13,
# here we will do a matrix performance profiling on multiple parameter combinations.

###############################################################################
# Manually edit parameters here:

work_dir=/lcrc/group/e3sm/ac.forsyth2/zstash_performance/
unique_id=profile_update_20251222

dir_to_copy_from=/lcrc/group/e3sm/ac.forsyth2/E3SMv2/v2.LR.historical_0201/
subdir_many_small_files=build/
subdir_few_large_files=init/
subdir_mix=run/

fresh_globus=true
# ENDPOINT UUIDS:
# LCRC_IMPROV_DTN_ENDPOINT=15288284-7006-4041-ba1a-6b52501e49f1
# NERSC_PERLMUTTER_ENDPOINT=6bdc7956-fc0f-4ad2-989c-7aa5ee643a79
# NERSC_HPSS_ENDPOINT=9cd89cfd-6d04-11e5-ba46-22000b92c6ec
# PIC_COMPY_DTN_ENDPOINT=68fbd2fa-83d7-11e9-8e63-029d279f7e24
# GLOBUS_TUTORIAL_COLLECTION_1_ENDPOINT=6c54cade-bde5-45c1-bdea-f4bd71dba2cc
globus_dst_endpoint_uuid=9cd89cfd-6d04-11e5-ba46-22000b92c6ec
globus_dst_archive_dir=/home/f/forsyth/zstash_performance/

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
    local subdir_many_small_files="${2}"
    local subdir_few_large_files="${3}"
    local subdir_mix="${4}"

    print_step "Validating configuration..."

    if [ ! -d "$dir_to_copy_from" ]; then
        print_error "Source directory does not exist: $dir_to_copy_from"
        exit 1
    fi

    if [ "$subdir_many_small_files" != "none" ] && [ -n "$subdir_many_small_files" ]; then
        if [ ! -d "${dir_to_copy_from}${subdir_many_small_files}" ]; then
            print_error "subdir_many_small_files does not exist: ${dir_to_copy_from}${subdir_many_small_files}"
            exit 1
        fi
    fi

    if [ "$subdir_few_large_files" != "none" ] && [ -n "$subdir_few_large_files" ]; then
        if [ ! -d "${dir_to_copy_from}${subdir_few_large_files}" ]; then
            print_error "subdir_few_large_files does not exist: ${dir_to_copy_from}${subdir_few_large_files}"
            exit 1
        fi
    fi

    if [ "$subdir_mix" != "none" ] && [ -n "$subdir_mix" ]; then
        if [ ! -d "${dir_to_copy_from}${subdir_mix}" ]; then
            print_error "subdir_mix does not exist: ${dir_to_copy_from}${subdir_mix}"
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
}

run_create()
{
    local dir_to_copy_from="${1}"
    local subdir="${2}"
    local archive_dir="${3}"
    local hpss_value="${4}"
    local cache_dir="${5}"
    local create_log="${6}"
    print_step "Starting CREATE operation..."

    print_info "Copying data from ${dir_to_copy_from}${subdir}"
    cp -r "${dir_to_copy_from}${subdir}" "${archive_dir}${subdir}"

    print_info "Running zstash create..."
    print_info "Command: zstash create --hpss=${hpss_value} --cache=${cache_dir} -v ${archive_dir}"

    if { time zstash create --hpss="${hpss_value}" --cache="${cache_dir}" -v "${archive_dir}" ; } 2>&1 | tee "${create_log}"; then
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
    local hpss_value="${4}"
    local cache_dir="${5}"
    local update_log="${6}"

    print_step "Starting UPDATE operation..."

    print_info "Copying additional data from ${dir_to_copy_from}${subdir}"
    cp -r "${dir_to_copy_from}${subdir}" "${archive_dir}${subdir}"

    print_info "Running zstash update..."
    print_info "Command: zstash update --hpss=${hpss_value} --cache=${cache_dir} -v"

    if { time zstash update --hpss="${hpss_value}" --cache="${cache_dir}" -v ; } 2>&1 | tee "${update_log}"; then
        print_success "zstash update completed successfully"
    else
        print_error "zstash update failed with exit code $?"
        exit 1
    fi
}

# Function to extract performance metrics from log file
extract_performance_metrics() {
    local log_file="${1}"
    local metric_name="${2}"

    # Extract time (in seconds)
    local time_value=$(grep "TIME PROFILE -- ${metric_name}:" "$log_file" | grep -oP '\d+\.\d+(?= seconds)')

    # Extract memory peak (in MB)
    local memory_value=$(grep "MEMORY PROFILE -- ${metric_name}:" "$log_file" | grep -oP 'peak=\K\d+\.\d+')

    echo "${time_value}s / ${memory_value}MB"
}

# Function to extract total time from log file
extract_total_time() {
    local log_file="${1}"

    # Look for the "real" time from the bash `time` command output
    local total_time=$(grep "^real" "$log_file" | awk '{print $2}')

    # If not found in that format, try alternative format
    if [ -z "$total_time" ]; then
        total_time=$(grep -oP 'real\s+\K[\d\.]+m[\d\.]+s' "$log_file")
    fi

    echo "${total_time}"
}

###############################################################################
# Main script:

valiate_configuration $dir_to_copy_from $subdir_many_small_files $subdir_few_large_files $subdir_mix

if [ "${fresh_globus}" == "true" ]; then
    refresh_globus
fi

analyze_directories "${dir_to_copy_from}${subdir_many_small_files}" "${dir_to_copy_from}${subdir_few_large_files}" "${dir_to_copy_from}${subdir_mix}"

subdirs=(
  "$subdir_many_small_files"
  "$subdir_few_large_files"
  "$subdir_mix"
)
hpss_cases=(
  "none"
  "globus"
)

# Create results file
results_file="${work_dir}${unique_id}/performance_results.md"
mkdir -p "${work_dir}${unique_id}"

# Write markdown table header
cat > "$results_file" << 'EOF'
# Performance Profiling Results

## Test Configuration
EOF

echo "- **Work Directory**: ${work_dir}${unique_id}" >> "$results_file"
echo "- **Source Directory**: ${dir_to_copy_from}" >> "$results_file"
echo "- **Many Small Files**: ${subdir_many_small_files}" >> "$results_file"
echo "- **Few Large Files**: ${subdir_few_large_files}" >> "$results_file"
echo "- **Mix**: ${subdir_mix}" >> "$results_file"
echo "" >> "$results_file"

cat >> "$results_file" << 'EOF'
## Performance Metrics

| Test Case | Create Dir | Update Dir | HPSS | File Gathering (Time/Memory) | Database Comparison (Time/Memory) | Add Files (Time/Memory) | Total Time |
|-----------|------------|------------|------|------------------------------|-----------------------------------|-------------------------|------------|
EOF

# Iterate through length of subdirs
for (( i=0; i<${#subdirs[@]}; i++ )); do
    subdir_create=${subdirs[i]}

    # Iterate through length of subdirs
    for (( j=0; j<${#subdirs[@]}; j++ )); do
        # Skip the same subdir so we only get the other ones
        if (( j == i )); then
             continue
        fi
        subdir_update=${subdirs[j]}

        # Iterate through length of hpss_cases
        for (( k=0; k<${#hpss_cases[@]}; k++ )); do
            hpss_case=${hpss_cases[k]}
            test_label="test_create${i}_update${j}_hpss${k}"

            echo "create dir: $subdir_create"
            echo "update dir: $subdir_update"
            echo "hpss value: $hpss_case"
            echo "test label: $test_label"

            # Create unique work directories for this test
            if [[ "$hpss_case" == "globus" ]]; then
                globus_dst_archive_subdir="${globus_dst_archive_dir}${unique_id}/${test_label}/"
                hpss_value="globus://${globus_dst_endpoint_uuid}/${globus_dst_archive_subdir}"
            else
                hpss_value="none"
            fi
            work_subdir="${work_dir}${unique_id}/${test_label}/"
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

            cd "${work_subdir}"
            run_create "$dir_to_copy_from" "$subdir_create" "$archive_dir" "$hpss_value" "$cache_dir" "$create_log"
            # For update, we need to be in archive_dir:
            cd "${archive_dir}"
            run_update "$dir_to_copy_from" "$subdir_update" "$archive_dir" "$hpss_value" "$cache_dir" "$update_log"
            cd "${work_subdir}"

            # Extract performance metrics
            file_gather=$(extract_performance_metrics "$update_log" "FILE GATHERING")
            db_compare=$(extract_performance_metrics "$update_log" "DATABASE COMPARISON")
            add_files=$(extract_performance_metrics "$update_log" "ADD FILES")
            total_time=$(extract_total_time "$update_log")

            # Write row to results file
            echo "| $test_label | $subdir_create | $subdir_update | $hpss_case | $file_gather | $db_compare | $add_files | $total_time |" >> "$results_file"
        done
    done
done

print_success "Performance profiling complete!"
print_info "Results saved to: $results_file"
echo ""
echo "========================================"
echo "RESULTS PREVIEW"
echo "========================================"
cat "$results_file"
