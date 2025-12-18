#!/bin/bash
set -e

###############################################################################
# Manually edit parameters here:

work_dir=/lcrc/group/e3sm/ac.forsyth2/zstash_performance/
unique_id=profile_update_20251218

dir_to_copy_from=/lcrc/group/e3sm/ac.forsyth2/E3SMv2/v2.LR.historical_0201/
subdir_for_create=build/ # Use "none" to skip create
subdir_for_update=init/ # Use "none" to skip update

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
# Colored messages

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

###############################################################################
# Utility functions

confirm() {
    read -p "$1 (y/n): " -n 1 -r
    echo
    [[ $REPLY =~ ^[Yy]$ ]]
}

###############################################################################
# Validation

print_step "Validating configuration..."

if [ ! -d "$dir_to_copy_from" ]; then
    print_error "Source directory does not exist: $dir_to_copy_from"
    exit 1
fi

if [ "$subdir_for_create" != "none" ] && [ -n "$subdir_for_create" ]; then
    if [ ! -d "${dir_to_copy_from}${subdir_for_create}" ]; then
        print_error "Create subdirectory does not exist: ${dir_to_copy_from}${subdir_for_create}"
        exit 1
    fi
fi

if [ "$subdir_for_update" != "none" ] && [ -n "$subdir_for_update" ]; then
    if [ ! -d "${dir_to_copy_from}${subdir_for_update}" ]; then
        print_error "Update subdirectory does not exist: ${dir_to_copy_from}${subdir_for_update}"
        exit 1
    fi
fi

print_success "Configuration validated"

###############################################################################
# Main script:

if [ "${fresh_globus}" == "true" ]; then
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
fi

print_step "Creating work directories..."

work_subdir=${work_dir}${unique_id}/
mkdir -p "${work_subdir}"

archive_dir=${work_subdir}archive_dir/
cache_dir=${work_subdir}cache/
log_dir=${work_subdir}logs/
mkdir -p "${archive_dir}"
mkdir -p "${cache_dir}"
mkdir -p "${log_dir}"

# Define log file paths early so they're always available
create_log=${log_dir}create.log
update_log=${log_dir}update.log

cd "${work_subdir}"
dst_archive_subdir=${dst_archive_dir}${unique_id}/

print_success "Work directories created at ${work_subdir}"

###############################################################################
# CREATE operation

if [ -n "$subdir_for_create" ] && [ "$subdir_for_create" != "none" ]; then
    print_step "Starting CREATE operation..."

    print_info "Copying data from ${dir_to_copy_from}${subdir_for_create}"
    cp -r "${dir_to_copy_from}${subdir_for_create}" "${archive_dir}${subdir_for_create}"

    echo ""
    print_info "Archive directory size:"
    time du -sh "${archive_dir}"
    echo ""

    print_info "Running zstash create..."
    print_info "Command: zstash create --hpss=globus://${dst_endpoint_uuid}/${dst_archive_subdir} --cache=${cache_dir} -v ${archive_dir}"

    if { time zstash create --hpss="globus://${dst_endpoint_uuid}/${dst_archive_subdir}" --cache="${cache_dir}" -v "${archive_dir}" ; } 2>&1 | tee "${create_log}"; then
        print_success "zstash create completed successfully"
    else
        print_error "zstash create failed with exit code $?"
        exit 1
    fi

    echo ""
    print_warning "Verification required:"
    echo "Go to your destination machine and run:"
    echo "  ls ${dst_archive_subdir}"
    echo "Expected output: 000000.tar   index.db"
    echo ""

    if ! confirm "Is the ls result correct?"; then
        print_error "Verification failed"
        exit 1
    fi

    print_success "CREATE operation verified"
else
    print_info "Skipping CREATE operation (subdir_for_create is 'none' or empty)"
fi

###############################################################################
# UPDATE operation

if [ -n "$subdir_for_update" ] && [ "$subdir_for_update" != "none" ]; then
    print_step "Starting UPDATE operation..."

    print_info "Copying additional data from ${dir_to_copy_from}${subdir_for_update}"
    cp -r "${dir_to_copy_from}${subdir_for_update}" "${archive_dir}${subdir_for_update}"

    echo ""
    print_info "Size of newly added data:"
    time du -sh "${dir_to_copy_from}${subdir_for_update}"
    echo ""

    # For update, we need to be in archive_dir
    cd "${archive_dir}"

    print_info "Running zstash update..."
    print_info "Command: zstash update --hpss=globus://${dst_endpoint_uuid}/${dst_archive_subdir} --cache=${cache_dir} -v"

    if { time zstash update --hpss="globus://${dst_endpoint_uuid}/${dst_archive_subdir}" --cache="${cache_dir}" -v ; } 2>&1 | tee "${update_log}"; then
        print_success "zstash update completed successfully"
    else
        print_error "zstash update failed with exit code $?"
        exit 1
    fi

    echo ""
    print_warning "Verification required:"
    echo "Go to your destination machine and run:"
    echo "  ls ${dst_archive_subdir}"
    echo "Expected output: 000000.tar   000001.tar   index.db"
    echo "(May show more tars if you have run update multiple times)"
    echo ""
    if ! confirm "Is the ls result correct?"; then
        print_error "Verification failed"
        exit 1
    fi

    print_success "UPDATE operation verified"
else
    print_info "Skipping UPDATE operation (subdir_for_update is 'none' or empty)"
fi

###############################################################################
# Performance Analysis

cd "${work_subdir}"

if [ -f "${update_log}" ] && [ -s "${update_log}" ]; then
    print_step "Analyzing performance metrics from update log..."

    echo ""
    print_info "=========================================="
    print_info "Performance Analysis (OPTIMIZED VERSION)"
    print_info "=========================================="

    # File gathering metrics (OPTIMIZED - now with stats)
    if grep -q "PERFORMANCE (get_files_to_archive_with_stats)" "$update_log" 2>/dev/null; then
        echo ""
        print_info "File Gathering Breakdown (with stats collection):"
        grep "PERFORMANCE (scandir):" "$update_log" | tail -5
        grep "PERFORMANCE (sort):" "$update_log"
        grep "PERFORMANCE (get_files_to_archive_with_stats): TOTAL TIME:" "$update_log"
    fi

    # Database comparison metrics (OPTIMIZED - no stats)
    if grep -q "PERFORMANCE: Database comparison completed" "$update_log" 2>/dev/null; then
        echo ""
        print_info "Database Comparison Breakdown (OPTIMIZED):"
        grep "PERFORMANCE: Total comparison time:" "$update_log"
        grep "PERFORMANCE: Files checked:" "$update_log"
        grep "PERFORMANCE: New files to archive:" "$update_log"
        grep "PERFORMANCE: Average rate:" "$update_log"
        grep "database load:" "$update_log"
        grep "comparison (in-memory):" "$update_log"
    fi

    # Optimization impact
    if grep -q "PERFORMANCE: Optimization impact:" "$update_log" 2>/dev/null; then
        echo ""
        print_info "Optimization Impact:"
        grep "stat operations eliminated:" "$update_log"
        grep "All stats performed during initial filesystem walk" "$update_log"
    fi

    # Overall summary
    if grep -q "PERFORMANCE: Update complete - Summary:" "$update_log" 2>/dev/null; then
        echo ""
        print_info "Overall Time Breakdown:"
        grep "PERFORMANCE: Update complete - Summary:" -A 6 "$update_log" | tail -6
    fi

    echo ""
    print_info "=========================================="
    print_info "Analysis Commands"
    print_info "=========================================="
    print_success "View full logs:"
    echo "  cat $log_dir/create.log       # Create profiling"
    echo "  cat $log_dir/update.log       # Update profiling"
    echo ""
    print_success "Extract specific metrics:"
    echo "  grep 'PERFORMANCE' $update_log | less"
    echo "  grep 'PERFORMANCE (scandir)' $update_log"
    echo "  grep 'database load' $update_log"
    echo "  grep 'Optimization impact' $update_log"
    echo ""
    print_success "Compare times:"
    echo "  grep 'TOTAL TIME' $update_log"
    echo ""
    print_success "View optimization benefits:"
    echo "  grep 'stat operations eliminated' $update_log"
    echo ""
else
    print_warning "No update log found or log is empty - skipping performance analysis"
fi

echo ""
print_info "=========================================="
print_success "Profiling Complete!"
print_info "=========================================="
print_info "Working directory: ${work_subdir}"
print_info "Logs available at: ${log_dir}"
echo ""
