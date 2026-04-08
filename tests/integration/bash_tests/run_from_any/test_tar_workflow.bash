#!/bin/bash

# Test script for zstash selective tar retrieval workflows
# Tests Method 1 (ls --tars) and Method 2 (check --keep)

# Assertions ##################################################################
check_log_has()
{
    local expected_grep="${1}"
    local log_file="${2}"
    grep -q "${expected_grep}" ${log_file}
    if [ $? != 0 ]; then
        echo "Expected grep '${expected_grep}' not found in $(realpath ${log_file}). Test failed."
        return 2
    fi
    return 0
}

check_log_does_not_have()
{
    local not_expected_grep="${1}"
    local log_file="${2}"
    grep -q "${not_expected_grep}" ${log_file}
    if [ $? == 0 ]; then
        echo "Not-expected grep '${not_expected_grep}' was found in $(realpath ${log_file}). Test failed."
        return 2
    fi
    return 0
}

check_file_exists()
{
    local file_path="${1}"
    if [ ! -f "${file_path}" ]; then
        echo "Expected file '${file_path}' not found. Test failed."
        return 2
    fi
    return 0
}

check_file_does_not_exist()
{
    local file_path="${1}"
    if [ -f "${file_path}" ]; then
        echo "Unexpected file '${file_path}' was found. Test failed."
        return 2
    fi
    return 0
}

# Helper functions ############################################################
setup()
{
    echo "##########################################################################################################"
    local case_name="${1}"
    local src_dir="${2}"
    echo "Testing: ${case_name}"
    full_dir="${src_dir}/${case_name}"
    rm -rf ${full_dir}
    mkdir -p ${full_dir}
    cd ${full_dir}

    # Create test data structure mimicking E3SM output
    mkdir -p test_archive/archive/lnd/hist
    mkdir -p test_archive/archive/ocean/hist
    mkdir -p test_archive/archive/atm/hist

    echo 'Land history file 1' > test_archive/archive/lnd/hist/file1.elm.h0.0001-01.nc
    echo 'Land history file 2' > test_archive/archive/lnd/hist/file2.elm.h0.0001-02.nc
    echo 'Ocean timeseries 1' > test_archive/archive/ocean/hist/file1.timeSeriesMonthly.0001-01.nc
    echo 'Ocean timeseries 2' > test_archive/archive/ocean/hist/file2.timeSeriesMonthly.0001-02.nc
    echo 'Atmosphere file 1' > test_archive/archive/atm/hist/file1.cam.h0.0001-01.nc
    echo 'Atmosphere file 2' > test_archive/archive/atm/hist/file2.cam.h0.0001-02.nc
}

get_endpoint()
{
    local endpoint_name=$1
    # Define endpoints; see https://app.globus.org/collections
    LCRC_IMPROV_DTN_ENDPOINT=15288284-7006-4041-ba1a-6b52501e49f1
    NERSC_PERLMUTTER_ENDPOINT=6bdc7956-fc0f-4ad2-989c-7aa5ee643a79
    NERSC_HPSS_ENDPOINT=9cd89cfd-6d04-11e5-ba46-22000b92c6ec
    PIC_COMPY_DTN_ENDPOINT=68fbd2fa-83d7-11e9-8e63-029d279f7e24
    GLOBUS_TUTORIAL_COLLECTION_1_ENDPOINT=6c54cade-bde5-45c1-bdea-f4bd71dba2cc
    case ${endpoint_name} in
        LCRC_IMPROV_DTN_ENDPOINT)
            echo ${LCRC_IMPROV_DTN_ENDPOINT}
            ;;
        NERSC_PERLMUTTER_ENDPOINT)
            echo ${NERSC_PERLMUTTER_ENDPOINT}
            ;;
        NERSC_HPSS_ENDPOINT)
            echo ${NERSC_HPSS_ENDPOINT}
            ;;
        PIC_COMPY_DTN_ENDPOINT)
            echo ${PIC_COMPY_DTN_ENDPOINT}
            ;;
        GLOBUS_TUTORIAL_COLLECTION_1_ENDPOINT)
            echo ${GLOBUS_TUTORIAL_COLLECTION_1_ENDPOINT}
            ;;
        *)
            echo "Unknown endpoint name: ${endpoint_name}" >&2
            exit 1
            ;;
    esac
}

confirm() {
    read -p "$1 (y/n): " -n 1 -r
    echo
    [[ $REPLY =~ ^[Yy]$ ]]
}

wait_for_directory() {
    local dir_path="$1"
    local max_wait=300  # 5 minutes
    local waited=0

    echo "Waiting for directory ${dir_path} to be created by Globus transfer..."
    while [ ! -d "${dir_path}" ] && [ ${waited} -lt ${max_wait} ]; do
        sleep 5
        waited=$((waited + 5))
        echo "  Waited ${waited}s..."
    done

    if [ -d "${dir_path}" ]; then
        echo "Directory appeared after ${waited}s"
        return 0
    else
        echo "Directory did not appear after ${max_wait}s"
        return 1
    fi
}

# Tests #######################################################################

test_method1_ls_tars()
{
    # Method 1: Use 'zstash ls --tars' to identify needed tars
    # Then manual transfer would happen (simulated here)

    local path_to_repo=$1
    local dst_endpoint=$2
    local dst_dir=$3

    src_dir=${path_to_repo}/tests/utils/selective_tar_retrieval
    rm -rf ${src_dir}
    mkdir -p ${src_dir}
    dst_endpoint_uuid=$(get_endpoint ${dst_endpoint})
    globus_path=globus://${dst_endpoint_uuid}/${dst_dir}

    case_name="method1_ls_tars"
    echo "Running test_method1_ls_tars on case=${case_name}"
    echo "Exit codes: 0 -- success, 1 -- zstash failed, 2 -- grep check failed"

    setup ${case_name} "${src_dir}"

    # Create the archive on Globus
    echo "Step 1: Creating archive with zstash..."
    zstash create --hpss=${globus_path}/${case_name} test_archive 2>&1 | tee ${case_name}_create.log
    if [ $? != 0 ]; then
        echo "${case_name} create failed. Check ${case_name}_create.log for details."
        return 1
    fi
    check_log_has "Creating new tar archive" ${case_name}_create.log || return 2

    # Wait for Globus transfer to complete
    wait_for_directory "${dst_dir}/${case_name}" || return 1

    # Set up a separate directory for ls testing
    mkdir -p method1_test
    cd method1_test

    # Step 2: Use ls --tars to identify tars containing land files
    echo ""
    echo "Step 2a: Listing tars containing land files..."
    zstash ls --hpss=${globus_path}/${case_name} --tars "archive/lnd/hist/*elm.h0*" 2>&1 | tee land_tars.log
    if [ $? != 0 ]; then
        echo "${case_name} ls land failed."
        return 1
    fi
    check_log_has "Tars:" land_tars.log || return 2
    check_log_has ".tar" land_tars.log || return 2
    check_log_has "elm.h0" land_tars.log || return 2

    # Step 3: Use ls --tars to identify tars containing ocean files
    echo ""
    echo "Step 2b: Listing tars containing ocean files..."
    zstash ls --hpss=${globus_path}/${case_name} --tars "archive/ocean/hist/*timeSeriesMonthly*" 2>&1 | tee ocean_tars.log
    if [ $? != 0 ]; then
        echo "${case_name} ls ocean failed."
        return 1
    fi
    check_log_has "Tars:" ocean_tars.log || return 2
    check_log_has ".tar" ocean_tars.log || return 2
    check_log_has "timeSeriesMonthly" ocean_tars.log || return 2

    echo ""
    echo "Step 3: In real workflow, user would manually transfer identified tars"
    echo "For testing purposes, we verify that ls correctly identified the files and tars"

    cd ..

    echo "${case_name} completed successfully."
    return 0
}

test_method1_ls_from_cache_with_keep()
{
    # Method 1 with --keep: Test using 'zstash ls --hpss=none' from a local cache
    # When archive is created with --keep, tars remain in local cache
    # and ls operations work without HPSS access

    local path_to_repo=$1
    local dst_endpoint=$2
    local dst_dir=$3

    src_dir=${path_to_repo}/tests/utils/selective_tar_retrieval
    rm -rf ${src_dir}
    mkdir -p ${src_dir}
    dst_endpoint_uuid=$(get_endpoint ${dst_endpoint})
    globus_path=globus://${dst_endpoint_uuid}/${dst_dir}

    case_name="method1_ls_from_cache_with_keep"
    echo "Running test_method1_ls_from_cache_with_keep on case=${case_name}"
    echo "Exit codes: 0 -- success, 1 -- zstash failed, 2 -- grep check failed"

    setup ${case_name} "${src_dir}"

    # Create the archive on Globus WITH --keep (tars will remain locally)
    echo "Step 1: Creating archive with zstash --keep..."
    zstash create --hpss=${globus_path}/${case_name} --keep test_archive 2>&1 | tee ${case_name}_create.log
    if [ $? != 0 ]; then
        echo "${case_name} create failed. Check ${case_name}_create.log for details."
        return 1
    fi
    check_log_has "Creating new tar archive" ${case_name}_create.log || return 2

    # Wait for Globus transfer to complete
    wait_for_directory "${dst_dir}/${case_name}" || return 1

    # Verify local cache has both index.db AND tars
    echo ""
    echo "Step 2: Verifying local cache has both index.db and tars (expected with --keep)..."
    ls test_archive/zstash/ 2>&1 | tee source_cache_ls.log
    check_log_has "index.db" source_cache_ls.log || return 2
    check_log_has "000000.tar" source_cache_ls.log || return 2
    echo "SUCCESS: Both index.db and tars present in cache"

    # Step 3: Copy cache to simulate manual transfer
    echo ""
    echo "Step 3: Copying complete cache to different location..."
    mkdir -p different_location
    cp -r test_archive/zstash different_location/

    ls different_location/zstash/ 2>&1 | tee copied_cache_ls.log
    check_log_has "index.db" copied_cache_ls.log || return 2
    check_log_has "000000.tar" copied_cache_ls.log || return 2

    # Step 4: Use zstash ls with --hpss=none and --cache
    echo ""
    echo "Step 4a: Listing contents using --cache parameter..."
    zstash ls --hpss=none --cache=different_location/zstash 2>&1 | tee ls_with_cache_param.log
    if [ $? != 0 ]; then
        echo "${case_name} ls with --cache failed."
        return 1
    fi
    check_log_has "elm.h0" ls_with_cache_param.log || return 2
    check_log_has "timeSeriesMonthly" ls_with_cache_param.log || return 2
    check_log_has "cam.h0" ls_with_cache_param.log || return 2

    # Step 5: Test ls from within the cache directory
    echo ""
    echo "Step 4b: Listing contents from cache directory itself..."
    cd different_location
    zstash ls --hpss=none 2>&1 | tee ../ls_from_cache_dir.log
    if [ $? != 0 ]; then
        echo "${case_name} ls from cache directory failed."
        cd ..
        return 1
    fi
    cd ..
    check_log_has "elm.h0" ls_from_cache_dir.log || return 2
    check_log_has "timeSeriesMonthly" ls_from_cache_dir.log || return 2
    check_log_has "cam.h0" ls_from_cache_dir.log || return 2

    # Step 6: Test ls with file patterns
    echo ""
    echo "Step 5: Listing with file patterns..."
    zstash ls --hpss=none --cache=different_location/zstash "archive/lnd/hist/*elm.h0*" 2>&1 | tee ls_pattern.log
    if [ $? != 0 ]; then
        echo "${case_name} ls with pattern failed."
        return 1
    fi
    check_log_has "elm.h0" ls_pattern.log || return 2
    check_log_does_not_have "timeSeriesMonthly" ls_pattern.log || return 2
    check_log_does_not_have "cam.h0" ls_pattern.log || return 2

    # Step 7: Test ls with --tars
    echo ""
    echo "Step 6: Listing with --tars to show which tar contains files..."
    zstash ls --hpss=none --cache=different_location/zstash --tars "archive/ocean/hist/*timeSeriesMonthly*" 2>&1 | tee ls_tars.log
    if [ $? != 0 ]; then
        echo "${case_name} ls --tars failed."
        return 1
    fi
    check_log_has "timeSeriesMonthly" ls_tars.log || return 2
    check_log_has "Tars:" ls_tars.log || return 2
    check_log_has "000000.tar" ls_tars.log || return 2

    echo ""
    echo "Step 7: Verifying no HPSS/Globus access was needed for ls operations..."
    check_log_does_not_have "Transferring file from HPSS" ls_with_cache_param.log || return 2
    check_log_does_not_have "globus_transfer" ls_with_cache_param.log || return 2

    echo "${case_name} completed successfully."
    return 0
}

test_method1_ls_from_cache_without_keep()
{
    # Method 1 without --keep: Verify expected failure when tars are missing
    # When archive is created without --keep (default), tars are deleted after transfer
    # ls operations fail because index.db alone is insufficient

    local path_to_repo=$1
    local dst_endpoint=$2
    local dst_dir=$3

    src_dir=${path_to_repo}/tests/utils/selective_tar_retrieval
    rm -rf ${src_dir}
    mkdir -p ${src_dir}
    dst_endpoint_uuid=$(get_endpoint ${dst_endpoint})
    globus_path=globus://${dst_endpoint_uuid}/${dst_dir}

    case_name="method1_ls_from_cache_without_keep"
    echo "Running test_method1_ls_from_cache_without_keep on case=${case_name}"
    echo "Exit codes: 0 -- success, 1 -- zstash failed, 2 -- grep check failed"

    setup ${case_name} "${src_dir}"

    # Create the archive on Globus WITHOUT --keep (default - tars will be deleted)
    echo "Step 1: Creating archive with zstash (without --keep - default behavior)..."
    zstash create --hpss=${globus_path}/${case_name} test_archive 2>&1 | tee ${case_name}_create.log
    if [ $? != 0 ]; then
        echo "${case_name} create failed. Check ${case_name}_create.log for details."
        return 1
    fi
    check_log_has "Creating new tar archive" ${case_name}_create.log || return 2

    # Wait for Globus transfer to complete
    wait_for_directory "${dst_dir}/${case_name}" || return 1

    # Verify local cache has ONLY index.db (tars were deleted after transfer)
    echo ""
    echo "Step 2: Verifying local cache has only index.db, no tars (expected without --keep)..."
    ls test_archive/zstash/ 2>&1 | tee source_cache_ls.log
    check_log_has "index.db" source_cache_ls.log || return 2
    check_log_does_not_have "000000.tar" source_cache_ls.log || return 2
    echo "SUCCESS: Only index.db present, tars were deleted as expected"

    # Step 3: Try to use zstash ls from wrong directory - should fail
    echo ""
    echo "Step 3: Attempting zstash ls --hpss=none from wrong directory (no zstash cache)..."
    mkdir -p wrong_directory
    cd wrong_directory
    # Use set +e and PIPESTATUS to capture the exit code from zstash, not tee
    set +e
    zstash ls --hpss=none 2>&1 | tee ../ls_no_cache.log
    ls_exit_code_1=${PIPESTATUS[0]}
    set -e
    cd ..

    # We EXPECT this to fail (non-zero exit code)
    if [ ${ls_exit_code_1} -eq 0 ]; then
        echo "ERROR: zstash ls should have failed from wrong directory but succeeded"
        return 2
    fi
    echo "SUCCESS: zstash ls failed as expected (exit code: ${ls_exit_code_1})"
    check_log_has "unable to open database file" ls_no_cache.log || return 2
    echo "SUCCESS: Got expected 'unable to open database file' error from wrong directory"

    # Step 4: Try with --cache pointing to incomplete cache (only index.db, no tars)
    # This tests if zstash ls works with ONLY index.db (without tar files)
    echo ""
    echo "Step 4: Attempting zstash ls --hpss=none --cache with incomplete cache (only index.db)..."
    set +e
    zstash ls --hpss=none --cache=test_archive/zstash 2>&1 | tee ls_incomplete_cache.log
    ls_exit_code_2=${PIPESTATUS[0]}
    set -e

    if [ ${ls_exit_code_2} -eq 0 ]; then
        echo "SUCCESS: zstash ls works with only index.db (tars not required for listing)"
        check_log_has "elm.h0" ls_incomplete_cache.log || return 2
        check_log_has "timeSeriesMonthly" ls_incomplete_cache.log || return 2
        check_log_has "cam.h0" ls_incomplete_cache.log || return 2
    else
        echo "NOTE: zstash ls failed with only index.db (exit code: ${ls_exit_code_2})"
        echo "This documents that tars ARE required for ls operations"
    fi

    echo "${case_name} completed successfully."
    return 0
}

test_method1_ls_with_cache_dot()
{
    # Test using 'zstash ls --hpss=none --cache=.' from within the cache directory itself
    # This clarifies that when you're IN the zstash cache directory, you must use --cache=.

    local path_to_repo=$1
    local dst_endpoint=$2
    local dst_dir=$3

    src_dir=${path_to_repo}/tests/utils/selective_tar_retrieval
    rm -rf ${src_dir}
    mkdir -p ${src_dir}
    dst_endpoint_uuid=$(get_endpoint ${dst_endpoint})
    globus_path=globus://${dst_endpoint_uuid}/${dst_dir}

    case_name="method1_ls_with_cache_dot"
    echo "Running test_method1_ls_with_cache_dot on case=${case_name}"
    echo "Exit codes: 0 -- success, 1 -- zstash failed, 2 -- grep check failed"

    setup ${case_name} "${src_dir}"

    # Create the archive with --keep so tars remain locally
    echo "Step 1: Creating archive with zstash --keep..."
    zstash create --hpss=${globus_path}/${case_name} --keep test_archive 2>&1 | tee ${case_name}_create.log
    if [ $? != 0 ]; then
        echo "${case_name} create failed. Check ${case_name}_create.log for details."
        return 1
    fi
    check_log_has "Creating new tar archive" ${case_name}_create.log || return 2

    # Wait for Globus transfer to complete
    wait_for_directory "${dst_dir}/${case_name}" || return 1

    # Verify cache has both index.db and tars
    echo ""
    echo "Step 2: Verifying cache contents..."
    ls test_archive/zstash/ 2>&1 | tee cache_ls.log
    check_log_has "index.db" cache_ls.log || return 2
    check_log_has "000000.tar" cache_ls.log || return 2

    # Step 3: Try zstash ls from within the cache directory WITHOUT --cache=. (should fail)
    echo ""
    echo "Step 3: Attempting zstash ls from WITHIN cache directory without --cache=. (expect failure)..."
    cd test_archive/zstash
    set +e
    zstash ls --hpss=none 2>&1 | tee ../../ls_without_cache_dot.log
    ls_exit_code_1=${PIPESTATUS[0]}
    set -e

    # We EXPECT this to fail because it's looking for ./zstash/index.db which doesn't exist
    if [ ${ls_exit_code_1} -eq 0 ]; then
        echo "ERROR: zstash ls should have failed without --cache=."
        cd ../..
        return 2
    fi
    echo "SUCCESS: zstash ls failed as expected (exit code: ${ls_exit_code_1})"
    cd ../..
    check_log_has "unable to open database file" ls_without_cache_dot.log || return 2
    echo "EXPECTED BEHAVIOR: When in the cache directory itself, zstash still looks for ./zstash/index.db by default"

    # Step 4: Now use --cache=. from within the cache directory (should succeed)
    echo ""
    echo "Step 4: Using zstash ls --hpss=none --cache=. from within cache directory (should succeed)..."
    cd test_archive/zstash
    zstash ls --hpss=none --cache=. 2>&1 | tee ../../ls_with_cache_dot.log
    ls_exit_code_2=$?
    cd ../..

    if [ ${ls_exit_code_2} -ne 0 ]; then
        echo "ERROR: zstash ls --cache=. failed"
        return 1
    fi
    echo "SUCCESS: zstash ls --cache=. works from within the cache directory"
    check_log_has "elm.h0" ls_with_cache_dot.log || return 2
    check_log_has "timeSeriesMonthly" ls_with_cache_dot.log || return 2
    check_log_has "cam.h0" ls_with_cache_dot.log || return 2

    # Step 5: Demonstrate the different behaviors with clear examples
    echo ""
    echo "Step 5: Summary of --cache behavior:"
    echo "  - Default behavior: zstash looks for ./zstash/index.db"
    echo "  - When IN the cache dir: must use --cache=. to specify current directory"
    echo "  - When in parent dir: can use --cache=zstash or just rely on default"

    # Test from parent directory without --cache (uses default)
    cd test_archive
    zstash ls --hpss=none 2>&1 | tee ../ls_from_parent_default.log
    if [ $? -ne 0 ]; then
        echo "ERROR: zstash ls from parent directory failed"
        cd ..
        return 1
    fi
    cd ..
    echo "  - Confirmed: zstash ls from parent directory works with default cache"

    # Test from parent directory with explicit --cache=zstash
    cd test_archive
    zstash ls --hpss=none --cache=zstash 2>&1 | tee ../ls_from_parent_explicit.log
    if [ $? -ne 0 ]; then
        echo "ERROR: zstash ls --cache=zstash from parent directory failed"
        cd ..
        return 1
    fi
    cd ..
    echo "  - Confirmed: zstash ls --cache=zstash from parent directory also works"

    echo "${case_name} completed successfully."
    return 0
}

test_method2_check_keep()
{
    # Method 2: Use 'zstash check --keep' to download tars without extraction

    local path_to_repo=$1
    local dst_endpoint=$2
    local dst_dir=$3

    src_dir=${path_to_repo}/tests/utils/selective_tar_retrieval
    rm -rf ${src_dir}
    mkdir -p ${src_dir}
    dst_endpoint_uuid=$(get_endpoint ${dst_endpoint})
    globus_path=globus://${dst_endpoint_uuid}/${dst_dir}

    case_name="method2_check_keep"
    echo "Running test_method2_check_keep on case=${case_name}"
    echo "Exit codes: 0 -- success, 1 -- zstash failed, 2 -- grep check failed"

    setup ${case_name} "${src_dir}"

    # Create the archive on Globus
    echo "Step 1: Creating archive with zstash..."
    zstash create --hpss=${globus_path}/${case_name} test_archive 2>&1 | tee ${case_name}_create.log
    if [ $? != 0 ]; then
        echo "${case_name} create failed. Check ${case_name}_create.log for details."
        return 1
    fi
    check_log_has "Creating new tar archive" ${case_name}_create.log || return 2

    # Wait for Globus transfer to complete
    wait_for_directory "${dst_dir}/${case_name}" || return 1

    # Set up a separate directory for check testing
    mkdir -p method2_test
    cd method2_test

    # Step 2: Use check --keep to download tars with land files (without extraction)
    echo ""
    echo "Step 2: Downloading tars with land files using check --keep..."
    zstash check --hpss=${globus_path}/${case_name} --keep "archive/lnd/hist/*elm.h0*" 2>&1 | tee land_check.log
    if [ $? != 0 ]; then
        echo "${case_name} check land failed."
        return 1
    fi
    check_log_has "Opening tar archive" land_check.log || return 2
    check_log_has "elm.h0" land_check.log || return 2

    # Verify tars are in cache
    echo ""
    echo "Verifying tars were downloaded to cache..."
    ls -lh zstash/*.tar 2>&1 | tee cache_after_land.log
    check_log_has ".tar" cache_after_land.log || return 2

    # Verify files were NOT extracted to working directory
    echo ""
    echo "Verifying files were NOT extracted (only tars downloaded)..."
    if [ -d "archive/lnd/hist" ]; then
        echo "ERROR: Files were extracted when they shouldn't have been!"
        return 2
    fi

    # Step 3: Use check --keep to download tars with ocean files
    echo ""
    echo "Step 3: Downloading tars with ocean files using check --keep..."
    zstash check --hpss=${globus_path}/${case_name} --keep "archive/ocean/hist/*timeSeriesMonthly*" 2>&1 | tee ocean_check.log
    if [ $? != 0 ]; then
        echo "${case_name} check ocean failed."
        return 1
    fi
    check_log_has "Opening tar archive" ocean_check.log || return 2
    check_log_has "timeSeriesMonthly" ocean_check.log || return 2

    # Step 4: Now extract land files to verify tars are usable
    echo ""
    echo "Step 4a: Extracting land files from cached tars..."
    mkdir -p land_dest
    cd land_dest
    zstash extract --hpss=none --cache=../zstash "archive/lnd/hist/*elm.h0*" 2>&1 | tee ../land_extract.log
    if [ $? != 0 ]; then
        echo "${case_name} extract land failed."
        cd ..
        return 1
    fi
    check_log_has "Extracting archive/lnd/hist" ../land_extract.log || return 2
    check_file_exists "archive/lnd/hist/file1.elm.h0.0001-01.nc" || return 2
    check_file_exists "archive/lnd/hist/file2.elm.h0.0001-02.nc" || return 2
    cd ..

    # Step 5: Extract ocean files to different location
    echo ""
    echo "Step 4b: Extracting ocean files from cached tars to different location..."
    mkdir -p ocean_dest
    cd ocean_dest
    zstash extract --hpss=none --cache=../zstash "archive/ocean/hist/*timeSeriesMonthly*" 2>&1 | tee ../ocean_extract.log
    if [ $? != 0 ]; then
        echo "${case_name} extract ocean failed."
        cd ..
        return 1
    fi
    check_log_has "Extracting archive/ocean/hist" ../ocean_extract.log || return 2
    check_file_exists "archive/ocean/hist/file1.timeSeriesMonthly.0001-01.nc" || return 2
    check_file_exists "archive/ocean/hist/file2.timeSeriesMonthly.0001-02.nc" || return 2
    cd ..

    # Verify files are in separate destinations
    echo ""
    echo "Step 5: Verifying files are in separate destinations..."
    ls -R land_dest/ 2>&1 | tee land_dest_contents.log
    check_log_has "elm.h0" land_dest_contents.log || return 2
    check_log_does_not_have "timeSeriesMonthly" land_dest_contents.log || return 2

    ls -R ocean_dest/ 2>&1 | tee ocean_dest_contents.log
    check_log_has "timeSeriesMonthly" ocean_dest_contents.log || return 2
    check_log_does_not_have "elm.h0" ocean_dest_contents.log || return 2

    cd ..

    echo "${case_name} completed successfully."
    return 0
}

test_method2_selective_download()
{
    # Method 2 variant: Verify that check --keep only downloads needed tars
    # (not all tars in the archive)

    local path_to_repo=$1
    local dst_endpoint=$2
    local dst_dir=$3

    src_dir=${path_to_repo}/tests/utils/selective_tar_retrieval
    rm -rf ${src_dir}
    mkdir -p ${src_dir}
    dst_endpoint_uuid=$(get_endpoint ${dst_endpoint})
    globus_path=globus://${dst_endpoint_uuid}/${dst_dir}

    case_name="method2_selective_download"
    echo "Running test_method2_selective_download on case=${case_name}"
    echo "Exit codes: 0 -- success, 1 -- zstash failed, 2 -- grep check failed"

    setup ${case_name} "${src_dir}"

    # Create files with actual size to force multiple tars
    # Create 30MB files - with maxsize=0.1 (100MB), we'll get ~3 files per tar
    echo "Adding files with size to force multiple tars..."
    for i in {3..8}; do
        dd if=/dev/zero of=test_archive/archive/lnd/hist/file${i}.elm.h0.000${i}-01.nc bs=1M count=30 2>/dev/null
    done
    for i in {3..8}; do
        dd if=/dev/zero of=test_archive/archive/ocean/hist/file${i}.timeSeriesMonthly.000${i}-01.nc bs=1M count=30 2>/dev/null
    done
    for i in {3..8}; do
        dd if=/dev/zero of=test_archive/archive/atm/hist/file${i}.cam.h0.000${i}-01.nc bs=1M count=30 2>/dev/null
    done

    # Create the archive on Globus with 100MB maxsize to force multiple tars
    # 6 files per category × 30MB = 180MB per category
    # With 100MB limit, should create at least 2 tars per category
    echo "Step 1: Creating archive with maxsize to force multiple tars..."
    zstash create --hpss=${globus_path}/${case_name} --maxsize 0.1 test_archive 2>&1 | tee ${case_name}_create.log
    if [ $? != 0 ]; then
        echo "${case_name} create failed. Check ${case_name}_create.log for details."
        return 1
    fi

    # Count how many tars were created
    num_tars=$(grep -c "Creating new tar archive" ${case_name}_create.log)
    echo "Created ${num_tars} tar archives"

    if [ ${num_tars} -lt 2 ]; then
        echo "ERROR: Expected multiple tars but only got ${num_tars}"
        echo "This test requires multiple tars to validate selective download."
        return 2
    fi

    # Wait for Globus transfer to complete
    wait_for_directory "${dst_dir}/${case_name}" || return 1

    # Set up a separate directory for testing
    mkdir -p method2_selective_test
    cd method2_selective_test

    # Step 2: Use check --keep for only land files
    echo ""
    echo "Step 2: Downloading only tars with land files..."
    zstash check --hpss=${globus_path}/${case_name} --keep "archive/lnd/hist/*elm.h0*" 2>&1 | tee land_check.log
    if [ $? != 0 ]; then
        echo "${case_name} check land failed."
        return 1
    fi

    # Count downloaded tars
    num_downloaded=$(ls zstash/*.tar 2>/dev/null | wc -l)
    echo "Downloaded ${num_downloaded} tar(s) out of ${num_tars} total"

    # Verify we downloaded fewer tars than total (selective download worked)
    if [ ${num_downloaded} -ge ${num_tars} ]; then
        echo "ERROR: Downloaded ${num_downloaded} tars but expected fewer than ${num_tars}"
        echo "This suggests zstash downloaded ALL tars instead of being selective"
        return 2
    fi

    if [ ${num_downloaded} -eq 0 ]; then
        echo "ERROR: No tars were downloaded"
        return 2
    fi

    echo "SUCCESS: Selective download verified - only ${num_downloaded} of ${num_tars} tars downloaded"

    cd ..

    echo "${case_name} completed successfully."
    return 0
}

test_method2_check_keep_union()
{
    # Method 2 variant: Use single 'zstash check --keep' with multiple patterns
    # to download all needed tars in one command

    local path_to_repo=$1
    local dst_endpoint=$2
    local dst_dir=$3

    src_dir=${path_to_repo}/tests/utils/selective_tar_retrieval
    rm -rf ${src_dir}
    mkdir -p ${src_dir}
    dst_endpoint_uuid=$(get_endpoint ${dst_endpoint})
    globus_path=globus://${dst_endpoint_uuid}/${dst_dir}

    case_name="method2_check_keep_union"
    echo "Running test_method2_check_keep_union on case=${case_name}"
    echo "Exit codes: 0 -- success, 1 -- zstash failed, 2 -- grep check failed"

    setup ${case_name} "${src_dir}"

    # Create the archive on Globus
    echo "Step 1: Creating archive with zstash..."
    zstash create --hpss=${globus_path}/${case_name} test_archive 2>&1 | tee ${case_name}_create.log
    if [ $? != 0 ]; then
        echo "${case_name} create failed. Check ${case_name}_create.log for details."
        return 1
    fi
    check_log_has "Creating new tar archive" ${case_name}_create.log || return 2

    # Wait for Globus transfer to complete
    wait_for_directory "${dst_dir}/${case_name}" || return 1

    # Set up a separate directory for check testing
    mkdir -p method2_union_test
    cd method2_union_test

    # Step 2: Use check --keep with BOTH patterns in a single command
    echo ""
    echo "Step 2: Downloading tars with both land AND ocean files in single command..."
    zstash check --hpss=${globus_path}/${case_name} --keep \
        "archive/lnd/hist/*elm.h0*" \
        "archive/ocean/hist/*timeSeriesMonthly*" \
        2>&1 | tee union_check.log
    if [ $? != 0 ]; then
        echo "${case_name} check union failed."
        return 1
    fi

    # Verify both file types were checked
    check_log_has "Opening tar archive" union_check.log || return 2
    check_log_has "elm.h0" union_check.log || return 2
    check_log_has "timeSeriesMonthly" union_check.log || return 2

    # Verify tars are in cache
    echo ""
    echo "Verifying tars were downloaded to cache..."
    ls -lh zstash/*.tar 2>&1 | tee cache_after_union.log
    check_log_has ".tar" cache_after_union.log || return 2

    # Verify files were NOT extracted to working directory
    echo ""
    echo "Verifying files were NOT extracted (only tars downloaded)..."
    if [ -d "archive" ]; then
        echo "ERROR: Files were extracted when they shouldn't have been!"
        return 2
    fi

    # Step 3: Extract land files to verify tars are usable
    echo ""
    echo "Step 3a: Extracting land files from cached tars..."
    mkdir -p land_dest
    cd land_dest
    zstash extract --hpss=none --cache=../zstash "archive/lnd/hist/*elm.h0*" 2>&1 | tee ../land_extract.log
    if [ $? != 0 ]; then
        echo "${case_name} extract land failed."
        cd ..
        return 1
    fi
    check_log_has "Extracting archive/lnd/hist" ../land_extract.log || return 2
    check_file_exists "archive/lnd/hist/file1.elm.h0.0001-01.nc" || return 2
    check_file_exists "archive/lnd/hist/file2.elm.h0.0001-02.nc" || return 2
    cd ..

    # Step 4: Extract ocean files to different location
    echo ""
    echo "Step 3b: Extracting ocean files from cached tars to different location..."
    mkdir -p ocean_dest
    cd ocean_dest
    zstash extract --hpss=none --cache=../zstash "archive/ocean/hist/*timeSeriesMonthly*" 2>&1 | tee ../ocean_extract.log
    if [ $? != 0 ]; then
        echo "${case_name} extract ocean failed."
        cd ..
        return 1
    fi
    check_log_has "Extracting archive/ocean/hist" ../ocean_extract.log || return 2
    check_file_exists "archive/ocean/hist/file1.timeSeriesMonthly.0001-01.nc" || return 2
    check_file_exists "archive/ocean/hist/file2.timeSeriesMonthly.0001-02.nc" || return 2
    cd ..

    # Step 5: Verify files are in separate destinations
    echo ""
    echo "Step 4: Verifying files are in separate destinations..."
    ls -R land_dest/ 2>&1 | tee land_dest_contents.log
    check_log_has "elm.h0" land_dest_contents.log || return 2
    check_log_does_not_have "timeSeriesMonthly" land_dest_contents.log || return 2

    ls -R ocean_dest/ 2>&1 | tee ocean_dest_contents.log
    check_log_has "timeSeriesMonthly" ocean_dest_contents.log || return 2
    check_log_does_not_have "elm.h0" ocean_dest_contents.log || return 2

    # Step 6: Verify the union command worked by checking both file types were retrieved
    echo ""
    echo "Step 5: Verifying union command successfully retrieved both land and ocean files..."
    echo "SUCCESS: Union command retrieved files matching both patterns in a single check operation"

    cd ..

    echo "${case_name} completed successfully."
    return 0
}

# Follow these directions #####################################################

# Example usage:
# ./test_tar_workflow.bash d417_20260106_try1 /home/ac.forsyth2/ez/zstash /home/ac.forsyth2/zstash_tests LCRC_IMPROV_DTN_ENDPOINT yes

# Command line parameters:
unique_id="$1"
path_to_repo="$2"
dst_basedir="$3"
endpoint_str="$4"
fresh_globus="${5:-no}"
machine_dst_dir=${dst_basedir}/test_selective_tar_retrieval_${unique_id}

echo "You may wish to clear your dst directories for a fresh start:"
echo "rm -rf ${dst_basedir}/test_selective_tar_retrieval*"
echo "It is advisable to just set a unique_id to avoid directory conflicts."
echo "Currently, unique_id=${unique_id}"
if ! confirm "Is the unique_id correct?"; then
    exit 1
fi

echo "Go to https://app.globus.org/file-manager?two_pane=true > For "Collection", choose the endpoint you're testing, and authenticate if needed:"
echo "LCRC Improv DTN, NERSC Perlmutter, NERSC HPSS, pic#compy-dtn"
if ! confirm "Have you authenticated into the endpoint you're testing?"; then
    exit 1
fi

if [ "$fresh_globus" == "yes" ]; then
    INI_PATH=${HOME}/.zstash.ini
    TOKEN_FILE=${HOME}/.zstash_globus_tokens.json
    rm -rf ${INI_PATH}
    rm -rf ${TOKEN_FILE}
    echo "Reset Globus consents:"
    echo "https://auth.globus.org/v2/web/consents > Globus Endpoint Performance Monitoring > rescind all"
    if ! confirm "Have you revoked Globus consents?"; then
        exit 1
    fi
fi

# Test execution with independent runs and pass/fail tracking
run_test_with_tracking() {
    local test_name="$1"
    shift
    local args=("$@")

    echo ""
    echo "=========================================="
    echo "Running: ${test_name}"
    echo "=========================================="

    if $test_name "${args[@]}"; then
        echo "✓ ${test_name} PASSED"
        test_results+=("✓ ${test_name} PASSED")
        ((tests_passed++))
        return 0
    else
        echo "✗ ${test_name} FAILED"
        test_results+=("✗ ${test_name} FAILED")
        ((tests_failed++))
        return 1
    fi
}

# Initialize counters
tests_passed=0
tests_failed=0
test_results=()

echo "Testing selective tar retrieval workflows"
echo "If a test hangs, check if https://app.globus.org/activity reports any errors on your transfers."

# Run all tests independently
run_test_with_tracking test_method1_ls_tars ${path_to_repo} ${endpoint_str} ${machine_dst_dir} || true
run_test_with_tracking test_method1_ls_from_cache_with_keep ${path_to_repo} ${endpoint_str} ${machine_dst_dir} || true
run_test_with_tracking test_method1_ls_from_cache_without_keep ${path_to_repo} ${endpoint_str} ${machine_dst_dir} || true
run_test_with_tracking test_method1_ls_with_cache_dot ${path_to_repo} ${endpoint_str} ${machine_dst_dir} || true
run_test_with_tracking test_method2_check_keep ${path_to_repo} ${endpoint_str} ${machine_dst_dir} || true
run_test_with_tracking test_method2_selective_download ${path_to_repo} ${endpoint_str} ${machine_dst_dir} || true
run_test_with_tracking test_method2_check_keep_union ${path_to_repo} ${endpoint_str} ${machine_dst_dir} || true

# Print summary
echo ""
echo "=========================================="
echo "TEST RESULTS"
echo "=========================================="
for result in "${test_results[@]}"; do
    echo "${result}"
done
echo "=========================================="
echo "TEST SUMMARY"
echo "=========================================="
echo "Total tests: $((tests_passed + tests_failed))"
echo "Passed: ${tests_passed}"
echo "Failed: ${tests_failed}"
echo "=========================================="

if [ ${tests_failed} -eq 0 ]; then
    echo "All selective tar retrieval tests completed successfully."
    exit 0
else
    echo "Some tests failed. Please review the logs above."
    exit 1
fi
