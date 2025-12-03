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

  mkdir zstash_demo
  mkdir zstash_demo/empty_dir
  mkdir zstash_demo/dir
  echo -n '' > zstash_demo/file_empty.txt
  echo 'file0 stuff' > zstash_demo/dir/file0.txt
}

get_endpoint()
{
    # Usage example:
    # uuid=$(get_endpoint NERSC_PERLMUTTER_ENDPOINT)

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
test_globus_tar_deletion()
{
    local path_to_repo=$1
    local dst_endpoint=$2
    local dst_dir=$3
    local blocking_str=$4
    local keep_str=$5

    src_dir=${path_to_repo}/tests/utils/globus_tar_deletion
    rm -rf ${src_dir} # Start fresh
    mkdir -p ${src_dir}
    dst_endpoint_uuid=$(get_endpoint ${dst_endpoint})
    globus_path=globus://${dst_endpoint_uuid}/${dst_dir}

    case_name=${blocking_str}_${keep_str}
    echo "Running test_globus_tar_deletion on case=${case_name}"
    echo "Exit codes: 0 -- success, 1 -- zstash failed, 2 -- grep check failed"

    setup ${case_name} "${src_dir}"

    if [ "$blocking_str" == "non-blocking" ]; then
        blocking_flag="--non-blocking"
    else
        blocking_flag=""
    fi

    if [ "$keep_str" == "keep" ]; then
        keep_flag="--keep"
    else
        keep_flag=""
    fi

    zstash create ${blocking_flag} ${keep_flag} --hpss=${globus_path}/${case_name} --maxsize 128 zstash_demo 2>&1 | tee ${case_name}.log
    if [ $? != 0 ]; then
        echo "${case_name} failed. Check ${case_name}_create.log for details. Cannot continue."
        return 1
    fi
    echo "${case_name} completed successfully. Checking ${case_name}.log now."
    check_log_has "Creating new tar archive 000000.tar" ${case_name}.log || return 2

    echo ""
    echo "Checking directory status after 'zstash create' has completed. src should only have index.db. dst should have tar and index.db."
    echo "Checking logs in current directory: ${PWD}"

    echo ""
    echo "Checking src"
    ls ${src_dir}/${case_name}/ 2>&1 | tee ls_${case_name}_src_output.log
    check_log_has "${case_name}.log" ls_${case_name}_src_output.log || return 2
    check_log_has "zstash_demo" ls_${case_name}_src_output.log || return 2
    echo ""
    ls ${src_dir}/${case_name}/zstash_demo 2>&1 | tee ls_${case_name}_src2_output.log
    check_log_has "zstash" ls_${case_name}_src2_output.log || return 2
    echo ""
    ls ${src_dir}/${case_name}/zstash_demo/zstash 2>&1 | tee ls_${case_name}_src3_output.log
    check_log_has "index.db" ls_${case_name}_src3_output.log || return 2
    if [ "$keep_str" == "keep" ]; then
        check_log_has "000000.tar" ls_${case_name}_src3_output.log || return 2
    else
        check_log_does_not_have "000000.tar" ls_${case_name}_src3_output.log || return 2
    fi

    echo ""
    echo "Checking dst"
    if [ "$blocking_str" == "non-blocking" ]; then
        wait_for_directory "${dst_dir}/${case_name}" || return 1
    fi
    ls ${dst_dir}/ 2>&1 | tee ls_${case_name}_dst_output.log
    check_log_has "${case_name}" ls_${case_name}_dst_output.log || return 2
    echo ""
    ls ${dst_dir}/${case_name}/ 2>&1 | tee ls_${case_name}_dst2_output.log
    check_log_has "index.db" ls_${case_name}_dst2_output.log || return 2
    check_log_has "000000.tar" ls_${case_name}_dst2_output.log || return 2

    return 0  # Success
}

# Follow these directions #####################################################

# Example usage:
# ./test_globus_tar_deletion.bash run1 /home/ac.forsyth2/ez/zstash /home/ac.forsyth2/zstash_tests LCRC_IMPROV_DTN_ENDPOINT

# Command line parameters:
unique_id="$1"
path_to_repo="$2" # /home/ac.forsyth2/ez/zstash
dst_basedir="$3" # /home/ac.forsyth2/zstash_tests
endpoint_str="$4" # LCRC_IMPROV_DTN_ENDPOINT
fresh_globus="${5:-no}" # Default to "no" if not provided
machine_dst_dir=${dst_basedir}/test_globus_tar_deletion_${unique_id}


echo "You may wish to clear your dst directories for a fresh start:"
echo "rm -rf ${dst_basedir}/test_globus_tar_deletion*"
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

    if test_globus_tar_deletion "${args[@]}"; then
        # Print test result in the output block AND at the end
        echo "✓ ${test_name} PASSED"
        test_results+=("✓ ${test_name} PASSED") # Uses Global variable
        ((tests_passed++))
        return 0
    else
        # Print test result in the output block AND at the end
        echo "✗ ${test_name} FAILED"
        test_results+=("✗ ${test_name} FAILED") # Uses Global variable
        ((tests_failed++))
        return 1
    fi
}

# Initialize counters
tests_passed=0
tests_failed=0
test_results=() # Global variable to hold test results

echo "Primary tests: single authentication code tests for each endpoint"
echo "If a test hangs, check if https://app.globus.org/activity reports any errors on your transfers."

# Run all tests independently
run_test_with_tracking "blocking_non-keep" ${path_to_repo} ${endpoint_str} ${machine_dst_dir} "blocking" "non-keep" || true
run_test_with_tracking "non-blocking_non-keep" ${path_to_repo} ${endpoint_str} ${machine_dst_dir} "non-blocking" "non-keep" || true
run_test_with_tracking "blocking_keep" ${path_to_repo} ${endpoint_str} ${machine_dst_dir} "blocking" "keep" || true
run_test_with_tracking "non-blocking_keep" ${path_to_repo} ${endpoint_str} ${machine_dst_dir} "non-blocking" "keep" || true

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
    echo "All globus tar deletion tests completed successfully."
    exit 0
else
    echo "Some tests failed. Please review the logs above."
    exit 1
fi
