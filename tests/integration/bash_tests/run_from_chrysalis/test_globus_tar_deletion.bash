# Assertions ##################################################################
check_log_has()
{
    local expected_grep="${1}"
    local log_file="${2}"
    grep -q "${expected_grep}" ${log_file}
    if [ $? != 0 ]; then
        echo "Expected grep '${expected_grep}' not found in ${log_file}. Test failed."
        return 2  # Changed from exit 2
    fi
    return 0
}

check_log_does_not_have()
{
    local not_expected_grep="${1}"
    local log_file="${2}"
    grep -q "${not_expected_grep}" ${log_file}
    if [ $? == 0 ]; then
        echo "Not-expected grep '${not_expected_grep}' was found in ${log_file}. Test failed."
        return 2  # Changed from exit 2
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
    case ${endpoint_name} in
        LCRC_IMPROV_DTN_ENDPOINT)
            echo ${LCRC_IMPROV_DTN_ENDPOINT}
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
        return 1  # Changed from exit 1
    fi
    echo "${case_name} completed successfully. Checking ${case_name}.log now."
    check_log_has "Creating new tar archive 000000.tar" ${case_name}.log || return 2  # Added return

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
# ./test_globus_tar_deletion.bash run1 /home/ac.forsyth2/ez/zstash /home/ac.forsyth2/zstash_tests

# Command line parameters:
unique_id="$1"
path_to_repo="$2" # /home/ac.forsyth2/ez/zstash
chrysalis_dst_basedir="$3" # /home/ac.forsyth2/zstash_tests
chrysalis_dst_dir=${chrysalis_dst_basedir}/test_globus_tar_deletion_${unique_id}


echo "You may wish to clear your dst directories for a fresh start:"
echo "Chrysalis: rm -rf ${chrysalis_dst_basedir}/test_globus_tar_deletion*"
echo "It is advisable to just set a unique_id to avoid directory conflicts."
echo "Currently, unique_id=${unique_id}"
if ! confirm "Is the unique_id correct?"; then
    exit 1
fi

echo "Go to https://app.globus.org/file-manager?two_pane=true > For "Collection", choose each of the following endpoints and, if needed, authenticate:"
echo "LCRC Improv DTN"
if ! confirm "Have you authenticated into all endpoints?"; then
    exit 1
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
        echo "✓ ${test_name} PASSED"
        ((tests_passed++))
        return 0
    else
        echo "✗ ${test_name} FAILED"
        ((tests_failed++))
        return 1
    fi
}

# Initialize counters
tests_passed=0
tests_failed=0

echo "Primary tests: single authentication code tests for each endpoint"
echo "If a test hangs, check if https://app.globus.org/activity reports any errors on your transfers."

# Run all tests independently
run_test_with_tracking "blocking_non-keep" ${path_to_repo} LCRC_IMPROV_DTN_ENDPOINT ${chrysalis_dst_dir} "blocking" "non-keep" || true
run_test_with_tracking "non-blocking_non-keep" ${path_to_repo} LCRC_IMPROV_DTN_ENDPOINT ${chrysalis_dst_dir} "non-blocking" "non-keep" || true
run_test_with_tracking "blocking_keep" ${path_to_repo} LCRC_IMPROV_DTN_ENDPOINT ${chrysalis_dst_dir} "blocking" "keep" || true
run_test_with_tracking "non-blocking_keep" ${path_to_repo} LCRC_IMPROV_DTN_ENDPOINT ${chrysalis_dst_dir} "non-blocking" "keep" || true

# Print summary
echo ""
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
