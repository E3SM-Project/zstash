# Assertions ##################################################################
check_log_has()
{
    local expected_grep="${1}"
    local log_file="${2}"
    grep -q "${expected_grep}" ${log_file}
    if [ $? != 0 ]; then
        echo "Expected grep '${expected_grep}' not found in ${log_file}. Test failed."
        exit 2
    fi
}

check_log_does_not_have()
{
    local not_expected_grep="${1}"
    local log_file="${2}"
    grep "${not_expected_grep}" ${log_file}
    if [ $? == 0 ]; then
        echo "Not-expected grep '${expected_grep}' was found in ${log_file}. Test failed."
        exit 2
    fi
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

# Tests #######################################################################
test_single_auth_code()
{
    local path_to_repo=$1
    local dst_endpoint=$2
    local dst_dir=$3

    src_dir=${path_to_repo}/tests/utils/globus_auth
    mkdir -p ${src_dir}
    dst_endpoint_uuid=$(get_endpoint ${dst_endpoint})
    globus_path=globus://${dst_endpoint_uuid}/${dst_dir}

    GLOBUS_CFG=${HOME}/.globus-native-apps.cfg
    INI_PATH=${HOME}/.zstash.ini
    TOKEN_FILE=${HOME}/.zstash_globus_tokens.json

    # Start fresh
    echo "Reset Globus consents:"
    echo "https://auth.globus.org/v2/web/consents > Globus Endpoint Performance Monitoring > rescind all"
    if ! confirm "Have you revoked Globus consents?"; then
        exit 1
    fi
    rm -rf ${GLOBUS_CFG}
    rm -rf ${INI_PATH}
    rm -rf ${TOKEN_FILE}

    echo "Running test_single_auth_code"
    echo "Exit codes: 0 -- success, 1 -- zstash failed, 2 -- grep check failed"

    case_name="run1"
    setup ${case_name} "${src_dir}"
    # Expecting to see exactly 1 authentication prompt
    zstash create --hpss=${globus_path}/${case_name} zstash_demo 2>&1 | tee ${case_name}.log
    if [ $? != 0 ]; then
        echo "${case_name} failed. Check ${case_name}_create.log for details. Cannot continue."
        exit 1
    fi
    echo "${case_name} completed successfully. Checking ${case_name}.log now."
    # From check_state_files
    check_log_does_not_have "WARNING: Globus CFG ${GLOBUS_CFG} exists. This may be left over from earlier versions of zstash, and may cause issues. Consider deleting." ${case_name}.log
    check_log_has "INFO: ${INI_PATH} does NOT exist. This means we won't be able to read the local endpoint ID from it." ${case_name}.log
    check_log_has "INFO: Token file ${TOKEN_FILE} does NOT exist. This means we won't be able to load tokens from it." ${case_name}.log
    # From get_local_endpoint_id
    check_log_has "INFO: Writing to empty ${INI_PATH}" ${case_name}.log
    check_log_has "INFO: Setting local_endpoint_id based on" ${case_name}.log
    # From get_transfer_client_with_auth
    check_log_has "INFO: No stored tokens found - starting authentication" ${case_name}.log
    check_log_has "Please go to this URL and login:" ${case_name}.log # Our one expected authentication prompt
    # From save_tokens
    check_log_has "INFO: Tokens saved successfully" ${case_name}.log


    case_name="run2"
    setup ${case_name} "${src_dir}"
    # Expecting to see exactly 0 authentication prompts
    zstash create --hpss=${globus_path}/${case_name} zstash_demo 2>&1 | tee ${case_name}.log
    if [ $? != 0 ]; then
        echo "${case_name} failed. Check ${case_name}_create.log for details. Cannot continue."
        exit 1
    fi
    echo "${case_name} completed successfully. Checking ${case_name}.log now."
    # From check_state_files
    check_log_does_not_have "WARNING: Globus CFG ${GLOBUS_CFG} exists. This may be left over from earlier versions of zstash, and may cause issues. Consider deleting." ${case_name}.log
    check_log_has "INFO: ${INI_PATH} exists. We can try to read the local endpoint ID from it." ${case_name}.log # Differs from run1
    check_log_has "INFO: Token file ${TOKEN_FILE} exists. We can try to load tokens from it." ${case_name}.log # Differs from run1
    # From get_local_endpoint_id
    check_log_has "INFO: Setting local_endpoint_id based on ${INI_PATH}" ${case_name}.log # Differs from run1
    check_log_has "INFO: Setting local_endpoint_id based on" ${case_name}.log
    # From get_transfer_client_with_auth
    check_log_has "INFO: Found stored refresh token - using it" ${case_name}.log # Differs from run1
    check_log_does_not_have "Please go to this URL and login:" ${case_name}.log # There should be no login prompts for run2!
    # From save_tokens
    check_log_does_not_have "INFO: Tokens saved successfully" ${case_name}.log # Differs from run1

    # This part replaces the original test_globus.py `testLs` function.
    zstash ls --hpss=${globus_path}/run1 2>&1 | tee run1_ls.log
    check_log_has "file_empty.txt" run1_ls.log
    check_log_has "dir/file0.txt" run1_ls.log
    check_log_has "empty_dir" run1_ls.log
    zstash ls --hpss=${globus_path}/run2 2>&1 | tee run2_ls.log
    check_log_has "file_empty.txt" run2_ls.log
    check_log_has "dir/file0.txt" run2_ls.log
    check_log_has "empty_dir" run2_ls.log
    # Could also test -l and -v options, but the above code covers the important part.

    if ! confirm "Did you only have to paste an auth code once (for run1, not run2)?"; then
        echo "Single-authentication test failed"
        exit 1
    fi
    # Cleanup:
    cd ${path_to_repo}/tests/integration/bash_tests/run_from_any
    rm -rf ${path_to_repo}/tests/utils/globus_auth
}

test_different_endpoint1()
{
    local path_to_repo=$1
    local dst_endpoint=$2
    local dst_dir=$3

    src_dir=${path_to_repo}/tests/utils/globus_auth
    mkdir -p ${src_dir}
    dst_endpoint_uuid=$(get_endpoint ${dst_endpoint})
    globus_path=globus://${dst_endpoint_uuid}/${dst_dir}

    echo "Running test_different_endpoint1"
    echo "Exit codes: 0 -- success, 1 -- failure"

    case_name="different_endpoint1"
    setup ${case_name} "${src_dir}"
    # Expecting to see exactly 1 authentication prompt
    zstash create --hpss=${globus_path}/${case_name} zstash_demo 2>&1 | tee ${case_name}.log
    check_log_has "INFO: Found stored refresh token - using it" ${case_name}.log
    check_log_has "ERROR: One possible cause" ${case_name}.log
    check_log_has "ERROR: Try deleting" ${case_name}.log
    check_log_has "ERROR: Another possible cause" ${case_name}.log
    check_log_has "try revoking consents before re-running" ${case_name}.log
    check_log_has "ERROR: Exception: Insufficient Globus consents" ${case_name}.log

    if ! confirm "Did you avoid having to paste any auth codes on this run?"; then
        echo "test_different_endpoint1 failed"
        exit 1
    fi
    # Cleanup:
    cd ${path_to_repo}/tests/integration/bash_tests/run_from_any
    rm -rf ${path_to_repo}/tests/utils/globus_auth
}

test_different_endpoint2()
{
    local path_to_repo=$1
    local dst_endpoint=$2
    local dst_dir=$3

    src_dir=${path_to_repo}/tests/utils/globus_auth
    mkdir -p ${src_dir}
    dst_endpoint_uuid=$(get_endpoint ${dst_endpoint})
    globus_path=globus://${dst_endpoint_uuid}/${dst_dir}

    echo "Reset Globus consents:"
    echo "https://auth.globus.org/v2/web/consents > Globus Endpoint Performance Monitoring > rescind all"
    if ! confirm "Have you revoked Globus consents?"; then
        exit 1
    fi

    echo "Running test_different_endpoint2"
    echo "Exit codes: 0 -- success, 1 -- failure"

    case_name="different_endpoint2a"
    setup ${case_name} "${src_dir}"
    zstash create --hpss=${globus_path}/${case_name} zstash_demo 2>&1 | tee ${case_name}.log
    check_log_has ".zstash_globus_tokens.json exists. We can try to load tokens from it." ${case_name}.log
    check_log_has ".zstash_globus_tokens.json may be configured for a different Globus endpoint." ${case_name}.log
    check_log_has "Try deleting" ${case_name}.log
    check_log_has "globus_sdk.services.auth.errors.AuthAPIError: ('POST', 'https://auth.globus.org/v2/oauth2/token', None, 400, 'Error', 'Bad Request')" ${case_name}.log

    rm -rf ~/.zstash_globus_tokens.json
    case_name="different_endpoint2b"
    setup ${case_name} "${src_dir}"
    # Expecting to see exactly 1 authentication prompt
    zstash create --hpss=${globus_path}/${case_name} zstash_demo 2>&1 | tee ${case_name}.log
    if [ $? != 0 ]; then
        echo "${case_name} failed. Check ${case_name}_create.log for details."
        exit 1
    fi

    if ! confirm "Did you only have to paste an auth code once (for 2b, not 2a)?"; then
        echo "test_different_endpoint2 failed"
        exit 1
    fi
    # Cleanup:
    cd ${path_to_repo}/tests/integration/bash_tests/run_from_any
    rm -rf ${path_to_repo}/tests/utils/globus_auth
}

test_different_endpoint3()
{
    local path_to_repo=$1
    local dst_endpoint=$2
    local dst_dir=$3

    src_dir=${path_to_repo}/tests/utils/globus_auth
    mkdir -p ${src_dir}
    dst_endpoint_uuid=$(get_endpoint ${dst_endpoint})
    globus_path=globus://${dst_endpoint_uuid}/${dst_dir}

    echo "Running test_different_endpoint3"
    echo "Exit codes: 0 -- success, 1 -- failure"

    rm -rf ~/.zstash_globus_tokens.json
    case_name="different_endpoint3"
    setup ${case_name} "${src_dir}"
    # Expecting to see exactly 1 authentication prompt
    zstash create --hpss=${globus_path}/${case_name} zstash_demo 2>&1 | tee ${case_name}.log
    if [ $? != 0 ]; then
        echo "${case_name} failed. Check ${case_name}_create.log for details."
        exit 1
    fi

    if ! confirm "Did you only have to paste an auth code once?"; then
        echo "test_different_endpoint2 failed"
        exit 1
    fi
    # Cleanup:
    cd ${path_to_repo}/tests/integration/bash_tests/run_from_any
    rm -rf ${path_to_repo}/tests/utils/globus_auth
}

# Follow these directions #####################################################

# Example usage:
# ./globus_auth.bash 21 chrysalis /home/ac.forsyth2/ez/zstash /home/ac.forsyth2/zstash_tests /global/homes/f/forsyth/zstash_tests /home/f/forsyth/zstash_tests /compyfs/fors729/zstash_tests

# NOTE: This test will not work from a repo on Compy located in /qfs/people/...
# pic#compy-dtn will only transfer to/from /compyfs/...

# Command line parameters:
unique_id="$1"
src_machine="$2" # chrysalis, perlmutter, compy
path_to_repo="$3" # /home/ac.forsyth2/ez/zstash, /global/homes/f/forsyth/ez/zstash, /qfs/people/fors729/ez/zstash
chrysalis_dst_basedir="$4" # /home/ac.forsyth2/zstash_tests
perlmutter_dst_basedir="$5" # /global/homes/f/forsyth/zstash_tests
hpss_dst_basedir="$6" # /home/f/forsyth/zstash_tests
compy_dst_basedir="$7" # /compyfs/fors729/zstash_tests (/qfs/people/fors729/ => permission denied)

chrysalis_dst_dir=${chrysalis_dst_basedir}/test_globus_auth_${unique_id}
perlmutter_dst_dir=${perlmutter_dst_basedir}/test_globus_auth_${unique_id}
hpss_dst_dir=${hpss_dst_basedir}/test_globus_auth_${unique_id}
compy_dst_dir=${compy_dst_basedir}/test_globus_auth_${unique_id}

# Determine which endpoints to use for the endpoint-switching tests
# switch1 should never be the last-tested endpoint (i.e., compy)
# Neither switch1 nor switch2 should be the src endpoint.
case ${src_machine} in
    chrysalis)
        dst_endpoint_switch1=NERSC_PERLMUTTER_ENDPOINT
        dst_endpoint_switch2=NERSC_HPSS_ENDPOINT
        dst_dir_switch1=${perlmutter_dst_dir}
        dst_dir_switch2=${hpss_dst_dir}
        ;;
    perlmutter)
        dst_endpoint_switch1=LCRC_IMPROV_DTN_ENDPOINT
        dst_endpoint_switch2=PIC_COMPY_DTN_ENDPOINT
        dst_dir_switch1=${chrysalis_dst_dir}
        dst_dir_switch2=${compy_dst_dir}
        ;;
    compy)
        dst_endpoint_switch1=NERSC_PERLMUTTER_ENDPOINT
        dst_endpoint_switch2=NERSC_HPSS_ENDPOINT
        dst_dir_switch1=${perlmutter_dst_dir}
        dst_dir_switch2=${hpss_dst_dir}
        ;;
    *)
        echo "Unknown machine name: ${src_machine}" >&2
        exit 1
        ;;
esac

echo "You may wish to clear your dst directories for a fresh start:"
echo "Chrysalis: rm -rf ${chrysalis_dst_basedir}/test_globus_auth*"
echo "Perlmutter: rm -rf ${perlmutter_dst_basedir}/test_globus_auth*"
echo "Compy: rm -rf ${compy_dst_basedir}/test_globus_auth*"
echo "This won't work on HPSS, because -rf flags are unsupported:"
echo "NERSC HPSS: rm -rf ${hpss_dst_basedir}/test_globus_auth*"
echo ""
echo "It is therefore advisable to just set a unique_id to avoid directory conflicts."
echo "Currently, unique_id=${unique_id}"
if ! confirm "Is the unique_id correct?"; then
    exit 1
fi

echo "Go to https://app.globus.org/file-manager?two_pane=true > For "Collection", choose each of the following endpoints and, if needed, authenticate:"
echo "LCRC Improv DTN, NERSC Perlmutter, NERSC HPSS, pic#compy-dtn"
if ! confirm "Have you authenticated into all endpoints?"; then
    exit 1
fi

echo "Primary tests: single authentication code tests for each endpoint"
echo "If a test hangs, check if https://app.globus.org/activity reports any errors on your transfers."
echo "Testing transfer to LCRC Improv DTN ####################################"
test_single_auth_code ${path_to_repo} LCRC_IMPROV_DTN_ENDPOINT ${chrysalis_dst_dir}
if [ "$src_machine" != "perlmutter" ]; then
    echo "Testing transfer to NERSC Perlmutter ###################################"
    test_single_auth_code ${path_to_repo} NERSC_PERLMUTTER_ENDPOINT ${perlmutter_dst_dir}
fi
echo "Testing transfer to NERSC HPSS #########################################"
test_single_auth_code ${path_to_repo} NERSC_HPSS_ENDPOINT ${hpss_dst_dir}
echo "Testing transfer to pic#compy-dtn ######################################"
test_single_auth_code ${path_to_repo} PIC_COMPY_DTN_ENDPOINT ${compy_dst_dir}

echo "Follow-up tests: behavior when switching to different endpoints"
echo "NOTE: if you commented out tests above, and your last endpoint used was NERSC_PERLMUTTER_ENDPOINT, the following test will not work properly."
echo "Test 1: What if we switch to a different endpoint? #####################"
test_different_endpoint1 ${path_to_repo} ${dst_endpoint_switch1} ${dst_dir_switch1}
echo "Test 2: What if we try a) revoking consents and then b) removing the token file? ###"
test_different_endpoint2 ${path_to_repo} ${dst_endpoint_switch1} ${dst_dir_switch1}
echo "Test 3: What if we switch to a different endpoint again, but first remove the token file? ###"
test_different_endpoint3 ${path_to_repo} ${dst_endpoint_switch2} ${dst_dir_switch2}
echo "Check https://auth.globus.org/v2/web/consents > Globus Endpoint Performance Monitoring: you should have *two* consents there now."
if ! confirm "Does https://auth.globus.org/v2/web/consents > Globus Endpoint Performance Monitoring show *two* consents?"; then
    exit 1
fi
echo "All globus_auth tests completed successfully."
