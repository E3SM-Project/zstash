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

check_log_has()
{
    local expected_grep="${1}"
    local log_file="${2}"
    grep "${expected_grep}" ${log_file}
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

run_test_cases()
{
    # This script requires user input and thus cannot be run automatically as part of a test suite.

    # To start fresh with Globus:
    # 1. Log into endpoints (LCRC Improv DTN, NERSC Perlmutter) at globus.org: File Manager > Add the endpoints in the "Collection" fields
    # 2. To start fresh, with no consents: https://auth.globus.org/v2/web/consents > Manage Your Consents > Globus Endpoint Performance Monitoring > rescind all"

    # Before each run:
    # Perlmutter:
    # cd /global/homes/f/forsyth/zstash/tests/
    # rm -rf test_globus_auth_try1 # Or just change $DST_DIR to a new directory
    #
    # Chrysalis:
    # cd ~/ez/zstash/
    # conda activate <env-name>
    # pre-commit run --all-files
    # python -m pip install .
    # cd tests/integration/workflows/run_from_chrysalis
    # ./globus_auth.bash

    PERLMUTTER_ENDPOINT=6bdc7956-fc0f-4ad2-989c-7aa5ee643a79

    TRY_NUM=8
    SRC_DIR=/lcrc/group/e3sm/ac.forsyth2/zstash_testing/test_globus_auth # Chrysalis
    DST_DIR=globus://${PERLMUTTER_ENDPOINT}/global/homes/f/forsyth/zstash/tests/test_globus_auth_try${TRY_NUM}

    GLOBUS_CFG=/home/ac.forsyth2/.globus-native-apps.cfg
    INI_PATH=/home/ac.forsyth2/.zstash.ini
    TOKEN_FILE=/home/ac.forsyth2/.zstash_globus_tokens.json

    # Start fresh
    rm -rf ${GLOBUS_CFG}
    rm -rf ${INI_PATH}
    rm -rf ${TOKEN_FILE}

    echo "Running globus_auth test"
    echo "Exit codes: 0 -- success, 1 -- zstash failed, 2 -- grep check failed"

    case_name="run1"
    setup ${case_name} "${SRC_DIR}"
    # Expecting to see exactly 1 authentication prompt
    zstash create --hpss=${DST_DIR}/${case_name} zstash_demo 2>&1 | tee ${case_name}.log
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
    check_log_has "INFO: Setting local_endpoint_id based on FQDN chrlogin2.lcrc.anl.gov:" ${case_name}.log
    # From get_transfer_client_with_auth
    check_log_has "INFO: No stored tokens found - starting authentication" ${case_name}.log
    check_log_has "Please go to this URL and login:" ${case_name}.log # Our one expected authentication prompt
    # From save_tokens
    check_log_has "INFO: Tokens saved successfully" ${case_name}.log


    case_name="run2"
    setup ${case_name} "${SRC_DIR}"
    # Expecting to see exactly 0 authentication prompts
    zstash create --hpss=${DST_DIR}/${case_name} zstash_demo 2>&1 | tee ${case_name}.log
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
    check_log_has "INFO: Setting local_endpoint_id based on FQDN chrlogin2.lcrc.anl.gov:" ${case_name}.log
    # From get_transfer_client_with_auth
    check_log_has "INFO: Found stored refresh token - using it" ${case_name}.log # Differs from run1
    check_log_does_not_have "Please go to this URL and login:" ${case_name}.log # There should be no login prompts for run2!
    # From save_tokens
    check_log_does_not_have "INFO: Tokens saved successfully" ${case_name}.log # Differs from run1

    # This part replaces the original test_globus.py `testLs` function.
    zstash ls --hpss=${DST_DIR}/run1 2>&1 | tee run1_ls.log
    check_log_has "file_empty.txt" run1_ls.log
    check_log_has "dir/file0.txt" run1_ls.log
    check_log_has "empty_dir" run1_ls.log
    zstash ls --hpss=${DST_DIR}/run2 2>&1 | tee run2_ls.log
    check_log_has "file_empty.txt" run2_ls.log
    check_log_has "dir/file0.txt" run2_ls.log
    check_log_has "empty_dir" run2_ls.log
    # Could also test -l and -v options, but the above code covers the important part.
}

run_test_cases
