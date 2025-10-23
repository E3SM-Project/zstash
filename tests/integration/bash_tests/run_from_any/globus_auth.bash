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

# Running #####################################################################
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

run_test_cases()
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
    rm -rf ${GLOBUS_CFG}
    rm -rf ${INI_PATH}
    rm -rf ${TOKEN_FILE}

    echo "Running globus_auth test"
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
    check_log_has "INFO: Setting local_endpoint_id based on FQDN chrlogin2.lcrc.anl.gov:" ${case_name}.log
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
    check_log_has "INFO: Setting local_endpoint_id based on FQDN chrlogin2.lcrc.anl.gov:" ${case_name}.log
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
}

# Follow these directions #####################################################
# Modify these parameters as needed.

# Step 1. Update try_num for each new test run to avoid conflicts with previous runs.
# Alternative: Remove previous test directories manually.
try_num=16

# Step 2. Set paths for your environment by uncommenting the appropriate lines.
# Running from Chrysalis:
path_to_repo = /home/ac.forsyth2/ez/zstash/
dst_dir = /home/ac.forsyth2/zstash/tests/test_globus_auth_try${try_num}
# Running from Perlmutter:
# path_to_repo = /global/homes/f/forsyth/ez/zstash
# dst_dir = global/homes/f/forsyth/zstash/tests/test_globus_auth_try${try_num}
# Running from Compy:
# path_to_repo = /qfs/people/fors729/ez/zstash
# dst_dir= /qfs/people/fors729/zstash/tests/test_globus_auth_try${try_num}

# Step 3. Run the test cases for each endpoint. 
# Do once: https://app.globus.org/file-manager?two_pane=true > For "Collection", select the endpoint for the machine you're on, and authenticate if needed.
# For each line below:
# A. Uncomment the appropriate line
# B. https://app.globus.org/file-manager?two_pane=true > For "Collection", select the dst_endpoint name, and authenticate if needed.
# C. https://auth.globus.org/v2/web/consents > Manage Your Consents > Globus Endpoint Performance Monitoring > rescind all"
# D. Run the script with `./globus_auth.bash`
#  - Paste the URL into your browser
#  - Authenticate to src_endpoint if needed.
#  - Authenticate to dst_endpoint if needed.
#  - Provide a label
#  - Copy the auth code to the command line
run_test_cases ${path_to_repo} LCRC_IMPROV_DTN_ENDPOINT ${dst_dir}
run_test_cases ${path_to_repo} NERSC_PERLMUTTER_ENDPOINT ${dst_dir}
run_test_cases ${path_to_repo} NERSC_HPSS_ENDPOINT ${dst_dir}
run_test_cases ${path_to_repo} PIC_COMPY_DTN_ENDPOINT ${dst_dir}
run_test_cases ${path_to_repo} GLOBUS_TUTORIAL_COLLECTION_1_ENDPOINT ${dst_dir}

