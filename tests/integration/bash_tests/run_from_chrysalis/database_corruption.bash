#!/bin/bash

# Usage: ./database_corruption.bash <unique_id>
#
# Environment overrides (optional):
#   MAX_ATTEMPTS  Attempts per zstash create/check on Globus infrastructure errors (default: 3)
#   RETRY_SLEEP   Base sleep in seconds between attempts; multiplied by attempt number (default: 60)
#
# Exit codes: 0 = all passed, 1 = at least one real failure, 2 = no real failures,
#             but some checks were inconclusive because Globus errors outlasted the retries.

MAX_ATTEMPTS=${MAX_ATTEMPTS:-3}
RETRY_SLEEP=${RETRY_SLEEP:-60}

# Errors that come from Globus/NERSC infrastructure rather than from zstash logic.
# Keep this narrow: the corruption cases intentionally produce
# "Database corruption detected" and "size does not match expected size",
# which must NOT match here.
INFRA_ERROR_REGEX='ExternalError|TransferAPIError|LoginFailed|Error validating login to endpoint'

setup()
{
  local case_name="${1}"
  local src_prefix="${2}"
  local quiet="${3}"
  if [ "${quiet}" != "quiet" ]; then
    echo "##########################################################################################################"
    echo "Testing: ${case_name}"
  fi
  rm -rf ${src_prefix}_check
  mkdir -p ${src_prefix}_check
  rm -rf ${src_prefix}_create
  mkdir -p ${src_prefix}_create
  cd ${src_prefix}_create

  mkdir zstash_demo
  mkdir zstash_demo/empty_dir
  mkdir zstash_demo/dir
  echo -n '' > zstash_demo/file_empty.txt
  echo 'file0 stuff' > zstash_demo/dir/file0.txt
}

# Record a log path for review (at most once per path).
add_review()
{
    local path="${1}"
    if [[ ",${review_str}" != *",${path},"* ]]; then
        review_str+="${path},"
    fi
}

# run_create <case_name> [extra zstash create args...]
# Runs setup + `zstash create`, retrying from a clean slate if Globus errors out.
# A failed create may leave a tar on the remote, so each retry uses a fresh remote path;
# that would otherwise cause a *real* duplicate and corrupt the test.
# Sets: HPSS_PATH (remote path actually used), LAST_RUN_INFRA_FAILED, CASE_CREATE_INFRA_FAILED.
# Leaves the working directory at ${SRC_DIR}/${case_name}_create.
run_create()
{
    local case_name="${1}"
    shift
    local src_prefix=${SRC_DIR}/${case_name}
    local attempt

    CASE_CREATE_INFRA_FAILED=0
    LAST_RUN_INFRA_FAILED=0
    for ((attempt=1; attempt<=MAX_ATTEMPTS; attempt++)); do
        if [ ${attempt} -eq 1 ]; then
            HPSS_PATH=${DST_DIR}/${case_name}
            setup ${case_name} ${src_prefix}
        else
            HPSS_PATH=${DST_DIR}/${case_name}_retry${attempt}
            setup ${case_name} ${src_prefix} quiet
        fi
        cd ${src_prefix}_create
        zstash create --hpss=${HPSS_PATH} "$@" zstash_demo 2>&1 | tee create.log
        if ! grep -Eq "${INFRA_ERROR_REGEX}" create.log; then
            return 0
        fi
        infra_retry_count=$((infra_retry_count+1))
        echo "WARNING (test script): Globus infrastructure error during create of ${case_name} (attempt ${attempt}/${MAX_ATTEMPTS})."
        if [ ${attempt} -lt ${MAX_ATTEMPTS} ]; then
            echo "WARNING (test script): Retrying with fresh remote path in $((attempt*RETRY_SLEEP))s."
            sleep $((attempt*RETRY_SLEEP))
        fi
    done
    echo "WARNING (test script): Giving up on create for ${case_name}; results will be marked inconclusive."
    LAST_RUN_INFRA_FAILED=1
    CASE_CREATE_INFRA_FAILED=1
    infra_str+="${case_name}_create/create.log,"
    return 1
}

# run_check [extra zstash check args...]
# Runs `zstash check` in the current directory (output to ./check.log), against ${HPSS_PATH}.
# Safe to retry in place: check does not modify the remote.
# Sets: LAST_RUN_INFRA_FAILED.
run_check()
{
    local attempt
    LAST_RUN_INFRA_FAILED=0
    if [ "${CASE_CREATE_INFRA_FAILED}" = "1" ]; then
        echo "WARNING (test script): Skipping check; the create step for this case never succeeded."
        LAST_RUN_INFRA_FAILED=1
        return 1
    fi
    for ((attempt=1; attempt<=MAX_ATTEMPTS; attempt++)); do
        zstash check --hpss=${HPSS_PATH} "$@" 2>&1 | tee check.log
        if ! grep -Eq "${INFRA_ERROR_REGEX}" check.log; then
            return 0
        fi
        infra_retry_count=$((infra_retry_count+1))
        echo "WARNING (test script): Globus infrastructure error during check (attempt ${attempt}/${MAX_ATTEMPTS})."
        if [ ${attempt} -lt ${MAX_ATTEMPTS} ]; then
            echo "WARNING (test script): Retrying in $((attempt*RETRY_SLEEP))s."
            sleep $((attempt*RETRY_SLEEP))
        fi
    done
    echo "WARNING (test script): Giving up on check; results will be marked inconclusive."
    LAST_RUN_INFRA_FAILED=1
    infra_str+="$(pwd)/check.log,"
    return 1
}

# expect_grep <pattern> <logfile> <review_path>
# Counts a success if the fixed-string pattern is in the log, a failure if not.
# If the preceding create/check step was defeated by Globus errors, the expectation
# is counted as inconclusive instead (we can't tell whether zstash behaved correctly).
expect_grep()
{
    local pattern="${1}"
    local logfile="${2}"
    local review_path="${3}"
    if [ "${LAST_RUN_INFRA_FAILED}" = "1" ]; then
        inconclusive_count=$((inconclusive_count+1))
        return
    fi
    grep -F "${pattern}" ${logfile}
    if [ $? != 0 ]; then
        fail_count=$((fail_count+1))
        add_review "${review_path}"
    else
        success_count=$((success_count+1))
    fi
}

run_test_cases()
{

    local unique_id=$1

    SRC_DIR=/lcrc/group/e3sm/ac.forsyth2/zstash_testing/test_database_corruption # Chrysalis
    DST_DIR=globus://6bdc7956-fc0f-4ad2-989c-7aa5ee643a79/global/homes/f/forsyth/zstash/tests/test_database_corruption_${unique_id} # Perlmutter
    # To start fresh, delete the directories on Perlmutter before running. Example:
    # rm -rf /global/homes/f/forsyth/zstash/tests/test_database_corruption_<unique_id>
    # Note: if Globus retries were needed, there may also be <case_name>_retry<N> directories there.

    success_count=0
    fail_count=0
    inconclusive_count=0
    infra_retry_count=0
    review_str=""
    infra_str=""

    # Test case explanations ##################################################
    # 1.`zstash create`, then run `zstash_check` from a different directory.
    # 2. `zstash create`, then run `zstash_check` from a directory that already has `zstash/index.db`.
    # 3. `zstash_create` with `--for-developers-force-database-corruption="simulate_row_existing" --error-on-duplicate-tar`. Errors out on create, so we don't even get to check.
    # 4. `zstash create` with `--for-developers-force-database-corruption="simulate_row_existing_bad_size" --overwrite-duplicate-tars`. We see there's a duplicate tar and we overwrite it with the latest data. `zstash check` confirms the tar is correct.
    # 5. `zstash create` with `--for-developers-force-database-corruption="simulate_row_existing"`. We simply add a duplicate tar, but `zstash check` with `--error-on-duplicate-tar` errors out because it finds two entries for the same tar.
    # 6. `zstash create` with `--for-developers-force-database-corruption="simulate_no_correct_size"` to construct a very bad database: two entries for the same tar, both with incorrect sizes. `zstash check` confirms that no entries match the actual file size.
    # 7. `zstash create` with `--for-developers-force-database-corruption="simulate_row_existing_bad_size"`. We add a duplicate tar, but with the wrong size. `zstash check` confirms that the other entry matches the actual file size, so it succeeds.
    # 8. `zstash create` with `--for-developers-force-database-corruption="simulate_bad_size_for_most_recent"` to construct two entries for the same tar, the most recent of which has an incorrect size. `zstash check` fails because the most recent size does not match, but it does log that one of the entries matches the actual file size.

    # Standard cases ##########################################################


    # Case 1: zstash create, check from different directory
    case_name="normal"
    src_prefix=${SRC_DIR}/${case_name}
    run_create ${case_name}
    expect_grep "INFO: Adding 000000.tar to the database." create.log "${case_name}_create/create.log"
    cd ${src_prefix}_check
    run_check
    expect_grep "INFO: 000000.tar: Found a single database entry." check.log "${case_name}_check/check.log"


    # Case 2: zstash create, check from same directory (i.e., an index.db already exists)
    case_name="check_from_same_dir"
    src_prefix=${SRC_DIR}/${case_name}
    run_create ${case_name}
    expect_grep "INFO: Adding 000000.tar to the database." create.log "${case_name}_create/create.log"
    cd ${src_prefix}_create/zstash_demo # Use a directory that already has a zstash/index.db!
    run_check
    expect_grep "INFO: 000000.tar: Found a single database entry." check.log "${case_name}_create/zstash_demo/check.log" # Notice this is a different path!


    # Corrupted database cases ################################################
    # --for-developers-force-database-corruption is set on `zstash create`


    # Case 3: Duplicates detected! Error out on create. Don't even get to check.
    case_name="error_on_create"
    src_prefix=${SRC_DIR}/${case_name}
    run_create ${case_name} --for-developers-force-database-corruption="simulate_row_existing" --error-on-duplicate-tar
    expect_grep "INFO: TESTING/DEBUGGING ONLY: Simulating row existing for 000000.tar." create.log "${case_name}_create/create.log"
    expect_grep "ERROR: Database corruption detected! 000000.tar is already in the database." create.log "${case_name}_create/create.log"


    # Case 4: Duplicates detected! Overwrite them. Proceed with check, as usual.
    case_name="overwrite_duplicate"
    src_prefix=${SRC_DIR}/${case_name}
    run_create ${case_name} --for-developers-force-database-corruption="simulate_row_existing_bad_size" --overwrite-duplicate-tars
    expect_grep "INFO: TESTING/DEBUGGING ONLY: Simulating row existing with bad size for 000000.tar." create.log "${case_name}_create/create.log"
    expect_grep "WARNING: Database corruption detected! 000000.tar is already in the database." create.log "${case_name}_create/create.log"
    expect_grep "WARNING: Updating existing tar 000000.tar to proceed." create.log "${case_name}_create/create.log"
    cd ${src_prefix}_check
    # We should have ovewritten the wrong size with the real size, so check should pass.
    run_check
    expect_grep "INFO: 000000.tar: Found a single database entry." check.log "${case_name}_check/check.log"


    # Case 5: Duplicates detected! Allow them. Error out on check because duplicates are present.
    case_name="check_detects_duplicate"
    src_prefix=${SRC_DIR}/${case_name}
    run_create ${case_name} --for-developers-force-database-corruption="simulate_row_existing"
    expect_grep "INFO: TESTING/DEBUGGING ONLY: Simulating row existing for 000000.tar." create.log "${case_name}_create/create.log"
    expect_grep "WARNING: Database corruption detected! 000000.tar is already in the database." create.log "${case_name}_create/create.log"
    expect_grep "WARNING: Adding a new entry for 000000.tar." create.log "${case_name}_create/create.log"
    cd ${src_prefix}_check
    run_check --error-on-duplicate-tar
    expect_grep "ERROR: Database corruption detected! Found 2 database entries for 000000.tar, with sizes" check.log "${case_name}_check/check.log"


    # Case 6: Duplicates detected! Allow them. Error out on check because none of the sizes match.
    case_name="check_finds_no_matching_size"
    src_prefix=${SRC_DIR}/${case_name}
    run_create ${case_name} --for-developers-force-database-corruption="simulate_no_correct_size"
    expect_grep "INFO: TESTING/DEBUGGING ONLY: Simulating no correct size for 000000.tar." create.log "${case_name}_create/create.log"
    cd ${src_prefix}_check
    run_check
    expect_grep "WARNING: Database corruption detected! Found 2 database entries for 000000.tar, with sizes" check.log "${case_name}_check/check.log"
    expect_grep "INFO: 000000.tar: No database entry matches the actual file size:" check.log "${case_name}_check/check.log"


    # Case 7: Duplicates detected! Allow them. Pass check because the most recent size matches.
    case_name="check_finds_most_recent_size_matches"
    src_prefix=${SRC_DIR}/${case_name}
    run_create ${case_name} --for-developers-force-database-corruption="simulate_row_existing_bad_size"
    expect_grep "INFO: TESTING/DEBUGGING ONLY: Simulating row existing with bad size for 000000.tar." create.log "${case_name}_create/create.log"
    expect_grep "WARNING: Database corruption detected! 000000.tar is already in the database." create.log "${case_name}_create/create.log"
    expect_grep "WARNING: Adding a new entry for 000000.tar." create.log "${case_name}_create/create.log"
    cd ${src_prefix}_check
    run_check
    expect_grep "WARNING: Database corruption detected! Found 2 database entries for 000000.tar, with sizes" check.log "${case_name}_check/check.log"
    expect_grep "INFO: 000000.tar: The most recent database entry has the same size as the actual file size:" check.log "${case_name}_check/check.log"


    # Case 8: Duplicates detected! Allow them. Error out on check because the most recent size doesn't match.
    case_name="check_finds_most_recent_size_does_not_match"
    src_prefix=${SRC_DIR}/${case_name}
    run_create ${case_name} --for-developers-force-database-corruption="simulate_bad_size_for_most_recent"
    expect_grep "INFO: TESTING/DEBUGGING ONLY: Simulating bad size for most recent entry for 000000.tar." create.log "${case_name}_create/create.log"
    cd ${src_prefix}_check
    run_check
    expect_grep "WARNING: Database corruption detected! Found 2 database entries for 000000.tar, with sizes" check.log "${case_name}_check/check.log"
    expect_grep "INFO: 000000.tar: A database entry matches the actual file size," check.log "${case_name}_check/check.log"


    # Summary
    echo "##########################################################################################################"
    echo "Success count: ${success_count}"
    echo "Fail count: ${fail_count}"
    echo "Inconclusive count (Globus errors outlasted retries): ${inconclusive_count}"
    echo "Globus retries performed: ${infra_retry_count}"
    echo "Review (real failures): ${review_str}"
    echo "Review (Globus infrastructure failures): ${infra_str}"

    if [ ${fail_count} -gt 0 ]; then
        return 1
    elif [ ${inconclusive_count} -gt 0 ]; then
        return 2
    fi
    return 0
}

if [ -z "$1" ]; then
    echo "Usage: $0 <unique_id>"
    exit 1
fi

run_test_cases "$1"
exit $?

# Expected on a healthy run:
# Success count: 25
# Fail count: 0
# Inconclusive count (Globus errors outlasted retries): 0
# Globus retries performed: 0
# Review (real failures):
# Review (Globus infrastructure failures):
