setup()
{
  echo "##########################################################################################################"
  local case_name="${1}"
  local src_prefix="${2}"
  echo "Testing: ${case_name}"
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

run_test_cases()
{

    local unique_id=$1

    SRC_DIR=/lcrc/group/e3sm/ac.forsyth2/zstash_testing/test_database_corruption # Chrysalis
    DST_DIR=globus://6bdc7956-fc0f-4ad2-989c-7aa5ee643a79/global/homes/f/forsyth/zstash/tests/test_database_corruption_${unique_id} # Perlmutter
    # To start fresh, delete the directories on Perlmutter before running. Example:
    # rm -rf /global/homes/f/forsyth/zstash/tests/test_database_corruption_<unique_id>

    success_count=0
    fail_count=0
    review_str=""

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
    setup ${case_name} ${src_prefix}
    cd ${src_prefix}_create
    zstash create --hpss=${DST_DIR}/${case_name} zstash_demo 2>&1 | tee create.log
    grep "INFO: Adding 000000.tar to the database." create.log
    if [ $? != 0 ]; then
        ((fail_count++))
        review_str+="${case_name}_create/create.log,"
    else
        ((success_count++))
    fi
    cd ${src_prefix}_check
    zstash check --hpss=${DST_DIR}/${case_name} 2>&1 | tee check.log
    grep "INFO: 000000.tar: Found a single database entry." check.log
    if [ $? != 0 ]; then
        ((fail_count++))
        review_str+="${case_name}_check/check.log,"
    else
        ((success_count++))
    fi


    # Case 2: zstash create, check from same directory (i.e., an index.db already exists)
    case_name="check_from_same_dir"
    src_prefix=${SRC_DIR}/${case_name}
    setup ${case_name} ${src_prefix}
    cd ${src_prefix}_create
    zstash create --hpss=${DST_DIR}/${case_name} zstash_demo 2>&1 | tee create.log
    grep "INFO: Adding 000000.tar to the database." create.log
    if [ $? != 0 ]; then
        ((fail_count++))
        review_str+="${case_name}_create/create.log,"
    else
        ((success_count++))
    fi
    cd zstash_demo # Use a directory that already has a zstash/index.db!
    zstash check --hpss=${DST_DIR}/${case_name} 2>&1 | tee check.log
    grep "INFO: 000000.tar: Found a single database entry." check.log
    if [ $? != 0 ]; then
        ((fail_count++))
        review_str+="${case_name}_create/zstash_demo/check.log," # Notice this is a different path!
    else
        ((success_count++))
    fi


    # Corrupted database cases ################################################
    # --for-developers-force-database-corruption is set on `zstash create`


    # Case 3: Duplicates detected! Error out on create. Don't even get to check.
    case_name="error_on_create"
    src_prefix=${SRC_DIR}/${case_name}
    setup ${case_name} ${src_prefix}
    cd ${src_prefix}_create
    zstash create --hpss=${DST_DIR}/${case_name} --for-developers-force-database-corruption="simulate_row_existing" --error-on-duplicate-tar zstash_demo 2>&1 | tee create.log
    grep "INFO: TESTING/DEBUGGING ONLY: Simulating row existing for 000000.tar." create.log
    if [ $? != 0 ]; then
        ((fail_count++))
        review_str+="${case_name}_create/create.log,"
    else
        ((success_count++))
    fi
    grep "ERROR: Database corruption detected! 000000.tar is already in the database." create.log
    if [ $? != 0 ]; then
        ((fail_count++))
    else
        ((success_count++))
    fi


    # Case 4: Duplicates detected! Overwrite them. Proceed with check, as usual.
    case_name="overwrite_duplicate"
    src_prefix=${SRC_DIR}/${case_name}
    setup ${case_name} ${src_prefix}
    cd ${src_prefix}_create
    zstash create --hpss=${DST_DIR}/${case_name} --for-developers-force-database-corruption="simulate_row_existing_bad_size" --overwrite-duplicate-tars zstash_demo 2>&1 | tee create.log
    grep "INFO: TESTING/DEBUGGING ONLY: Simulating row existing with bad size for 000000.tar." create.log
    if [ $? != 0 ]; then
        ((fail_count++))
        review_str+="${case_name}_create/create.log,"
    else
        ((success_count++))
    fi
    grep "WARNING: Database corruption detected! 000000.tar is already in the database." create.log
    if [ $? != 0 ]; then
        ((fail_count++))
    else
        ((success_count++))
    fi
    grep "WARNING: Updating existing tar 000000.tar to proceed." create.log
    if [ $? != 0 ]; then
        ((fail_count++))
    else
        ((success_count++))
    fi
    cd ${src_prefix}_check
    # We should have ovewritten the wrong size with the real size, so check should pass.
    zstash check --hpss=${DST_DIR}/${case_name} 2>&1 | tee check.log
    grep "INFO: 000000.tar: Found a single database entry." check.log
    if [ $? != 0 ]; then
        ((fail_count++))
        review_str+="${case_name}_check/check.log,"
    else
        ((success_count++))
    fi


    # Case 5: Duplicates detected! Allow them. Error out on check because duplicates are present.
    case_name="check_detects_duplicate"
    src_prefix=${SRC_DIR}/${case_name}
    setup ${case_name} ${src_prefix}
    cd ${src_prefix}_create
    zstash create --hpss=${DST_DIR}/${case_name} --for-developers-force-database-corruption="simulate_row_existing" zstash_demo 2>&1 | tee create.log
    grep "INFO: TESTING/DEBUGGING ONLY: Simulating row existing for 000000.tar." create.log
    if [ $? != 0 ]; then
        ((fail_count++))
        review_str+="${case_name}_create/create.log,"
    else
        ((success_count++))
    fi
    grep "WARNING: Database corruption detected! 000000.tar is already in the database." create.log
    if [ $? != 0 ]; then
        ((fail_count++))
    else
        ((success_count++))
    fi
    grep "WARNING: Adding a new entry for 000000.tar." create.log
    if [ $? != 0 ]; then
        ((fail_count++))
    else
        ((success_count++))
    fi
    cd ${src_prefix}_check
    zstash check --hpss=${DST_DIR}/${case_name} --error-on-duplicate-tar 2>&1 | tee check.log
    grep "ERROR: Database corruption detected! Found 2 database entries for 000000.tar, with sizes" check.log
    if [ $? != 0 ]; then
        ((fail_count++))
        review_str+="${case_name}_check/check.log,"
    else
        ((success_count++))
    fi


    # Case 6: Duplicates detected! Allow them. Error out on check because none of the sizes match.
    case_name="check_finds_no_matching_size"
    src_prefix=${SRC_DIR}/${case_name}
    setup ${case_name} ${src_prefix}
    cd ${src_prefix}_create
    zstash create --hpss=${DST_DIR}/${case_name} --for-developers-force-database-corruption="simulate_no_correct_size" zstash_demo 2>&1 | tee create.log
    grep "INFO: TESTING/DEBUGGING ONLY: Simulating no correct size for 000000.tar." create.log
    if [ $? != 0 ]; then
        ((fail_count++))
        review_str+="${case_name}_create/create.log,"
    else
        ((success_count++))
    fi
    cd ${src_prefix}_check
    zstash check --hpss=${DST_DIR}/${case_name} 2>&1 | tee check.log
    grep "WARNING: Database corruption detected! Found 2 database entries for 000000.tar, with sizes" check.log
    if [ $? != 0 ]; then
        ((fail_count++))
        review_str+="${case_name}_check/check.log,"
    else
        ((success_count++))
    fi
    grep "INFO: 000000.tar: No database entry matches the actual file size:" check.log
    if [ $? != 0 ]; then
        ((fail_count++))
    else
        ((success_count++))
    fi


    # Case 7: Duplicates detected! Allow them. Pass check because the most recent size matches.
    case_name="check_finds_most_recent_size_matches"
    src_prefix=${SRC_DIR}/${case_name}
    setup ${case_name} ${src_prefix}
    cd ${src_prefix}_create
    zstash create --hpss=${DST_DIR}/${case_name} --for-developers-force-database-corruption="simulate_row_existing_bad_size" zstash_demo 2>&1 | tee create.log
    grep "INFO: TESTING/DEBUGGING ONLY: Simulating row existing with bad size for 000000.tar." create.log
    if [ $? != 0 ]; then
        ((fail_count++))
        review_str+="${case_name}_create/create.log,"
    else
        ((success_count++))
    fi
    grep "WARNING: Database corruption detected! 000000.tar is already in the database." create.log
    if [ $? != 0 ]; then
        ((fail_count++))
    else
        ((success_count++))
    fi
    grep "WARNING: Adding a new entry for 000000.tar." create.log
    if [ $? != 0 ]; then
        ((fail_count++))
    else
        ((success_count++))
    fi
    cd ${src_prefix}_check
    zstash check --hpss=${DST_DIR}/${case_name} 2>&1 | tee check.log
    grep "WARNING: Database corruption detected! Found 2 database entries for 000000.tar, with sizes" check.log
    if [ $? != 0 ]; then
        ((fail_count++))
        review_str+="${case_name}_check/check.log,"
    else
        ((success_count++))
    fi
    grep "INFO: 000000.tar: The most recent database entry has the same size as the actual file size:" check.log
    if [ $? != 0 ]; then
        ((fail_count++))
    else
        ((success_count++))
    fi

    # Case 8: Duplicates detected! Allow them. Error out on check because the most recent size doesn't match.
    case_name="check_finds_most_recent_size_does_not_match"
    src_prefix=${SRC_DIR}/${case_name}
    setup ${case_name} ${src_prefix}
    cd ${src_prefix}_create
    zstash create --hpss=${DST_DIR}/${case_name} --for-developers-force-database-corruption="simulate_bad_size_for_most_recent" zstash_demo 2>&1 | tee create.log
    grep "INFO: TESTING/DEBUGGING ONLY: Simulating bad size for most recent entry for 000000.tar." create.log
    if [ $? != 0 ]; then
        ((fail_count++))
        review_str+="${case_name}_create/create.log,"
    else
        ((success_count++))
    fi
    cd ${src_prefix}_check
    zstash check --hpss=${DST_DIR}/${case_name} 2>&1 | tee check.log
    grep "WARNING: Database corruption detected! Found 2 database entries for 000000.tar, with sizes" check.log
    if [ $? != 0 ]; then
        ((fail_count++))
        review_str+="${case_name}_check/check.log,"
    else
        ((success_count++))
    fi
    grep "INFO: 000000.tar: A database entry matches the actual file size," check.log
    if [ $? != 0 ]; then
        ((fail_count++))
    else
        ((success_count++))
    fi


    # Summary
    echo "Success count: ${success_count}"
    echo "Fail count: ${fail_count}"
    echo "Review: ${review_str}"
}

run_test_cases "$1"

# Success count: 25
# Fail count: 0
# Review:
