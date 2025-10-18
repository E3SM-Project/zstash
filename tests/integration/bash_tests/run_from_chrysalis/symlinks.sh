# Test symlinks
# Adjusted from https://github.com/E3SM-Project/zstash/issues/341

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

test_cases()
{
  local follow_symlinks=$1
  cd /home/ac.forsyth2/ez/zstash/tests/utils/test_symlinks
  rm -rf workdir workdir2 workdir3
  mkdir workdir workdir2 workdir3
  cd workdir
  mkdir -p src/d1 src/d2
  touch src/d1/large_file.txt

  # This creates a symlink in d2 that links to a file in d1
  # Notice absolute path is used for source
  ln -s /home/ac.forsyth2/ez/zstash/tests/utils/test_symlinks/workdir/src/d1/large_file.txt src/d2/large_file.txt

  echo ""
  echo "ls -l  src/d2"
  case_name="ls_1"
  ls -l  src/d2 2>&1 | tee ${case_name}.log
  # symlink
  check_log_has "large_file.txt -> /home/ac.forsyth2/ez/zstash/tests/utils/test_symlinks/workdir/src/d1/large_file.txt" ${case_name}.log

  echo ""
  case_name="create"
  if [[ "${follow_symlinks,,}" == "true" ]]; then
    echo "zstash create --hpss=none --follow-symlinks --cache /home/ac.forsyth2/ez/zstash/tests/utils/test_symlinks/workdir2 src/d2"
    zstash create --hpss=none --follow-symlinks --cache /home/ac.forsyth2/ez/zstash/tests/utils/test_symlinks/workdir2 src/d2  2>&1 | tee ${case_name}.log
  else
    echo "zstash create --hpss=none --cache /home/ac.forsyth2/ez/zstash/tests/utils/test_symlinks/workdir2 src/d2"
    zstash create --hpss=none --cache /home/ac.forsyth2/ez/zstash/tests/utils/test_symlinks/workdir2 src/d2 2>&1 | tee ${case_name}.log
  fi
  check_log_has "Adding 000000.tar" ${case_name}.log

  echo ""
  echo "ls -l  src/d2"
  case_name="ls_2"
  ls -l  src/d2 2>&1 | tee ${case_name}.log
  # symlink (src is unaffected)
  check_log_has "large_file.txt -> /home/ac.forsyth2/ez/zstash/tests/utils/test_symlinks/workdir/src/d1/large_file.txt" ${case_name}.log

  cd ../workdir3
  echo ""
  echo "zstash extract --hpss=none --cache /home/ac.forsyth2/ez/zstash/tests/utils/test_symlinks/workdir2"
  case_name="extract"
  zstash extract --hpss=none --cache /home/ac.forsyth2/ez/zstash/tests/utils/test_symlinks/workdir2  2>&1 | tee ${case_name}.log
  check_log_has "No failures detected when extracting the files" ${case_name}.log

  cd ..
  echo ""
  echo "ls workdir3"
  case_name="ls_3"
  ls workdir3 2>&1 | tee ${case_name}.log
  # large_file.txt
  check_log_has "large_file.txt" ${case_name}.log

  cd /home/ac.forsyth2/ez/zstash/tests/utils/test_symlinks
  rm -rf workdir workdir2 workdir3 ls_3.log

}

test_cases true
test_cases false
