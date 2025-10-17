#!/bin/bash

# From Claude:
check_log_has()
{
    local log_file="${@: -1}"  # Last argument is the log file
    local patterns=("${@:1:$#-1}")  # All but last argument are patterns

    for pattern in "${patterns[@]}"; do
        if ! grep -q "${pattern}" "${log_file}"; then
            echo "Expected grep '${pattern}' not found in ${log_file}. Test failed."
            exit 1
        fi
    done
}

setup()
{
  echo "##########################################################################################################"
  use_hpss=$1
  follow_symlinks=$2
  case_name="${3}"
  archive_name=$4
  if [[ "${use_hpss}" == "true" ]]; then
      hsi rm -R ${archive_name}
  fi
  echo "use_hpss=${use_hpss}"
  echo "follow_symlinks=${follow_symlinks}"
  echo "case_name=${case_name}"
  local_archive_name=test_follow_symlinks
  non_archived_dir=${local_archive_name}_non_archived
  test_dir=/global/homes/f/forsyth/ez/zstash/tests/utils/
  cd ${test_dir}
  rm -rf ${local_archive_name}
  rm -rf ${non_archived_dir}
  mkdir ${local_archive_name}
  # At the same level as local_archive_name
  mkdir ${non_archived_dir}
  cd ${local_archive_name}

  mkdir zstash_demo
  mkdir zstash_demo/empty_dir
  mkdir zstash_demo/dir
  mkdir non_archived
  echo -n '' > zstash_demo/file_empty.txt
  echo 'file0 stuff' > zstash_demo/dir/file0.txt
  echo 'file1 stuff' > non_archived/file1.txt
  echo 'file2 stuff' > ../${non_archived_dir}/file2.txt
  # NOTE: `ln -s` appears to require absolute paths for the source files
  ln -s ${test_dir}/${local_archive_name}/non_archived/file1.txt zstash_demo/file3.txt
  check_log_has "file1 stuff" zstash_demo/file3.txt
  ln -s ${test_dir}/${non_archived_dir}/file2.txt zstash_demo/file4.txt
  check_log_has "file2 stuff" zstash_demo/file4.txt
}

zstash_create()
{
  archive_name=$1
  follow_symlinks=$2
  echo "Starting zstash create from:"
  pwd
  if [[ "${follow_symlinks}" == "true" ]]; then
      zstash create --hpss=${archive_name} zstash_demo --follow-symlinks
  else
      zstash create --hpss=${archive_name} zstash_demo
  fi
}

zstash_extract()
{
  archive_name=$1
  rm -rf zstash_extraction
  mkdir zstash_extraction
  cd zstash_extraction
  if [[ "${archive_name}" == "none" ]]; then
    echo "Copying zstash"
    cp -r ../zstash_demo/zstash/ zstash
  fi
  echo "Starting zstash extract from:"
  pwd
  zstash extract --hpss=${archive_name}
  echo "> ls"
  ls 2>&1 | tee out_ls.txt
  echo "> ls -l"
  ls -l  2>&1 | tee out_ls_l.txt
  echo "> zstash ls"
  zstash ls --hpss=${archive_name} 2>&1 | tee out_zstash_ls.txt
  echo "> zstash ls -l"
  zstash ls -l --hpss=${archive_name} 2>&1 | tee out_zstash_ls_l.txt
  cd ..
}

test_cases()
{
  test_num=$1
  use_hpss=$2
  follow_symlinks=$3
  if [[ "${use_hpss}" == "true" ]]; then
      archive_name=/home/f/forsyth/zstash_test_follow_symlinks
  else
      archive_name=none
  fi

  echo "##########################################################################################################"
  echo "Test ${test_num}: use_hpss=${use_hpss}, follow_symlinks=${follow_symlinks}"
  case_name="Case ${test_num}.1: Don't delete original file"
  setup ${use_hpss} ${follow_symlinks} "${case_name}" ${archive_name}
  zstash_create ${archive_name} ${follow_symlinks} 2>&1 | tee case_${test_num}.1_create.txt
  check_log_has \
    "Archiving file3.txt" \
    "Archiving file4.txt" \
    "Archiving file_empty.txt" \
    "Archiving dir/file0.txt" \
    "Archiving empty_dir" \
    "Completed archive file 000000.tar" \
    case_${test_num}.1_create.txt
  zstash_extract ${archive_name} 2>&1 | tee case_${test_num}.1_extract.txt
  check_log_has \
    "Extracting file3.txt" \
    "Extracting file4.txt" \
    "Extracting file_empty.txt" \
    "Extracting dir/file0.txt" \
    "Extracting empty_dir" \
    "No failures detected when extracting the files." \
    case_${test_num}.1_extract.txt
  check_log_has "file1 stuff" zstash_extraction/file3.txt
  check_log_has "file2 stuff" zstash_extraction/file4.txt
  check_log_has \
    "dir" \
    "empty_dir" \
    "file3.txt" \
    "file4.txt" \
    "file_empty.txt" \
    "zstash" \
    zstash_extraction/out_ls.txt
  check_log_has \
    "dir" \
    "empty_dir" \
    "file3.txt" \
    "file4.txt" \
    "file_empty.txt" \
    "zstash" \
    zstash_extraction/out_ls_l.txt
  check_log_has \
    "file3.txt" \
    "file4.txt" \
    "file_empty.txt" \
    "dir/file0.txt" \
    "empty_dir" \
    zstash_extraction/out_zstash_ls.txt
  check_log_has \
    "file3.txt" \
    "file4.txt" \
    "file_empty.txt" \
    "dir/file0.txt" \
    "empty_dir" \
    "000000.tar" \
    zstash_extraction/out_zstash_ls_l.txt

  case_name="Case ${test_num}.2: Delete before create"
  setup ${use_hpss} ${follow_symlinks} "${case_name}" ${archive_name}
  rm non_archived/file1.txt # Remove the file that file3 links to
  zstash_create ${archive_name} ${follow_symlinks} 2>&1 | tee case_${test_num}.2_create.txt
  if [ "${follow_symlinks}" = "true" ]; then
    check_log_has \
      "Archiving file3.txt" \
      "FileNotFoundError" \
      "ERROR: Archiving file3.txt" \
      "Exception: Archive creation failed due to broken symlink." \
      case_${test_num}.2_create.txt
  else
    check_log_has \
      "Archiving file3.txt" \
      "Archiving file4.txt" \
      "Archiving file_empty.txt" \
      "Archiving dir/file0.txt" \
      "Archiving empty_dir" \
      "Completed archive file 000000.tar" \
      case_${test_num}.2_create.txt
    zstash_extract ${archive_name} 2>&1 | tee case_${test_num}.2_extract.txt
    check_log_has \
      "Extracting file3.txt" \
      "Extracting file4.txt" \
      "Extracting file_empty.txt" \
      "Extracting dir/file0.txt" \
      "Extracting empty_dir" \
      "No failures detected when extracting the files." \
      case_${test_num}.2_extract.txt
    if [ -f "zstash_extraction/file3.txt" ]; then
        echo "zstash_extraction/file3.txt exists, but it should not because the file it links to was deleted."
        exit 2
    fi
    check_log_has "file2 stuff" zstash_extraction/file4.txt
    check_log_has \
      "dir" \
      "empty_dir" \
      "file3.txt" \
      "file4.txt" \
      "file_empty.txt" \
      "zstash" \
      zstash_extraction/out_ls.txt
    check_log_has \
      "dir" \
      "empty_dir" \
      "file3.txt" \
      "file4.txt" \
      "file_empty.txt" \
      "zstash" \
      zstash_extraction/out_ls_l.txt
    check_log_has \
      "file3.txt" \
      "file4.txt" \
      "file_empty.txt" \
      "dir/file0.txt" \
      "empty_dir" \
      zstash_extraction/out_zstash_ls.txt
    check_log_has \
      "file3.txt" \
      "file4.txt" \
      "file_empty.txt" \
      "dir/file0.txt" \
      "empty_dir" \
      "000000.tar" \
      zstash_extraction/out_zstash_ls_l.txt
  fi

  case_name="Case ${test_num}.3: Delete after create"
  setup ${use_hpss} ${follow_symlinks} "${case_name}" ${archive_name}
  zstash_create ${archive_name} ${follow_symlinks} 2>&1 | tee case_${test_num}.3_create.txt
  check_log_has \
    "Archiving file3.txt" \
    "Archiving file4.txt" \
    "Archiving file_empty.txt" \
    "Archiving dir/file0.txt" \
    "Archiving empty_dir" \
    "Completed archive file 000000.tar" \
    case_${test_num}.3_create.txt
  rm non_archived/file1.txt
  zstash_extract ${archive_name} 2>&1 | tee case_${test_num}.3_extract.txt
  check_log_has \
    "Extracting file3.txt" \
    "Extracting file4.txt" \
    "Extracting file_empty.txt" \
    "Extracting dir/file0.txt" \
    "Extracting empty_dir" \
    "No failures detected when extracting the files." \
    case_${test_num}.3_extract.txt
  if [ "${follow_symlinks}" = "true" ]; then
    check_log_has "file1 stuff" zstash_extraction/file3.txt
  else
    if [ -f "zstash_extraction/file3.txt" ]; then
        echo "zstash_extraction/file3.txt exists, but it should not because the file it links to was deleted."
        exit 3
    fi
  fi
  check_log_has "file2 stuff" zstash_extraction/file4.txt
  check_log_has \
    "dir" \
    "empty_dir" \
    "file3.txt" \
    "file4.txt" \
    "file_empty.txt" \
    "zstash" \
    zstash_extraction/out_ls.txt
  check_log_has \
    "dir" \
    "empty_dir" \
    "file3.txt" \
    "file4.txt" \
    "file_empty.txt" \
    "zstash" \
    zstash_extraction/out_ls_l.txt
  check_log_has \
    "file3.txt" \
    "file4.txt" \
    "file_empty.txt" \
    "dir/file0.txt" \
    "empty_dir" \
    zstash_extraction/out_zstash_ls.txt
  check_log_has \
    "file3.txt" \
    "file4.txt" \
    "file_empty.txt" \
    "dir/file0.txt" \
    "empty_dir" \
    "000000.tar" \
    zstash_extraction/out_zstash_ls_l.txt

}

# Begin tests
test_cases 1 true true # HPSS, follow symlinks
test_cases 2 false true # No HPSS, follow symlinks
test_cases 3 true false # HPSS, don't follow symlinks
test_cases 4 false false # No HPSS, don't follow symlinks
