#!/bin/bash

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
  test_dir=/global/homes/f/forsyth/zstash/tests
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
  ln -s ${test_dir}/${non_archived_dir}/file2.txt zstash_demo/file4.txt
  cat zstash_demo/file3.txt
  cat zstash_demo/file4.txt
}

zstash_create()
{
  archive_name=$1
  follow_symlinks=$2
  echo "Starting zstash create"
  if [[ "${follow_symlinks}" == "true" ]]; then
      zstash create --hpss=${archive_name} zstash_demo --follow-symlinks
  else
      zstash create --hpss=${archive_name} zstash_demo
  fi
}

zstash_extract()
{
  archive_name=$1
  mkdir zstash_extraction
  cd zstash_extraction
  if [[ "${archive_name}" == "none" ]]; then
    echo "Copying zstash"
    cp -r ../zstash_demo/zstash/ zstash
  fi
  echo "Starting zstash extract"
  zstash extract --hpss=${archive_name}
  cat file3.txt
  cat file4.txt
  echo "> ls"
  ls
  echo "> ls -l"
  ls -l
  echo "> zstash ls"
  zstash ls --hpss=${archive_name}
  echo "> zstash ls -l"
  zstash ls -l --hpss=${archive_name}
  cd ..
}

test_cases()
{
  use_hpss=$1
  follow_symlinks=$2
  if [[ "${use_hpss}" == "true" ]]; then
      archive_name=/home/f/forsyth/zstash_test_follow_symlinks
  else
      archive_name=none
  fi

  case_name="Don't delete original file"
  setup ${use_hpss} ${follow_symlinks} "${case_name}" ${archive_name}
  zstash_create ${archive_name} ${follow_symlinks}
  zstash_extract ${archive_name}

  case_name="Delete before create"
  setup ${use_hpss} ${follow_symlinks} "${case_name}" ${archive_name}
  rm non_archived/file1.txt
  rm ../run_n247_non_archived/file2.txt
  zstash_create ${archive_name} ${follow_symlinks}
  zstash_extract ${archive_name}

  case_name="Delete after create"
  setup ${use_hpss} ${follow_symlinks} "${case_name}" ${archive_name}
  zstash_create ${archive_name} ${follow_symlinks}
  rm non_archived/file1.txt
  rm ../run_n247_non_archived/file2.txt
  zstash_extract ${archive_name}

}

conda_env=zstash_dev_n247
# Set up Conda
source /global/homes/f/forsyth/miniconda3/etc/profile.d/conda.sh
conda activate ${conda_env}
# Install branch
cd /global/homes/f/forsyth/zstash
pip install .
# Begin tests
test_cases true true # HPSS, follow symlinks
test_cases false true # No HPSS, follow symlinks
test_cases true false # HPSS, don't follow symlinks
test_cases false false # No HPSS, don't follow symlinks
