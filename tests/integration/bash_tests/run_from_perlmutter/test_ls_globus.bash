#!/bin/bash

hpss_globus_endpoint="6c54cade-bde5-45c1-bdea-f4bd71dba2cc"
hpss_path="globus://${hpss_globus_endpoint}/~/zstash_test/"
cache=zstash # Set via `self.cache = "zstash"`

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

# base.setupDirs ##############################################################
test_dir=zstash_test

# Create files and directories
echo "Creating files."
mkdir -p ${test_dir}
mkdir -p ${test_dir}/empty_dir
mkdir -p ${test_dir}/dir

echo "file0 stuff" > ${test_dir}/file0.txt
echo "" > ${test_dir}/file_empty.txt
echo "file1 stuff" > ${test_dir}/dir/file1.txt

# Symbolic (soft) link (points to a file name which points to an inode)
# ${test_dir}/file_0_soft.txt points to ${test_dir}/file0.txt
# The python `os.symlink` call omits the first `test_dir`
# because it simply looks in the same directory for the file to link to.
ln -s ${test_dir}/file0.txt ${test_dir}/file_0_soft.txt
# Bad symbolic (soft) link (points to a file name which points to an inode)
ln -s ${test_dir}/file0_that_doesnt_exist.txt ${test_dir}/file0_soft_bad.txt
# Hard link (points to an inode directly)
ln -s ${test_dir}/file0.txt ${test_dir}/file0_hard.txt

# base.create #################################################################
echo "Adding files to HPSS"
case_name="create"
zstash create --hpss=${hpss_path} ${test_dir} 2>&1 | tee ${case_name}.log
check_log_has "INFO: Adding 000000.tar" ${case_name}.log
echo "Cache:"
ls -l ${test_dir}/${cache} 2>&1 | tee ${case_name}_cache.log
check_log_has "index.db" ${case_name}_cache.log
# NOTE:
# It appears that we can't access the HPSS path directly with `hsi`.
# That's presumably because we're using the Globus tutorial endpoint
# rather than the NERSC HPSS endpoint.

# back in test_globus.helperLsGlobus ##########################################
cd ${test_dir}
case_name="ls"
zstash ls --hpss=${hpss_path} 2>&1 | tee ${case_name}.log
check_log_has \
    "file0.txt" \
    "file0_hard.txt" \
    "file0_soft_bad.txt" \
    "file_0_soft.txt" \
    "file_empty.txt" \
    "dir/file1.txt" \
    "empty_dir" \
    ${case_name}.log
cd ..
echo "Cache:"
ls -l ${test_dir}/${cache} 2>&1 | tee ${case_name}_cache.log
check_log_has "index.db" ${case_name}_cache.log

rm -rf ${test_dir} # Cleanup
rm create.log
rm create_cache.log
rm ls_cache.log
