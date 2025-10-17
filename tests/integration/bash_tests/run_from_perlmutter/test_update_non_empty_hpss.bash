#!/bin/bash

hpss_path=zstash_test # Set via `HPSS_ARCHIVE = "zstash_test"`
cache=zstash # Set via `self.cache = "zstash"`

check_log_has()
{
    local expected_grep="${1}"
    local log_file="${2}"
    grep "${expected_grep}" ${log_file}
    if [ $? != 0 ]; then
        echo "Expected grep '${expected_grep}' not found in ${log_file}. Test failed."
        exit 1
    fi
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
echo "HPSS:"
hsi ls -l ${hpss_path} 2>&1 | tee ${case_name}_hpss.log
check_log_has "000000.tar" ${case_name}_hpss.log
check_log_has "index.db" ${case_name}_hpss.log

# base.add_files ##############################################################
echo "Testing update with an actual change"
mkdir -p ${test_dir}/dir2
echo "file2 stuff" > ${test_dir}/dir2/file2.txt
echo "file1 stuff with changes" > ${test_dir}/dir/file1.txt
cd ${test_dir}
case_name="update1"
zstash update -v --hpss=${hpss_path} 2>&1 | tee ${case_name}.log
check_log_has "INFO: Adding 000001.tar" ${case_name}.log
cd ..
echo "Cache:"
ls -l ${test_dir}/${cache} 2>&1 | tee ${case_name}_cache.log
check_log_has "index.db" ${case_name}_cache.log
echo "HPSS:"
hsi ls -l ${hpss_path} 2>&1 | tee ${case_name}_hpss.log
check_log_has "000000.tar" ${case_name}_hpss.log
check_log_has "000001.tar" ${case_name}_hpss.log
check_log_has "index.db" ${case_name}_hpss.log

echo "Adding more files to the HPSS archive."
echo "file3 stuff" > ${test_dir}/file3.txt
cd ${test_dir}
case_name="update2"
zstash update --hpss=${hpss_path} 2>&1 | tee ${case_name}.log
check_log_has "INFO: Adding 000002.tar" ${case_name}.log
cd ..
echo "Cache:"
ls -l ${test_dir}/${cache} 2>&1 | tee ${case_name}_cache.log
check_log_has "index.db" ${case_name}_cache.log
echo "HPSS:"
hsi ls -l ${hpss_path} 2>&1 | tee ${case_name}_hpss.log
check_log_has "000000.tar" ${case_name}_hpss.log
check_log_has "000001.tar" ${case_name}_hpss.log
check_log_has "000002.tar" ${case_name}_hpss.log
check_log_has "index.db" ${case_name}_hpss.log

echo "file4 stuff" > ${test_dir}/file4.txt
cd ${test_dir}
case_name="update3"
zstash update --hpss=${hpss_path} 2>&1 | tee ${case_name}.log
check_log_has "INFO: Adding 000003.tar" ${case_name}.log
cd ..
echo "Cache:"
ls -l ${test_dir}/${cache} 2>&1 | tee ${case_name}_cache.log
check_log_has "index.db" ${case_name}_cache.log
echo "HPSS:"
hsi ls -l ${hpss_path} 2>&1 | tee ${case_name}_hpss.log
check_log_has "000000.tar" ${case_name}_hpss.log
check_log_has "000001.tar" ${case_name}_hpss.log
check_log_has "000002.tar" ${case_name}_hpss.log
check_log_has "000003.tar" ${case_name}_hpss.log
check_log_has "index.db" ${case_name}_hpss.log

echo "file5 stuff" > ${test_dir}/file5.txt
cd ${test_dir}
case_name="update4"
zstash update --hpss=${hpss_path} 2>&1 | tee ${case_name}.log
check_log_has "INFO: Adding 000004.tar" ${case_name}.log
cd ..
echo "Cache:"
ls -l ${test_dir}/${cache} 2>&1 | tee ${case_name}_cache.log
check_log_has "index.db" ${case_name}_cache.log
echo "HPSS:"
hsi ls -l ${hpss_path} 2>&1 | tee ${case_name}_hpss.log
check_log_has "000000.tar" ${case_name}_hpss.log
check_log_has "000001.tar" ${case_name}_hpss.log
check_log_has "000002.tar" ${case_name}_hpss.log
check_log_has "000003.tar" ${case_name}_hpss.log
check_log_has "000004.tar" ${case_name}_hpss.log
check_log_has "index.db" ${case_name}_hpss.log

# back in test_update.helperUpdateNonEmpty ####################################
echo "Cache check actually performed in the unit test:"
case_name="ls"
ls -l ${test_dir}/${cache} 2>&1 | tee ${case_name}.log
check_log_has "index.db" ${case_name}.log

# base.tearDown ###############################################################
echo "Removing test files, both locally and at the HPSS repo"
rm -rf ${test_dir}
rm create.log
rm create_*.log
rm ls.log
rm update*.log
hsi rm -R ${hpss_path}
