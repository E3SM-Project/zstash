#!/bin/bash

hpss_globus_endpoint="6c54cade-bde5-45c1-bdea-f4bd71dba2cc"
hpss_path="globus://${hpss_globus_endpoint}/~/zstash_test/"
cache=zstash # Set via `self.cache = "zstash"`

# base.setupDirs ##############################################################
use_hpss=true
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
zstash create --hpss=${hpss_path} ${test_dir}
# Archives 000000.tar
echo "Cache:"
ls -l ${test_dir}/${cache} # just index.db
echo "HPSS:"
hsi ls -l ${hpss_path} # 000000.tar, index.db

# back in test_globus.helperLsGlobus ##########################################
cd ${test_dir}
zstash ls --hpss=${hpss_path}
cd ..
echo "Cache:"
ls -l ${test_dir}/${cache}
echo "HPSS:"
hsi ls -l ${hpss_path}
