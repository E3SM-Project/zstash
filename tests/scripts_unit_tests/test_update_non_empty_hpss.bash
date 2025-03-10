#!/bin/bash

hpss_path=zstash_test # Set via `HPSS_ARCHIVE = "zstash_test"`
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

# base.add_files ##############################################################
echo "Testing update with an actual change"
mkdir -p ${test_dir}/dir2
echo "file2 stuff" > ${test_dir}/dir2/file2.txt
echo "file1 stuff with changes" > ${test_dir}/dir/file1.txt
cd ${test_dir}
zstash update -v --hpss=${hpss_path}
# Archives 000001.tar
cd ..
echo "Cache:"
ls -l ${test_dir}/${cache} # just index.db
echo "HPSS:"
hsi ls -l ${hpss_path} # 000000.tar, 000001.tar, index.db

echo "Adding more files to the HPSS archive."
echo "file3 stuff" > ${test_dir}/file3.txt
cd ${test_dir}
zstash update --hpss=${hpss_path}
# Archives 000002.tar
cd ..
echo "Cache:"
ls -l ${test_dir}/${cache} # just index.db
echo "HPSS:"
hsi ls -l ${hpss_path} # 000000.tar, 000001.tar, 000002.tar, index.db

echo "file4 stuff" > ${test_dir}/file4.txt
cd ${test_dir}
zstash update --hpss=${hpss_path}
# Archives 000003.tar
cd ..
echo "Cache:"
ls -l ${test_dir}/${cache} # just index.db
echo "HPSS:"
hsi ls -l ${hpss_path} # 000000.tar, 000001.tar, 000002.tar, 000003.tar, index.db

echo "file5 stuff" > ${test_dir}/file5.txt
cd ${test_dir}
zstash update --hpss=${hpss_path}
# Archives 000004.tar
cd ..
echo "Cache:"
ls -l ${test_dir}/${cache} # just index.db
echo "HPSS:"
hsi ls -l ${hpss_path} # 000000.tar, 000001.tar, 000002.tar, 000003.tar, 000004.tar, index.db

# back in test_update.helperUpdateNonEmpty ####################################
echo "Cache check actually performed in the unit test:"
ls -l ${test_dir}/${cache} # just index.db

# base.tearDown ###############################################################
echo "Removing test files, both locally and at the HPSS repo"
rm -rf ${test_dir}
hsi rm -R ${hpss_path}
