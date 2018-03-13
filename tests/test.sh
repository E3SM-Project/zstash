# TODO: Change the hpss directory to a dir that's accessable to everyone
HPSS_PATH='/home/z/zshaheen/zstash_test'

# Create files and directories
mkdir zstash_test
mkdir zstash_test/empty_dir
mkdir zstash_test/dir

echo "file0 stuff" >> zstash_test/file0.txt
echo "" >> zstash_test/file_empty.txt
echo "file1 stuff" >> zstash_test/dir/file1.txt
# TODO: symlinks don't seem to work
# ln -s zstash_test/file0.txt zstash_test/file0_soft.txt
ln zstash_test/file0.txt zstash_test/file0_hard.txt

# Adding the files and directory to HPSS
zstash create --hpss=$HPSS_PATH zstash_test

# Nothing should happen
# ERROR: STUFF ACTUALLY DOES HAPPEN
# It archives file0_hard.txt  again
echo "Nothing should happen"
cd zstash_test
zstash update --hpss=$HPSS_PATH
cd ../

# Testing update with an actual change
mkdir zstash_test/dir2
echo "file2 stuff" >> zstash_test/dir2/file2.txt
# zstash update --hpss=/home/z/zshaheen/zstash_test zstash_test/dir2
cd zstash_test
zstash update --hpss=$HPSS_PATH
cd ../

# Testing extract functionality
mv zstash_test zstash_test_backup
mkdir zstash_test
cd zstash_test
zstash extract --hpss=$HPSS_PATH
cd ../

# Testing update, nothing should happen
# And nothing does happen, this is good
echo "Nothing should happen"
cd zstash_test
zstash update --hpss=$HPSS_PATH
cd ../


echo "Verifying the data from database with the actual files"
# Check that zstash_test/index.db matches the stuff from zstash_backup/*
echo "Checksums from HPSS"
sqlite3 zstash_test/zstash/index.db "select md5, name from files;" | sort -n
echo "Checksums from local files"
find zstash_test_backup/* -regex ".*\.txt.*" -exec md5sum {} + | sort -n

# Cleanup
rm -r zstash_test
rm -r zstash_test_backup
# TODO: This should be removed soon, not good
# rm -r zstash
hsi "rm -R $HPSS_PATH"

