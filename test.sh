# You must run this from the zstash/tests directory

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
# TODO: Change the hpss directory to a dir that's accessable to everyone
python zstash create --hpss=/home/z/zshaheen/zstash_test zstash_test

rm -r zstash_test


