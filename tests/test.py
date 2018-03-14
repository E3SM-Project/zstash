import os
import sys
import subprocess
import shutil

def write_file(name, contents):
    """
    Write contents to a file named name.
    """
    with open(name, 'w') as f:
        f.write(contents)   

def run_cmd(cmd):
    """
    Run a command while printing and returning the stdout and stderr
    """
    print('+ {}'.format(cmd))
    p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, err = p.communicate()
    print(output)
    print(err)
    return output, err

def str_not_in(output, msg):
    """
    If the msg is not in the output string, then everything is fine.
    """
    if msg in output:
        print('*'*40)
        print('This was not supposed to be found: {}',format(msg))
        print('*'*40)
        exit()

def str_in(output, msg):
    """
    If the msg is in the output string, then the everything is fine.
    """
    if not msg in output:
        print('*'*40)
        print('This was supposed to be found, but was not: {}'.format(msg))
        print('*'*40)
        exit()

def cleanup():
    """
    After this script is ran, remove all created files, even those on the HPSS repo.
    """
    print('Removing test files, both locally and at the HPSS repo')
    shutil.rmtree('zstash_test')
    cmd = 'hsi rm -R {}'.format(HPSS_PATH)
    run_cmd(cmd)

def exit():
    """
    Cleanup and stop running this script
    """
    cleanup()
    sys.exit()


# TODO: Change the hpss directory to a dir that's accessable to everyone
HPSS_PATH='/home/z/zshaheen/zstash_test'


# Create files and directories
print('Creating files.')
if not os.path.exists('zstash_test'):
    os.mkdir('zstash_test')
if not os.path.exists('zstash_test/empty_dir'):
    os.mkdir('zstash_test/empty_dir')
if not os.path.exists('zstash_test/dir'):
    os.mkdir('zstash_test/dir')

write_file('zstash_test/file0.txt', 'file0 stuff')
write_file('zstash_test/file_empty.txt', '')
write_file('zstash_test/dir/file1.txt', 'file1 stuff')

# TODO: symlinks don't seem to work
#if not os.path.lexists('zstash_test/file0_soft.txt'):
#    os.symlink('zstash_test/file0.txt', 'zstash_test/file0_soft.txt')

if not os.path.lexists('zstash_test/file0_hard.txt'):
    os.link('zstash_test/file0.txt', 'zstash_test/file0_hard.txt')

print('Adding files to HPSS')
cmd = 'zstash create --hpss={} zstash_test'.format(HPSS_PATH)
output, err = run_cmd(cmd)
str_in(output+err, 'Transferring file to HPSS')

# TODO: Nothing should happen, but stuff does happen
print('Running update on the newly created directory')
print('Nothing should happen')
os.chdir('zstash_test')
cmd = 'zstash update --hpss={}'.format(HPSS_PATH)
output, err = run_cmd(cmd)
os.chdir('../')
str_in(output+err, 'Nothing to update')


cleanup()

'''
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
#rm -r zstash_test
#rm -r zstash_test_backup
# TODO: This should be removed soon, not good
# rm -r zstash
hsi "rm -R $HPSS_PATH"
'''
