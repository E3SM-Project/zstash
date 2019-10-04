"""
Run the test suite like:
    python test.py
You'll get a statement printed
if all of the tests pass.
"""

import os
import sys
import subprocess
import shutil
from collections import Counter

def write_file(name, contents):
    """
    Write contents to a file named name.
    """
    with open(name, 'w') as f:
        f.write(contents)   

def run_cmd(cmd):
    """
    Run a command while printing and returning the stdout and stderr.
    """
    print('+ {}'.format(cmd))
    if isinstance(cmd, str):
        cmd = cmd.split()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, err = p.communicate()

    # When running in Python 3, the output of subprocess.Popen.communicate()
    # is a bytes object. We need to convert it to a string.
    if isinstance(output, bytes):
        output = output.decode("utf-8")
    if isinstance(err, bytes):
        err = err.decode("utf-8")

    print(output)
    print(err)
    return output, err

def str_not_in(output, msg):
    """
    If the msg is not in the output string, then everything is fine.
    """
    if msg in output:
        print('*'*40)
        print('This was not supposed to be found: {}'.format(msg))
        print('*'*40)
        stop()

def str_in(output, msg):
    """
    If the msg is in the output string, then the everything is fine.
    """
    if not msg in output:
        print('*'*40)
        print('This was supposed to be found, but was not: {}'.format(msg))
        print('*'*40)
        stop()

def cleanup():
    """
    After this script is ran, remove all created files, even those on the HPSS repo.
    """
    print('Removing test files, both locally and at the HPSS repo')
    if os.path.exists('zstash_test'):
        shutil.rmtree('zstash_test')
    if os.path.exists('zstash_test_backup'):
        shutil.rmtree('zstash_test_backup')
    if os.path.exists('zstash'):
        shutil.rmtree('zstash')
    cmd = 'hsi rm -R {}'.format(HPSS_PATH)
    run_cmd(cmd)

def stop():
    """
    Cleanup and stop running this script.
    """
    cleanup()
    sys.exit()

# Compare content of two (unordered lists)
# https://stackoverflow.com/questions/7828867/how-to-efficiently-compare-two-unordered-lists-not-sets-in-python
def compare(s, t):
    return Counter(s) == Counter(t)

# Makes this in the home dir of the user on HPSS.
# Ex: /home/z/zshaheen/zstash_test
HPSS_PATH='zstash_test'

# Create files and directories
for option in ['-v', '']:
    print('Creating files {}.'.format(option))
    cleanup()
    os.mkdir('zstash_test')
    os.mkdir('zstash_test/empty_dir')
    os.mkdir('zstash_test/dir')

    write_file('zstash_test/file0.txt', 'file0 stuff')
    write_file('zstash_test/file_empty.txt', '')
    write_file('zstash_test/dir/file1.txt', 'file1 stuff')

    if not os.path.lexists('zstash_test/file0_soft.txt'):
        # If we symlink zstash_test/file0_soft.txt to zstash_test/file0.txt
        # zstash_test/file0_soft.txt links to zstash_test/zstash_test/file0.txt
        os.symlink('file0.txt', 'zstash_test/file0_soft.txt')

    if not os.path.lexists('zstash_test/file0_soft_bad.txt'):
        # If we symlink zstash_test/file0_soft.txt to zstash_test/file0.txt
        # zstash_test/file0_soft.txt links to zstash_test/zstash_test/file0.txt
        os.symlink('file0_that_doesnt_exist.txt', 'zstash_test/file0_soft_bad.txt')

    if not os.path.lexists('zstash_test/file0_hard.txt'):
        os.link('zstash_test/file0.txt', 'zstash_test/file0_hard.txt')


    print('Adding files to HPSS')
    cmd = 'zstash create {} --hpss={} zstash_test'.format(option, HPSS_PATH)
    output, err = run_cmd(cmd)
    str_in(output+err, 'Transferring file to HPSS')
    str_not_in(output+err, 'ERROR')


print('Testing chgrp')
GROUP = 'acme'
for option in ['-v', '']:
    print('Running zstash chgrp {}'.format(option))
    cmd = 'zstash chgrp {} -R {} {}'.format(option, GROUP, HPSS_PATH)
    output, err = run_cmd(cmd)
    str_not_in(output+err, 'ERROR')
    print('Now check that the files are in the {} group'.format(GROUP))
    cmd = 'hsi ls -l {}'.format(HPSS_PATH)
    output, err = run_cmd(cmd)
    str_in(output+err, 'acme')
    str_not_in(output+err, 'ERROR')


print('Running update on the newly created directory, nothing should happen')
os.chdir('zstash_test')
cmd = 'zstash update -v --hpss={}'.format(HPSS_PATH)
output, err = run_cmd(cmd)
os.chdir('../')
str_in(output+err, 'Nothing to update')
str_not_in(output+err, 'ERROR')


print('Testing update with an actual change')
if not os.path.exists('zstash_test/dir2'):
    os.mkdir('zstash_test/dir2')
write_file('zstash_test/dir2/file2.txt', 'file2 stuff')
write_file('zstash_test/dir/file1.txt', 'file1 stuff with changes')

os.chdir('zstash_test')
cmd = 'zstash update -v --hpss={}'.format(HPSS_PATH)
output, err = run_cmd(cmd)
os.chdir('../')
str_in(output+err, 'Transferring file to HPSS')
str_not_in(output+err, 'ERROR')
# Make sure none of the old files are moved.
str_not_in(output+err, 'file0')
str_not_in(output+err, 'file_empty')
str_not_in(output+err, 'empty_dir')
str_not_in(output+err, 'ERROR')


print('Adding many more files to the HPSS archive.')
msg = 'This is because we need many separate tar archives'
msg += ' for testing zstash extract/check with parallel.'
print(msg)
write_file('zstash_test/file3.txt', 'file3 stuff')
os.chdir('zstash_test')
cmd = 'zstash update --hpss={}'.format(HPSS_PATH)
output, err = run_cmd(cmd)
os.chdir('../')
write_file('zstash_test/file4.txt', 'file4 stuff')
os.chdir('zstash_test')
cmd = 'zstash update --hpss={}'.format(HPSS_PATH)
output, err = run_cmd(cmd)
os.chdir('../')
write_file('zstash_test/file5.txt', 'file5 stuff')
os.chdir('zstash_test')
cmd = 'zstash update --hpss={}'.format(HPSS_PATH)
output, err = run_cmd(cmd)
os.chdir('../')


for option in ['', '-v', '-l']:
    print('Testing zstash ls {}'.format(option))
    cmd = 'zstash ls {} --hpss={}'.format(option, HPSS_PATH)
    output, err = run_cmd(cmd)
    str_in(output+err, 'file0.txt')
    str_not_in(output+err, 'ERROR')


print('Testing the checking functionality')
cmd = 'zstash check --hpss={}'.format(HPSS_PATH)
output, err = run_cmd(cmd)
str_in(output+err, 'Checking file0.txt')
str_in(output+err, 'Checking file0_hard.txt')
str_in(output+err, 'Checking file0_soft.txt')
str_in(output+err, 'Checking file_empty.txt')
str_in(output+err, 'Checking dir/file1.txt')
str_in(output+err, 'Checking empty_dir')
str_in(output+err, 'Checking dir2/file2.txt')
str_in(output+err, 'Checking file3.txt')
str_in(output+err, 'Checking file4.txt')
str_in(output+err, 'Checking file5.txt')
str_not_in(output+err, 'ERROR')
cmd = 'zstash check -v --hpss={}'.format(HPSS_PATH)
output, err = run_cmd(cmd)
str_in(output+err, 'Checking file0.txt')
str_in(output+err, 'Checking file0_hard.txt')
str_in(output+err, 'Checking file0_soft.txt')
str_in(output+err, 'Checking file_empty.txt')
str_in(output+err, 'Checking dir/file1.txt')
str_in(output+err, 'Checking empty_dir')
str_in(output+err, 'Checking dir2/file2.txt')
str_in(output+err, 'Checking file3.txt')
str_in(output+err, 'Checking file4.txt')
str_in(output+err, 'Checking file5.txt')
str_not_in(output+err, 'ERROR')


print('Testing the extract functionality')
os.rename('zstash_test', 'zstash_test_backup')
os.mkdir('zstash_test')
os.chdir('zstash_test')
cmd = 'zstash extract --hpss={}'.format(HPSS_PATH)
output, err = run_cmd(cmd)
os.chdir('../')
str_in(output+err, 'Extracting file0.txt')
str_in(output+err, 'Extracting file0_hard.txt')
str_in(output+err, 'Extracting file0_soft.txt')
str_in(output+err, 'Extracting file_empty.txt')
str_in(output+err, 'Extracting dir/file1.txt')
str_in(output+err, 'Extracting empty_dir')
str_in(output+err, 'Extracting dir2/file2.txt')
str_in(output+err, 'Extracting file3.txt')
str_in(output+err, 'Extracting file4.txt')
str_in(output+err, 'Extracting file5.txt')
str_not_in(output+err, 'ERROR')
str_not_in(output+err, 'Not extracting')

print('Testing the extract functionality again, nothing should happen')
os.chdir('zstash_test')
cmd = 'zstash extract -v --hpss={}'.format(HPSS_PATH)
output, err = run_cmd(cmd)
# Check that the zstash/ directory is empty.
# It should only contain an 'index.db'.
if not compare(os.listdir('zstash'), ['index.db']):
    print('*'*40)
    print('The zstash directory should not have any tars.')
    print('It has: {}'.format(os.listdir('zstash')))
    print('*'*40)
    stop()
os.chdir('../')
str_in(output+err, 'Not extracting file0.txt')
str_in(output+err, 'Not extracting file0_hard.txt')
# It's okay to extract the symlinks.
str_not_in(output+err, 'Not extracting file0_soft.txt')
str_in(output+err, 'Not extracting file_empty.txt')
str_in(output+err, 'Not extracting dir/file1.txt')
# It's okay to extract empty dirs.
str_not_in(output+err, 'Not extracting empty_dir')
str_in(output+err, 'Not extracting dir2/file2.txt')
str_in(output+err, 'Not extracting file3.txt')
str_in(output+err, 'Not extracting file4.txt')
str_in(output+err, 'Not extracting file5.txt')
str_not_in(output+err, 'ERROR')


msg = 'Deleting the extracted files and doing it again, '
msg += 'while making sure the tars are kept.'
print(msg)
shutil.rmtree('zstash_test')
os.mkdir('zstash_test')
os.chdir('zstash_test')
cmd = 'zstash extract -v --hpss={} --keep'.format(HPSS_PATH)
output, err = run_cmd(cmd)
# Check that the zstash/ directory contains all expected files
if not compare(os.listdir('zstash'), ['index.db', '000000.tar', '000001.tar', '000002.tar', '000003.tar', '000004.tar']):
    print('*'*40)
    print('The zstash directory does not contain expected files')
    print('It has: {}'.format(os.listdir('zstash')))
    print('*'*40)
    stop()
os.chdir('../')
str_in(output+err, 'Transferring from HPSS')
str_in(output+err, 'Extracting file0.txt')
str_in(output+err, 'Extracting file0_hard.txt')
str_in(output+err, 'Extracting file0_soft.txt')
str_in(output+err, 'Extracting file_empty.txt')
str_in(output+err, 'Extracting dir/file1.txt')
str_in(output+err, 'Extracting empty_dir')
str_in(output+err, 'Extracting dir2/file2.txt')
str_in(output+err, 'Extracting file3.txt')
str_in(output+err, 'Extracting file4.txt')
str_in(output+err, 'Extracting file5.txt')
str_not_in(output+err, 'ERROR')
str_not_in(output+err, 'Not extracting')

msg = 'Deleting the extracted files and doing it again without verbose option, '
msg += 'while making sure the tars are kept.'
print(msg)
shutil.rmtree('zstash_test')
os.mkdir('zstash_test')
os.chdir('zstash_test')
cmd = 'zstash extract --hpss={} --keep'.format(HPSS_PATH)
output, err = run_cmd(cmd)
# Check that the zstash/ directory contains all expected files
if not compare(os.listdir('zstash'), ['index.db', '000000.tar', '000001.tar', '000002.tar', '000003.tar', '000004.tar']):
    print('*'*40)
    print('The zstash directory does not contain expected files')
    print('It has: {}'.format(os.listdir('zstash')))
    print('*'*40)
    stop()
os.chdir('../')
str_in(output+err, 'Transferring from HPSS')
str_in(output+err, 'Extracting file0.txt')
str_in(output+err, 'Extracting file0_hard.txt')
str_in(output+err, 'Extracting file0_soft.txt')
str_in(output+err, 'Extracting file_empty.txt')
str_in(output+err, 'Extracting dir/file1.txt')
str_in(output+err, 'Extracting empty_dir')
str_in(output+err, 'Extracting dir2/file2.txt')
str_in(output+err, 'Extracting file3.txt')
str_in(output+err, 'Extracting file4.txt')
str_in(output+err, 'Extracting file5.txt')
str_not_in(output+err, 'ERROR')
str_not_in(output+err, 'Not extracting')


print('Deleting the extracted files and doing it again in parallel.')
shutil.rmtree('zstash_test')
os.mkdir('zstash_test')
os.chdir('zstash_test')
cmd = 'zstash extract -v --hpss={} --workers=3'.format(HPSS_PATH)
output, err = run_cmd(cmd)
os.chdir('../')
str_in(output+err, 'Transferring from HPSS')
str_in(output+err, 'Extracting file0.txt')
str_in(output+err, 'Extracting file0_hard.txt')
str_in(output+err, 'Extracting file0_soft.txt')
str_in(output+err, 'Extracting file_empty.txt')
str_in(output+err, 'Extracting dir/file1.txt')
str_in(output+err, 'Extracting empty_dir')
str_in(output+err, 'Extracting dir2/file2.txt')
str_in(output+err, 'Extracting file3.txt')
str_in(output+err, 'Extracting file4.txt')
str_in(output+err, 'Extracting file5.txt')
str_not_in(output+err, 'ERROR')
str_not_in(output+err, 'Not extracting')
# Checking that the printing was done in order.
tar_order = []
console_output = output+err
for word in console_output.replace('\n', ' ').split(' '):
    if '.tar' in word:
        word = word.replace('zstash/', '')
        tar_order.append(word)
if tar_order != sorted(tar_order):
    print('*'*40)
    print('The tars were printed in this order: {}'.format(tar_order))
    print('When it should have been in this order: {}'.format(sorted(tar_order)))
    print('*'*40)
    stop()

print('Deleting the extracted files and doing it again in parallel without verbose option.')
shutil.rmtree('zstash_test')
os.mkdir('zstash_test')
os.chdir('zstash_test')
cmd = 'zstash extract --hpss={} --workers=3'.format(HPSS_PATH)
output, err = run_cmd(cmd)
os.chdir('../')
str_in(output+err, 'Transferring from HPSS')
str_in(output+err, 'Extracting file0.txt')
str_in(output+err, 'Extracting file0_hard.txt')
str_in(output+err, 'Extracting file0_soft.txt')
str_in(output+err, 'Extracting file_empty.txt')
str_in(output+err, 'Extracting dir/file1.txt')
str_in(output+err, 'Extracting empty_dir')
str_in(output+err, 'Extracting dir2/file2.txt')
str_in(output+err, 'Extracting file3.txt')
str_in(output+err, 'Extracting file4.txt')
str_in(output+err, 'Extracting file5.txt')
str_not_in(output+err, 'ERROR')
str_not_in(output+err, 'Not extracting')
# Checking that the printing was done in order.
tar_order = []
console_output = output+err
for word in console_output.replace('\n', ' ').split(' '):
    if '.tar' in word:
        word = word.replace('zstash/', '')
        tar_order.append(word)
if tar_order != sorted(tar_order):
    print('*'*40)
    print('The tars were printed in this order: {}'.format(tar_order))
    print('When it should have been in this order: {}'.format(sorted(tar_order)))
    print('*'*40)
    stop()


print('Checking the files again in parallel.')
os.chdir('zstash_test')
cmd = 'zstash check -v --hpss={} --workers=3'.format(HPSS_PATH)
output, err = run_cmd(cmd)
os.chdir('../')
str_in(output+err, 'Checking file0.txt')
str_in(output+err, 'Checking file0_hard.txt')
str_in(output+err, 'Checking file0_soft.txt')
str_in(output+err, 'Checking file_empty.txt')
str_in(output+err, 'Checking dir/file1.txt')
str_in(output+err, 'Checking empty_dir')
str_in(output+err, 'Checking dir2/file2.txt')
str_in(output+err, 'Checking file3.txt')
str_in(output+err, 'Checking file4.txt')
str_in(output+err, 'Checking file5.txt')
str_not_in(output+err, 'ERROR')

print('Checking the files again in parallel without verbose option.')
os.chdir('zstash_test')
cmd = 'zstash check --hpss={} --workers=3'.format(HPSS_PATH)
output, err = run_cmd(cmd)
os.chdir('../')
str_in(output+err, 'Checking file0.txt')
str_in(output+err, 'Checking file0_hard.txt')
str_in(output+err, 'Checking file0_soft.txt')
str_in(output+err, 'Checking file_empty.txt')
str_in(output+err, 'Checking dir/file1.txt')
str_in(output+err, 'Checking empty_dir')
str_in(output+err, 'Checking dir2/file2.txt')
str_in(output+err, 'Checking file3.txt')
str_in(output+err, 'Checking file4.txt')
str_in(output+err, 'Checking file5.txt')
str_not_in(output+err, 'ERROR')


print('Causing MD5 mismatch errors and checking the files.')
os.chdir('zstash_test')
shutil.copy('zstash/index.db', 'zstash/index_old.db')
print('Messing up the MD5 of all of the files with an even id.')
cmd = ['sqlite3', 'zstash/index.db', 'UPDATE files SET md5 = 0 WHERE id % 2 = 0;']
run_cmd(cmd)
cmd = 'zstash check -v --hpss={}'.format(HPSS_PATH)
output, err = run_cmd(cmd)
str_in(output+err, 'md5 mismatch for: dir/file1.txt')
str_in(output+err, 'md5 mismatch for: file3.txt')
str_in(output+err, 'md5 mismatch for: file3.txt')
str_in(output+err, 'ERROR: 000001.tar')
str_in(output+err, 'ERROR: 000004.tar')
str_in(output+err, 'ERROR: 000002.tar')
str_not_in(output+err, 'ERROR: 000000.tar')
str_not_in(output+err, 'ERROR: 000003.tar')
str_not_in(output+err, 'ERROR: 000005.tar')
# Put the original index.db back.
os.remove('zstash/index.db')
shutil.copy('zstash/index_old.db', 'zstash/index.db')
os.chdir('../')


print('Causing MD5 mismatch errors and checking the files in parallel.')
os.chdir('zstash_test')
shutil.copy('zstash/index.db', 'zstash/index_old.db')
print('Messing up the MD5 of all of the files with an even id.')
cmd = ['sqlite3', 'zstash/index.db', 'UPDATE files SET md5 = 0 WHERE id % 2 = 0;']
run_cmd(cmd)
cmd = 'zstash check -v --hpss={} --workers=3'.format(HPSS_PATH)
output, err = run_cmd(cmd)
str_in(output+err, 'md5 mismatch for: dir/file1.txt')
str_in(output+err, 'md5 mismatch for: file3.txt')
str_in(output+err, 'md5 mismatch for: file3.txt')
str_in(output+err, 'ERROR: 000001.tar')
str_in(output+err, 'ERROR: 000004.tar')
str_in(output+err, 'ERROR: 000002.tar')
str_not_in(output+err, 'ERROR: 000000.tar')
str_not_in(output+err, 'ERROR: 000003.tar')
str_not_in(output+err, 'ERROR: 000005.tar')
# Put the original index.db back.
os.remove('zstash/index.db')
shutil.copy('zstash/index_old.db', 'zstash/index.db')
os.chdir('../')


print('Verifying the data from database with the actual files')
# Checksums from HPSS
cmd = ['sqlite3', 'zstash_test/zstash/index.db', 'SELECT md5, name FROM files;']
output_hpss, err_hpss = run_cmd(cmd)
hpss_dict = {}

for l in output_hpss.split('\n'):
    l = l.split('|')
    if len(l) >= 2:
        f_name = l[1]
        f_hash = l[0]
        hpss_dict[f_name] = f_hash

# Checksums from local files
cmd = '''find zstash_test_backup -regex .*\.txt.* -exec md5sum {} + '''
output_local, err_local = run_cmd(cmd)
local_dict = {}

for l in output_local.split('\n'):
    l = l.split('  ')
    if len(l) >= 2:
        f_name = l[1].split('/')  # remove the 'zstash_test_backup'
        f_name = '/'.join(f_name[1:])
        f_hash = l[0]
        local_dict[f_name] = f_hash
print('filename|HPSS hash|local file hash')
for k in local_dict:
    print('{}|{}|{}'.format(k, hpss_dict[k], local_dict[k]))


cleanup()
print('*'*40)
print('All of the tests passed! :)')
print('*'*40)
