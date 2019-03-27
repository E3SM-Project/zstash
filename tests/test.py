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
    Run a command while printing and returning the stdout and stderr.
    """
    print('+ {}'.format(cmd))
    if isinstance(cmd, str):
        cmd = cmd.split()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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


# TODO: Change the hpss directory to a dir that's accessable to everyone.
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
cmd = 'zstash create --hpss={} zstash_test'.format(HPSS_PATH)
output, err = run_cmd(cmd)
str_in(output+err, 'Transferring file to HPSS')
str_not_in(output+err, 'ERROR')

print('Testing ls')
cmd = 'zstash ls --hpss={}'.format(HPSS_PATH)
output, err = run_cmd(cmd)
str_in(output+err, 'file0.txt')
str_not_in(output+err, 'ERROR')
cmd = 'zstash ls -l --hpss={}'.format(HPSS_PATH)
output, err = run_cmd(cmd)
str_in(output+err, 'tar')
str_not_in(output+err, 'ERROR')

print('Testing chgrp')
GROUP = 'acme'
print('First, make sure that the files are not already in the {} group'.format(GROUP))
cmd = 'hsi ls -l {}'.format(HPSS_PATH)
output, err = run_cmd(cmd)
str_not_in(output+err, GROUP)
str_not_in(output+err, 'ERROR')
print('Running zstash chgrp')
cmd = 'zstash chgrp -R {} {}'.format(GROUP, HPSS_PATH)
output, err = run_cmd(cmd)
str_not_in(output+err, 'ERROR')
print('Now check that the files are in the {} group'.format(GROUP))
cmd = 'hsi ls -l {}'.format(HPSS_PATH)
output, err = run_cmd(cmd)
str_in(output+err, 'acme')
str_not_in(output+err, 'ERROR')

print('Running update on the newly created directory, nothing should happen')
os.chdir('zstash_test')
cmd = 'zstash update --hpss={}'.format(HPSS_PATH)
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
cmd = 'zstash update --hpss={}'.format(HPSS_PATH)
output, err = run_cmd(cmd)
os.chdir('../')
str_in(output+err, 'Transferring file to HPSS')
str_not_in(output+err, 'ERROR')
# Make sure none of the old files are moved.
str_not_in(output+err, 'file0')
str_not_in(output+err, 'file_empty')
str_not_in(output+err, 'empty_dir')
str_not_in(output+err, 'ERROR')

print('Testing the checking functionality')
cmd = 'zstash check --hpss={}'.format(HPSS_PATH)
output, err = run_cmd(cmd)
str_not_in(output+err, 'ERROR')

print('Testing the extract functionality')
os.rename('zstash_test', 'zstash_test_backup')
os.mkdir('zstash_test')
os.chdir('zstash_test')
cmd = 'zstash extract --hpss={}'.format(HPSS_PATH)
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
str_not_in(output+err, 'ERROR')
str_not_in(output+err, 'Not extracting')


print('Testing the extract functionality again, nothing should happen')
os.chdir('zstash_test')
cmd = 'zstash extract --hpss={}'.format(HPSS_PATH)
output, err = run_cmd(cmd)
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
str_not_in(output+err, 'ERROR')

print('Running update on the newly extracted directory, nothing should happen')
os.chdir('zstash_test')
cmd = 'zstash update --hpss={}'.format(HPSS_PATH)
output, err = run_cmd(cmd)
os.chdir('../')
str_in(output+err, 'Nothing to update')
str_not_in(output+err, 'ERROR')

print('Verifying the data from database with the actual files')
# Checksums from HPSS
cmd = ['sqlite3', 'zstash_test/zstash/index.db', 'select md5, name from files;']
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
