"""
Run the test suite with `python test.py`
"""

import os
import shutil
import subprocess
import unittest
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
    print(err, flush=True)
    return output, err


def cleanup(hpss_path):
    """
    After the script has failed/run, remove all created files, even those on the HPSS repo.
    """
    print('Removing test files, both locally and at the HPSS repo')
    if os.path.exists('zstash_test'):
        shutil.rmtree('zstash_test')
    if os.path.exists('zstash_test_backup'):
        shutil.rmtree('zstash_test_backup')
    if os.path.exists('zstash'):
        shutil.rmtree('zstash')
    cmd = 'hsi rm -R {}'.format(hpss_path)
    run_cmd(cmd)


# Compare content of two (unordered lists)
# https://stackoverflow.com/questions/7828867/how-to-efficiently-compare-two-unordered-lists-not-sets-in-python
def compare(s, t):
    return Counter(s) == Counter(t)


class TestZStash(unittest.TestCase):
    def stop(self, hpss_path, error_message):
        """
        Cleanup and stop running this script.
        """
        cleanup(hpss_path)
        self.fail(error_message)

    def str_not_in(self, cmd, hpss_path, output, msg):
        """
        If the msg is not in the output string, then everything is fine.
        """
        if msg in output:
            print('*' * 40)
            error_message = 'Command=`{}`. This was not supposed to be found: {}.'.format(cmd, msg)
            print(error_message)
            print('*' * 40)
            self.stop(hpss_path, error_message)

    def str_in(self, cmd, hpss_path, output, msg):
        """
        If the msg is in the output string, then the everything is fine.
        """
        if msg not in output:
            print('*' * 40)
            error_message = 'Command=`{}`. This was supposed to be found, but was not: {}'.format(cmd, msg)
            print(error_message)
            print('*' * 40)
            self.stop(hpss_path, error_message)

    def testZstashWithHPSS(self):
        print('*' * 40)
        print('testZstashWithHPSS')
        print('*' * 40)
        print('0. Setup')
        # Makes this in the home dir of the user on HPSS.
        # Ex: /home/z/zshaheen/zstash_test
        hpss_path = 'zstash_test'
        # Create files and directories
        for option in ['-v', '']:
            print('Creating files {}.'.format(option))
            cleanup(hpss_path)
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

        print('1. Adding files to HPSS')
        cmd = 'zstash create {} --hpss={} zstash_test'.format(option, hpss_path)
        output, err = run_cmd(cmd)
        self.str_in(cmd, hpss_path, output+err, 'Transferring file to HPSS')
        self.str_not_in(cmd, hpss_path, output+err, 'ERROR')

        print('2. Testing chgrp')
        GROUP = 'acme'
        for option in ['-v', '']:
            print('Running zstash chgrp {}'.format(option))
            cmd = 'zstash chgrp {} -R {} {}'.format(option, GROUP, hpss_path)
            output, err = run_cmd(cmd)
            self.str_not_in(cmd, hpss_path, output+err, 'ERROR')
            print('Now check that the files are in the {} group'.format(GROUP))
            cmd = 'hsi ls -l {}'.format(hpss_path)
            output, err = run_cmd(cmd)
            self.str_in(cmd, hpss_path, output+err, 'acme')
            self.str_not_in(cmd, hpss_path, output+err, 'ERROR')

        print('3. Running update on the newly created directory, nothing should happen')
        os.chdir('zstash_test')
        cmd = 'zstash update -v --hpss={}'.format(hpss_path)
        output, err = run_cmd(cmd)
        os.chdir('../')
        self.str_in(cmd, hpss_path, output+err, 'Nothing to update')
        self.str_not_in(cmd, hpss_path, output+err, 'ERROR')

        print('4. Testing update with an actual change')
        if not os.path.exists('zstash_test/dir2'):
            os.mkdir('zstash_test/dir2')
        write_file('zstash_test/dir2/file2.txt', 'file2 stuff')
        write_file('zstash_test/dir/file1.txt', 'file1 stuff with changes')

        os.chdir('zstash_test')
        cmd = 'zstash update -v --hpss={}'.format(hpss_path)
        output, err = run_cmd(cmd)
        os.chdir('../')
        self.str_in(cmd, hpss_path, output+err, 'Transferring file to HPSS')
        self.str_not_in(cmd, hpss_path, output+err, 'ERROR')
        # Make sure none of the old files are moved.
        self.str_not_in(cmd, hpss_path, output+err, 'file0')
        self.str_not_in(cmd, hpss_path, output+err, 'file_empty')
        self.str_not_in(cmd, hpss_path, output+err, 'empty_dir')
        self.str_not_in(cmd, hpss_path, output+err, 'ERROR')

        print('5. Adding many more files to the HPSS archive.')
        msg = 'This is because we need many separate tar archives'
        msg += ' for testing zstash extract/check with parallel.'
        print(msg)
        write_file('zstash_test/file3.txt', 'file3 stuff')
        os.chdir('zstash_test')
        cmd = 'zstash update --hpss={}'.format(hpss_path)
        output, err = run_cmd(cmd)
        os.chdir('../')
        write_file('zstash_test/file4.txt', 'file4 stuff')
        os.chdir('zstash_test')
        cmd = 'zstash update --hpss={}'.format(hpss_path)
        output, err = run_cmd(cmd)
        os.chdir('../')
        write_file('zstash_test/file5.txt', 'file5 stuff')
        os.chdir('zstash_test')
        cmd = 'zstash update --hpss={}'.format(hpss_path)
        output, err = run_cmd(cmd)
        os.chdir('../')

        for option in ['', '-v', '-l']:
            print('6. Testing zstash ls {}'.format(option))
            cmd = 'zstash ls {} --hpss={}'.format(option, hpss_path)
            output, err = run_cmd(cmd)
            self.str_in(cmd, hpss_path, output+err, 'file0.txt')
            self.str_not_in(cmd, hpss_path, output+err, 'ERROR')

        print('7. Testing the checking functionality')
        cmd = 'zstash check --hpss={}'.format(hpss_path)
        output, err = run_cmd(cmd)
        self.str_in(cmd, hpss_path, output+err, 'Checking file0.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking file0_hard.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking file0_soft.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking file_empty.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking dir/file1.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking empty_dir')
        self.str_in(cmd, hpss_path, output+err, 'Checking dir2/file2.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking file3.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking file4.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking file5.txt')
        self.str_not_in(cmd, hpss_path, output+err, 'ERROR')
        cmd = 'zstash check -v --hpss={}'.format(hpss_path)
        output, err = run_cmd(cmd)
        self.str_in(cmd, hpss_path, output+err, 'Checking file0.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking file0_hard.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking file0_soft.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking file_empty.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking dir/file1.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking empty_dir')
        self.str_in(cmd, hpss_path, output+err, 'Checking dir2/file2.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking file3.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking file4.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking file5.txt')
        self.str_not_in(cmd, hpss_path, output+err, 'ERROR')

        print('8. Testing the extract functionality')
        os.rename('zstash_test', 'zstash_test_backup')
        os.mkdir('zstash_test')
        os.chdir('zstash_test')
        cmd = 'zstash extract --hpss={}'.format(hpss_path)
        output, err = run_cmd(cmd)
        os.chdir('../')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file0.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file0_hard.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file0_soft.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file_empty.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting dir/file1.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting empty_dir')
        self.str_in(cmd, hpss_path, output+err, 'Extracting dir2/file2.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file3.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file4.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file5.txt')
        self.str_not_in(cmd, hpss_path, output+err, 'ERROR')
        self.str_not_in(cmd, hpss_path, output+err, 'Not extracting')

        print('9. Testing the extract functionality again, nothing should happen')
        os.chdir('zstash_test')
        cmd = 'zstash extract -v --hpss={}'.format(hpss_path)
        output, err = run_cmd(cmd)
        # Check that the zstash/ directory is empty.
        # It should only contain an 'index.db'.
        if not compare(os.listdir('zstash'), ['index.db']):
            print('*'*40)
            error_message = 'The zstash directory should not have any tars.\nIt has: {}'.format(
                os.listdir('zstash'))
            print(error_message)
            print('*'*40)
            self.stop(hpss_path, error_message)
        os.chdir('../')
        self.str_in(cmd, hpss_path, output+err, 'Not extracting file0.txt')
        self.str_in(cmd, hpss_path, output+err, 'Not extracting file0_hard.txt')
        # It's okay to extract the symlinks.
        self.str_not_in(cmd, hpss_path, output+err, 'Not extracting file0_soft.txt')
        self.str_in(cmd, hpss_path, output+err, 'Not extracting file_empty.txt')
        self.str_in(cmd, hpss_path, output+err, 'Not extracting dir/file1.txt')
        # It's okay to extract empty dirs.
        self.str_not_in(cmd, hpss_path, output+err, 'Not extracting empty_dir')
        self.str_in(cmd, hpss_path, output+err, 'Not extracting dir2/file2.txt')
        self.str_in(cmd, hpss_path, output+err, 'Not extracting file3.txt')
        self.str_in(cmd, hpss_path, output+err, 'Not extracting file4.txt')
        self.str_in(cmd, hpss_path, output+err, 'Not extracting file5.txt')
        self.str_not_in(cmd, hpss_path, output+err, 'ERROR')


        msg = 'Deleting the extracted files and doing it again, '
        msg += 'while making sure the tars are kept.'
        print(msg)
        shutil.rmtree('zstash_test')
        os.mkdir('zstash_test')
        os.chdir('zstash_test')
        cmd = 'zstash extract -v --hpss={} --keep'.format(hpss_path)
        output, err = run_cmd(cmd)
        # Check that the zstash/ directory contains all expected files
        if not compare(os.listdir('zstash'), ['index.db', '000000.tar', '000001.tar', '000002.tar', '000003.tar', '000004.tar']):
            print('*'*40)
            error_message = 'The zstash directory does not contain expected files.\nIt has: {}'.format(
                os.listdir('zstash'))
            print(error_message)
            print('*'*40)
            self.stop(hpss_path, error_message)
        os.chdir('../')
        self.str_in(cmd, hpss_path, output+err, 'Transferring file from HPSS')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file0.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file0_hard.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file0_soft.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file_empty.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting dir/file1.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting empty_dir')
        self.str_in(cmd, hpss_path, output+err, 'Extracting dir2/file2.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file3.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file4.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file5.txt')
        self.str_not_in(cmd, hpss_path, output+err, 'ERROR')
        self.str_not_in(cmd, hpss_path, output+err, 'Not extracting')

        msg = '10. Deleting the extracted files and doing it again without verbose option, '
        msg += 'while making sure the tars are kept.'
        print(msg)
        shutil.rmtree('zstash_test')
        os.mkdir('zstash_test')
        os.chdir('zstash_test')
        cmd = 'zstash extract --hpss={} --keep'.format(hpss_path)
        output, err = run_cmd(cmd)
        # Check that the zstash/ directory contains all expected files
        if not compare(os.listdir('zstash'), ['index.db', '000000.tar', '000001.tar', '000002.tar', '000003.tar', '000004.tar']):
            print('*'*40)
            error_message = 'The zstash directory does not contain expected files.\nIt has: {}'.format(
                os.listdir('zstash'))
            print(error_message)
            print('*'*40)
            self.stop(hpss_path, error_message)
        os.chdir('../')
        self.str_in(cmd, hpss_path, output+err, 'Transferring file from HPSS')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file0.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file0_hard.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file0_soft.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file_empty.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting dir/file1.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting empty_dir')
        self.str_in(cmd, hpss_path, output+err, 'Extracting dir2/file2.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file3.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file4.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file5.txt')
        self.str_not_in(cmd, hpss_path, output+err, 'ERROR')
        self.str_not_in(cmd, hpss_path, output+err, 'Not extracting')

        print('11. Deleting the extracted files and doing it again in parallel.')
        shutil.rmtree('zstash_test')
        os.mkdir('zstash_test')
        os.chdir('zstash_test')
        cmd = 'zstash extract -v --hpss={} --workers=3'.format(hpss_path)
        output, err = run_cmd(cmd)
        os.chdir('../')
        self.str_in(cmd, hpss_path, output+err, 'Transferring file from HPSS')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file0.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file0_hard.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file0_soft.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file_empty.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting dir/file1.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting empty_dir')
        self.str_in(cmd, hpss_path, output+err, 'Extracting dir2/file2.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file3.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file4.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file5.txt')
        self.str_not_in(cmd, hpss_path, output+err, 'ERROR')
        self.str_not_in(cmd, hpss_path, output+err, 'Not extracting')
        # Checking that the printing was done in order.
        tar_order = []
        console_output = output+err
        for word in console_output.replace('\n', ' ').split(' '):
            if '.tar' in word:
                word = word.replace('zstash/', '')
                tar_order.append(word)
        if tar_order != sorted(tar_order):
            print('*'*40)
            error_message = 'The tars were printed in this order: {}\nWhen it should have been in this order: {}'.format(
                tar_order, sorted(tar_order))
            print(error_message)
            print('*'*40)
            self.stop(hpss_path, error_message)

        shutil.rmtree('zstash_test')
        os.mkdir('zstash_test')
        os.chdir('zstash_test')
        cmd = 'zstash extract --hpss={} --workers=3'.format(hpss_path)
        output, err = run_cmd(cmd)
        os.chdir('../')
        self.str_in(cmd, hpss_path, output+err, 'Transferring file from HPSS')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file0.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file0_hard.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file0_soft.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file_empty.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting dir/file1.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting empty_dir')
        self.str_in(cmd, hpss_path, output+err, 'Extracting dir2/file2.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file3.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file4.txt')
        self.str_in(cmd, hpss_path, output+err, 'Extracting file5.txt')
        self.str_not_in(cmd, hpss_path, output+err, 'ERROR')
        self.str_not_in(cmd, hpss_path, output+err, 'Not extracting')
        # Checking that the printing was done in order.
        tar_order = []
        console_output = output+err
        for word in console_output.replace('\n', ' ').split(' '):
            if '.tar' in word:
                word = word.replace('zstash/', '')
                tar_order.append(word)
        if tar_order != sorted(tar_order):
            print('*'*40)
            error_message = 'The tars were printed in this order: {}\nWhen it should have been in this order: {}'.format(
                tar_order, sorted(tar_order))
            print(error_message)
            print('*'*40)
            self.stop(hpss_path, error_message)

        print('12. Checking the files again in parallel.')
        os.chdir('zstash_test')
        cmd = 'zstash check -v --hpss={} --workers=3'.format(hpss_path)
        output, err = run_cmd(cmd)
        os.chdir('../')
        self.str_in(cmd, hpss_path, output+err, 'Checking file0.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking file0_hard.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking file0_soft.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking file_empty.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking dir/file1.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking empty_dir')
        self.str_in(cmd, hpss_path, output+err, 'Checking dir2/file2.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking file3.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking file4.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking file5.txt')
        self.str_not_in(cmd, hpss_path, output+err, 'ERROR')

        print('13. Checking the files again in parallel without verbose option.')
        os.chdir('zstash_test')
        cmd = 'zstash check --hpss={} --workers=3'.format(hpss_path)
        output, err = run_cmd(cmd)
        os.chdir('../')
        self.str_in(cmd, hpss_path, output+err, 'Checking file0.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking file0_hard.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking file0_soft.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking file_empty.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking dir/file1.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking empty_dir')
        self.str_in(cmd, hpss_path, output+err, 'Checking dir2/file2.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking file3.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking file4.txt')
        self.str_in(cmd, hpss_path, output+err, 'Checking file5.txt')
        self.str_not_in(cmd, hpss_path, output+err, 'ERROR')

        print('Causing MD5 mismatch errors and checking the files.')
        os.chdir('zstash_test')
        shutil.copy('zstash/index.db', 'zstash/index_old.db')
        print('14. Messing up the MD5 of all of the files with an even id.')
        cmd = ['sqlite3', 'zstash/index.db', 'UPDATE files SET md5 = 0 WHERE id % 2 = 0;']
        run_cmd(cmd)
        cmd = 'zstash check -v --hpss={}'.format(hpss_path)
        output, err = run_cmd(cmd)
        self.str_in(cmd, hpss_path, output+err, 'md5 mismatch for: dir/file1.txt')
        self.str_in(cmd, hpss_path, output+err, 'md5 mismatch for: file3.txt')
        self.str_in(cmd, hpss_path, output+err, 'md5 mismatch for: file3.txt')
        self.str_in(cmd, hpss_path, output+err, 'ERROR: 000001.tar')
        self.str_in(cmd, hpss_path, output+err, 'ERROR: 000004.tar')
        self.str_in(cmd, hpss_path, output+err, 'ERROR: 000002.tar')
        self.str_not_in(cmd, hpss_path, output+err, 'ERROR: 000000.tar')
        self.str_not_in(cmd, hpss_path, output+err, 'ERROR: 000003.tar')
        self.str_not_in(cmd, hpss_path, output+err, 'ERROR: 000005.tar')
        # Put the original index.db back.
        os.remove('zstash/index.db')
        shutil.copy('zstash/index_old.db', 'zstash/index.db')
        os.chdir('../')

        print('Causing MD5 mismatch errors and checking the files in parallel.')
        os.chdir('zstash_test')
        shutil.copy('zstash/index.db', 'zstash/index_old.db')
        print('15. Messing up the MD5 of all of the files with an even id.')
        cmd = ['sqlite3', 'zstash/index.db', 'UPDATE files SET md5 = 0 WHERE id % 2 = 0;']
        run_cmd(cmd)
        cmd = 'zstash check -v --hpss={} --workers=3'.format(hpss_path)
        output, err = run_cmd(cmd)
        self.str_in(cmd, hpss_path, output+err, 'md5 mismatch for: dir/file1.txt')
        self.str_in(cmd, hpss_path, output+err, 'md5 mismatch for: file3.txt')
        self.str_in(cmd, hpss_path, output+err, 'md5 mismatch for: file3.txt')
        self.str_in(cmd, hpss_path, output+err, 'ERROR: 000001.tar')
        self.str_in(cmd, hpss_path, output+err, 'ERROR: 000004.tar')
        self.str_in(cmd, hpss_path, output+err, 'ERROR: 000002.tar')
        self.str_not_in(cmd, hpss_path, output+err, 'ERROR: 000000.tar')
        self.str_not_in(cmd, hpss_path, output+err, 'ERROR: 000003.tar')
        self.str_not_in(cmd, hpss_path, output+err, 'ERROR: 000005.tar')
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

        cleanup(hpss_path)


if __name__ == '__main__':
    unittest.main()
