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
    for d in ['zstash_test', 'zstash_test_backup', 'zstash']:
        if os.path.exists(d):
            shutil.rmtree(d)
    if hpss_path and hpss_path.lower() != 'none':
        cmd = 'hsi rm -R {}'.format(hpss_path)
        run_cmd(cmd)


# Compare content of two (unordered lists)
# https://stackoverflow.com/questions/7828867/how-to-efficiently-compare-two-unordered-lists-not-sets-in-python
def compare(s, t):
    return Counter(s) == Counter(t)


def print_in_box(string):
    print('*' * 40)
    print(string)
    print('*' * 40)


class TestZstash(unittest.TestCase):
    def stop(self, hpss_path, error_message):
        """
        Cleanup and stop running this script.
        """
        print_in_box(error_message)
        cleanup(hpss_path)
        self.fail(error_message)

    def check_strings(self, command, hpss_path, output, expected_present, expected_absent):
        error_messages = []
        for string in expected_present:
            if string not in output:
                error_message = 'This was supposed to be found, but was not: {}.'.format(string)
                error_messages.append(error_message)
        for string in expected_absent:
            if string in output:
                error_message = 'This was not supposed to be found, but was: {}.'.format(string)
                error_messages.append(error_message)
        if error_messages:
            error_message = 'Command=`{}`. Errors={}'.format(command, error_messages)
            print_in_box(error_message)
            self.stop(hpss_path, error_message)

    def helper(self, test_name, hpss_path):
        if hpss_path.lower() == 'none':
            use_hpss = False
        else:
            use_hpss = True
        print_in_box(test_name)
        print('0. Setup')
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

        if use_hpss:
            step_str = '1. Adding files to HPSS'
        else:
            step_str = '1. Adding files to local archive'
        print(step_str)
        cmd = 'zstash create {} --hpss={} zstash_test'.format(option, hpss_path)
        output, err = run_cmd(cmd)
        if use_hpss:
            expected_present = ['Transferring file to HPSS']
        else:
            expected_present = ['put: HPSS is unavailable']
        self.check_strings(cmd, hpss_path, output + err, expected_present, ['ERROR'])

        print('2. Testing chgrp')
        GROUP = 'acme'
        for option in ['-v', '']:
            print('Running zstash chgrp {}'.format(option))
            cmd = 'zstash chgrp {} -R {} {}'.format(option, GROUP, hpss_path)
            output, err = run_cmd(cmd)
            if use_hpss:
                self.check_strings(cmd, hpss_path, output + err, [], ['ERROR'])
                print('Now check that the files are in the {} group'.format(GROUP))
                cmd = 'hsi ls -l {}'.format(hpss_path)
                output, err = run_cmd(cmd)
                expected_present = 'acme'
            else:
                expected_present = 'chgrp: HPSS is unavailable'
            self.check_strings(cmd, hpss_path, output + err, expected_present, ['ERROR'])

        print('3. Running update on the newly created directory, nothing should happen')
        os.chdir('zstash_test')
        cmd = 'zstash update -v --hpss={}'.format(hpss_path)
        output, err = run_cmd(cmd)
        os.chdir('../')
        self.check_strings(cmd, hpss_path, output + err, ['Nothing to update'], ['ERROR'])

        print('4. Testing update with an actual change')
        if not os.path.exists('zstash_test/dir2'):
            os.mkdir('zstash_test/dir2')
        write_file('zstash_test/dir2/file2.txt', 'file2 stuff')
        write_file('zstash_test/dir/file1.txt', 'file1 stuff with changes')

        os.chdir('zstash_test')
        cmd = 'zstash update -v --hpss={}'.format(hpss_path)
        output, err = run_cmd(cmd)
        os.chdir('../')
        if use_hpss:
            expected_present = 'Transferring file to HPSS'
        else:
            expected_present = 'put: HPSS is unavailable'
        expected_absent = ['ERROR', 'file0', # Make sure none of the old files are moved.
            'file_empty', 'empty_dir']
        self.check_strings(cmd, hpss_path, output + err, expected_present, expected_absent)

        print('5. Adding many more files to the HPSS archive.')
        msg = 'This is because we need many separate tar archives'
        msg += ' for testing zstash extract/check with parallel.'
        print(msg)
        write_file('zstash_test/file3.txt', 'file3 stuff')
        os.chdir('zstash_test')
        cmd = 'zstash update --hpss={}'.format(hpss_path)
        run_cmd(cmd)
        os.chdir('../')
        write_file('zstash_test/file4.txt', 'file4 stuff')
        os.chdir('zstash_test')
        cmd = 'zstash update --hpss={}'.format(hpss_path)
        run_cmd(cmd)
        os.chdir('../')
        write_file('zstash_test/file5.txt', 'file5 stuff')
        os.chdir('zstash_test')
        cmd = 'zstash update --hpss={}'.format(hpss_path)
        run_cmd(cmd)
        os.chdir('../')

        if not use_hpss:
            os.chdir('zstash_test')
        for option in ['', '-v', '-l']:
            print('6. Testing zstash ls {}'.format(option))
            cmd = 'zstash ls {} --hpss={}'.format(option, hpss_path)
            output, err = run_cmd(cmd)
            self.check_strings(cmd, hpss_path, output + err, ['file0.txt'], ['ERROR'])

        print('7. Testing the checking functionality')
        cmd = 'zstash check --hpss={}'.format(hpss_path)
        output, err = run_cmd(cmd)
        expected_present = ['Checking file0.txt', 'Checking file0_hard.txt', 'Checking file0_soft.txt',
                            'Checking file_empty.txt', 'Checking dir/file1.txt', 'Checking empty_dir',
                            'Checking dir2/file2.txt', 'Checking file3.txt', 'Checking file4.txt', 'Checking file5.txt']
        expected_absent = ['ERROR']
        self.check_strings(cmd, hpss_path, output + err, expected_present, expected_absent)
        cmd = 'zstash check -v --hpss={}'.format(hpss_path)
        output, err = run_cmd(cmd)
        self.check_strings(cmd, hpss_path, output + err, expected_present, expected_absent)

        print('8. Testing the extract functionality')
        if not use_hpss:
            os.chdir('../')
        os.rename('zstash_test', 'zstash_test_backup')
        os.mkdir('zstash_test')
        os.chdir('zstash_test')
        if not use_hpss:
            shutil.copytree('../zstash_test_backup/zstash', 'zstash')
        cmd = 'zstash extract --hpss={}'.format(hpss_path)
        output, err = run_cmd(cmd)
        os.chdir('../')
        expected_present = ['Extracting file0.txt', 'Extracting file0_hard.txt', 'Extracting file0_soft.txt',
                            'Extracting file_empty.txt', 'Extracting dir/file1.txt', 'Extracting empty_dir',
                            'Extracting dir2/file2.txt', 'Extracting file3.txt', 'Extracting file4.txt',
                            'Extracting file5.txt']
        expected_absent = ['ERROR']
        if use_hpss:
            expected_absent.append('Not extracting')
        self.check_strings(cmd, hpss_path, output + err, expected_present, expected_absent)

        print('9. Testing the extract functionality again, nothing should happen')
        os.chdir('zstash_test')
        cmd = 'zstash extract -v --hpss={}'.format(hpss_path)
        output, err = run_cmd(cmd)
        if use_hpss:
            # Check that the zstash/ directory is empty.
            # It should only contain an 'index.db'.
            if not compare(os.listdir('zstash'), ['index.db']):
                error_message = 'The zstash directory should not have any tars.\nIt has: {}'.format(
                    os.listdir('zstash'))
                self.stop(hpss_path, error_message)
        os.chdir('../')
        expected_present = ['Not extracting file0.txt', 'Not extracting file0_hard.txt',
                            'Not extracting file_empty.txt', 'Not extracting dir/file1.txt',
                            'Not extracting dir2/file2.txt', 'Not extracting file3.txt', 'Not extracting file4.txt',
                            'Not extracting file5.txt']
        expected_absent = ['Not extracting file0_soft.txt', # It's okay to extract the symlinks.
            'ERROR']
        if use_hpss:
            # It's okay to extract empty dirs.
            expected_absent.append('Not extracting empty_dir')
        self.check_strings(cmd, hpss_path, output + err, expected_present, expected_absent)

        msg = 'Deleting the extracted files and doing it again, '
        msg += 'while making sure the tars are kept.'
        print(msg)
        shutil.rmtree('zstash_test')
        os.mkdir('zstash_test')
        os.chdir('zstash_test')
        if not use_hpss:
            shutil.copytree('../zstash_test_backup/zstash', 'zstash')
        cmd = 'zstash extract -v --hpss={} --keep'.format(hpss_path)
        output, err = run_cmd(cmd)
        # Check that the zstash/ directory contains all expected files
        if not compare(os.listdir('zstash'),
                       ['index.db', '000000.tar', '000001.tar', '000002.tar', '000003.tar', '000004.tar']):
            error_message = 'The zstash directory does not contain expected files.\nIt has: {}'.format(
                os.listdir('zstash'))
            self.stop(hpss_path, error_message)
        os.chdir('../')
        expected_present = ['Extracting file0.txt', 'Extracting file0_hard.txt', 'Extracting file0_soft.txt',
                            'Extracting file_empty.txt', 'Extracting dir/file1.txt', 'Extracting empty_dir',
                            'Extracting dir2/file2.txt', 'Extracting file3.txt', 'Extracting file4.txt',
                            'Extracting file5.txt']
        if use_hpss:
            expected_present.append('Transferring file from HPSS')
        expected_absent = ['ERROR', 'Not extracting']
        self.check_strings(cmd, hpss_path, output + err, expected_present, expected_absent)

        msg = '10. Deleting the extracted files and doing it again without verbose option, '
        msg += 'while making sure the tars are kept.'
        print(msg)
        shutil.rmtree('zstash_test')
        os.mkdir('zstash_test')
        os.chdir('zstash_test')
        if not use_hpss:
            shutil.copytree('../zstash_test_backup/zstash', 'zstash')
        cmd = 'zstash extract --hpss={} --keep'.format(hpss_path)
        output, err = run_cmd(cmd)
        # Check that the zstash/ directory contains all expected files
        if not compare(os.listdir('zstash'),
                       ['index.db', '000000.tar', '000001.tar', '000002.tar', '000003.tar', '000004.tar']):
            error_message = 'The zstash directory does not contain expected files.\nIt has: {}'.format(
                os.listdir('zstash'))
            self.stop(hpss_path, error_message)
        os.chdir('../')
        self.check_strings(cmd, hpss_path, output + err, expected_present, expected_absent)

        print('11. Deleting the extracted files and doing it again in parallel.')
        shutil.rmtree('zstash_test')
        os.mkdir('zstash_test')
        os.chdir('zstash_test')
        if not use_hpss:
            shutil.copytree('../zstash_test_backup/zstash', 'zstash')
        cmd = 'zstash extract -v --hpss={} --workers=3'.format(hpss_path)
        output, err = run_cmd(cmd)
        os.chdir('../')
        self.check_strings(cmd, hpss_path, output + err, expected_present, expected_absent)
        # Checking that the printing was done in order.
        tar_order = []
        console_output = output + err
        for word in console_output.replace('\n', ' ').split(' '):
            if '.tar' in word:
                word = word.replace('zstash/', '')
                tar_order.append(word)
        if tar_order != sorted(tar_order):
            error_message = 'The tars were printed in this order: {}\nWhen it should have been in this order: {}'.format(
                tar_order, sorted(tar_order))
            self.stop(hpss_path, error_message)

        shutil.rmtree('zstash_test')
        os.mkdir('zstash_test')
        os.chdir('zstash_test')
        if not use_hpss:
            shutil.copytree('../zstash_test_backup/zstash', 'zstash')
        cmd = 'zstash extract --hpss={} --workers=3'.format(hpss_path)
        output, err = run_cmd(cmd)
        os.chdir('../')
        self.check_strings(cmd, hpss_path, output + err, expected_present, expected_absent)
        # Checking that the printing was done in order.
        tar_order = []
        console_output = output + err
        for word in console_output.replace('\n', ' ').split(' '):
            if '.tar' in word:
                word = word.replace('zstash/', '')
                tar_order.append(word)
        if tar_order != sorted(tar_order):
            error_message = 'The tars were printed in this order: {}\nWhen it should have been in this order: {}'.format(
                tar_order, sorted(tar_order))
            self.stop(hpss_path, error_message)

        print('12. Checking the files again in parallel.')
        os.chdir('zstash_test')
        cmd = 'zstash check -v --hpss={} --workers=3'.format(hpss_path)
        output, err = run_cmd(cmd)
        os.chdir('../')
        expected_present = ['Checking file0.txt', 'Checking file0_hard.txt', 'Checking file0_soft.txt',
                            'Checking file_empty.txt', 'Checking dir/file1.txt', 'Checking empty_dir',
                            'Checking dir2/file2.txt', 'Checking file3.txt', 'Checking file4.txt', 'Checking file5.txt']
        expected_absent = ['ERROR']
        self.check_strings(cmd, hpss_path, output + err, expected_present, expected_absent)

        print('13. Checking the files again in parallel without verbose option.')
        os.chdir('zstash_test')
        cmd = 'zstash check --hpss={} --workers=3'.format(hpss_path)
        output, err = run_cmd(cmd)
        os.chdir('../')
        self.check_strings(cmd, hpss_path, output + err, expected_present, expected_absent)

        print('Causing MD5 mismatch errors and checking the files.')
        os.chdir('zstash_test')
        shutil.copy('zstash/index.db', 'zstash/index_old.db')
        print('14. Messing up the MD5 of all of the files with an even id.')
        cmd = ['sqlite3', 'zstash/index.db', 'UPDATE files SET md5 = 0 WHERE id % 2 = 0;']
        run_cmd(cmd)
        cmd = 'zstash check -v --hpss={}'.format(hpss_path)
        output, err = run_cmd(cmd)
        expected_present = ['md5 mismatch for: dir/file1.txt', 'md5 mismatch for: file3.txt',
                            'ERROR: 000001.tar', 'ERROR: 000004.tar', 'ERROR: 000002.tar']
        expected_absent = ['ERROR: 000000.tar', 'ERROR: 000003.tar', 'ERROR: 000005.tar']
        self.check_strings(cmd, hpss_path, output + err, expected_present, expected_absent)
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
        self.check_strings(cmd, hpss_path, output + err, expected_present, expected_absent)
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

    @unittest.skipIf(os.system('which hsi') != 0, 'This system does not have hsi')
    def testZstashWithHPSS(self):
        # Makes zstash_test in the home dir of the user on HPSS.
        # e.g., /home/z/zshaheen/zstash_test
        self.helper('testZstashWithHPSS', 'zstash_test')

    def testZstashWithoutHPSS(self):
        self.helper('testZstashWithoutHPSS', 'none')

    def testZstashNoneHPSS(self):
        self.helper('testZstashNoneHPSS', 'None')

    def testKeepTars(self):
        print_in_box('testKeepTars')
        if os.path.exists('test_files'):
            shutil.rmtree('test_files')
        os.mkdir('test_files')
        run_cmd('touch test_files/file1.txt')
        run_cmd('touch test_files/file2.txt')
        hpss_path = 'none'
        # Run `zstash create`
        run_cmd('zstash create --hpss={} test_files'.format(hpss_path))
        actual = sorted(os.listdir('test_files/zstash/'))
        expected = sorted(['000000.tar', 'index.db'])
        self.assertEqual(actual, expected)
        os.chdir('test_files')
        # Delete txt files
        run_cmd('rm file1.txt file2.txt')
        # Run `zstash extract`
        output, err = run_cmd('zstash extract --hpss={}'.format(hpss_path))
        # Run `zstash check`
        output, err = run_cmd('zstash check --hpss={}'.format(hpss_path))
        self.assertEqual(output + err,
                         'INFO: Opening tar archive zstash/000000.tar\nINFO: Checking file1.txt\nINFO: Checking file2.txt\nINFO: No failures detected when checking the files.\n')
        # Check that tar and db files were not deleted
        actual = sorted(os.listdir('zstash/'))
        expected = sorted(['000000.tar', 'index.db'])
        self.assertEqual(actual, expected)
        # Check that tar file is read-only
        # https://stackoverflow.com/questions/1861836/checking-file-permissions-in-linux-with-python
        stat = os.stat('zstash/000000.tar')
        oct_mode = str(oct(stat.st_mode))[-3:]
        # https://en.wikipedia.org/wiki/Chmod#Numerical_permissions
        # Write mode is permitted when any of 2,3,6,7 are included
        # That is, in binary, the numbers with middle digit of 1: 010, 011, 110, 111.
        invalid_permissions = [2,3,6,7]
        # https://stackoverflow.com/questions/3697432/how-to-find-list-intersection
        intersection = [n for n in oct_mode if int(n) in invalid_permissions]
        if intersection:
            error_message = 'oct_mode={} includes {}'.format(oct_mode, intersection)
            self.fail(error_message)
        os.chdir('..')
        shutil.rmtree('test_files')


if __name__ == '__main__':
    unittest.main()
