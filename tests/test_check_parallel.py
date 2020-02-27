import os
import shutil
import unittest
from base import *


class TestCheckParallel(TestZstash):
    """
    Test `zstash check` in parallel.
    """
    # `zstash check` is tested in TestCheck and TestCheckParallel.
    # x = on, no mark = off, b = both on and off tested
    # option | Check | CheckMismatch | CheckKeepTars | CheckParallel | CheckParallelVerboseMismatch | CheckParallelKeepTars |
    # --hpss    |x|x|x|x|x| |
    # --workers | | | |x|x|x|
    # --cache   |b| | | | | |
    # --keep    | | | | | |b|
    # -v        |b|x| |b|x| |

    def helperCheckParallel(self, test_name, hpss_path, zstash_path=ZSTASH_PATH, verbose=False):
        """
        Test `zstash check` in parallel.
        """
        self.hpss_path = hpss_path
        use_hpss = self.setupDirs(test_name)
        self.create(use_hpss, zstash_path)
        self.add_files(use_hpss, zstash_path)
        self.extract(use_hpss, zstash_path)
        print_starred('Checking the files in parallel.')
        self.assertWorkspace()
        os.chdir(self.test_dir)
        v_option = ' -v' if verbose else ''
        cmd = '{}zstash check{} --hpss={} --workers=3'.format(zstash_path, v_option, self.hpss_path)
        output, err = run_cmd(cmd)
        os.chdir(TOP_LEVEL)
        expected_present = ['Checking file0.txt', 'Checking file0_hard.txt', 'Checking file0_soft.txt',
                            'Checking file_empty.txt', 'Checking dir/file1.txt', 'Checking empty_dir',
                            'Checking dir2/file2.txt', 'Checking file3.txt', 'Checking file4.txt', 'Checking file5.txt']
        expected_absent = ['ERROR']
        self.check_strings(cmd, output + err, expected_present, expected_absent)

    def helperCheckParallelVerboseMismatch(self, test_name, hpss_path, zstash_path=ZSTASH_PATH):
        """
        Test `zstash check -v` in parallel with MD5 mismatch.
        """
        self.hpss_path = hpss_path
        use_hpss = self.setupDirs(test_name)
        self.create(use_hpss, zstash_path)
        self.add_files(use_hpss, zstash_path)
        self.extract(use_hpss, zstash_path)
        print_starred('Causing MD5 mismatch errors and checking the files in parallel.')
        self.assertWorkspace()
        os.chdir(self.test_dir)
        shutil.copy('{}/index.db'.format(self.cache), '{}/index_old.db'.format(self.cache))
        print('Messing up the MD5 of all of the files with an even id.')
        cmd = ['sqlite3', '{}/index.db'.format(self.cache), 'UPDATE files SET md5 = 0 WHERE id % 2 = 0;']
        run_cmd(cmd)
        cmd = '{}zstash check -v --hpss={} --workers=3'.format(zstash_path, self.hpss_path)
        output, err = run_cmd(cmd)
        # These files have an even `id` in the sqlite3 table.
        expected_present = ['md5 mismatch for: dir/file1.txt', 'md5 mismatch for: file3.txt',
                            'ERROR: 000001.tar', 'ERROR: 000004.tar', 'ERROR: 000002.tar']
        # These files have an odd `id` in the sqlite3 table.
        expected_absent = ['ERROR: 000000.tar', 'ERROR: 000003.tar', 'ERROR: 000005.tar']
        self.check_strings(cmd, output + err, expected_present, expected_absent)
        # Put the original index.db back.
        os.remove('{}/index.db'.format(self.cache))
        shutil.copy('{}/index_old.db'.format(self.cache), '{}/index.db'.format(self.cache))
        os.chdir(TOP_LEVEL)

        print('Verifying the data from database with the actual files')
        # Checksums from HPSS
        cmd = ['sqlite3', '{}/{}/index.db'.format(self.test_dir, self.cache), 'SELECT md5, name FROM files;']
        output_hpss, err_hpss = run_cmd(cmd)
        hpss_dict = {}

        for l in output_hpss.split('\n'):
            l = l.split('|')
            if len(l) >= 2:
                f_name = l[1]
                f_hash = l[0]
                hpss_dict[f_name] = f_hash

        # Checksums from local files
        cmd = 'find {} '.format(self.backup_dir)
        cmd += '''-regex .*\.txt.* -exec md5sum {} + ''' # Literal {}, not for formatting
        output_local, err_local = run_cmd(cmd)
        local_dict = {}

        for l in output_local.split('\n'):
            l = l.split('  ')
            if len(l) >= 2:
                f_name = l[1].split('/')  # remove the backup_dir
                f_name = '/'.join(f_name[1:])
                f_hash = l[0]
                local_dict[f_name] = f_hash
        print('filename|HPSS hash|local file hash')
        for k in local_dict:
            print('{}|{}|{}'.format(k, hpss_dict[k], local_dict[k]))

    def helperCheckParallelKeepTars(self, test_name, hpss_path, zstash_path=ZSTASH_PATH):
        """
        Test `zstash check` in parallel when hpss is set in `zstash create`.
        """
        self.assertWorkspace()
        self.hpss_path = hpss_path
        self.setupDirs(test_name)
        if self.hpss_path.lower() != 'none':
            keep_option = ' --keep'
        else:
            keep_option = ''
        # Run `zstash create`
        run_cmd('{}zstash create --hpss={}{} --maxsize 128 {}'.format(zstash_path, self.hpss_path, keep_option, self.test_dir))
        files = os.listdir('{}/{}'.format(self.test_dir, self.cache))
        if not compare(files, ['000000.tar', 'index.db']):
            error_message = 'The zstash cache does not contain expected files.\nIt has: {}'.format(files)
            self.stop(error_message)
        # Run `zstash check` without specifying hpss
        os.chdir(self.test_dir)
        run_cmd('{}zstash check{} --workers=2'.format(zstash_path, keep_option))
        os.chdir(TOP_LEVEL)
        files = os.listdir('{}/{}'.format(self.test_dir, self.cache))
        if not compare(files, ['000000.tar', 'index.db']):
            error_message = 'The zstash cache does not contain expected files.\nIt has: {}'.format(files)
            self.stop(error_message)

    def testCheckParallelVerbose(self):
        self.helperCheckParallel('testCheckParallelVerbose', 'none', verbose=True)

    def testCheckParallelVerboseHPSS(self):
        self.conditional_hpss_skip()
        self.helperCheckParallel('testCheckParallelVerboseHPSS', HPSS_ARCHIVE, verbose=True)

    def testCheckParallel(self):
        self.helperCheckParallel('testCheckParallel', 'none')

    def testCheckParallelHPSS(self):
        self.conditional_hpss_skip()
        self.helperCheckParallel('testCheckParallelHPSS', HPSS_ARCHIVE)
        
    def testCheckParallelVerboseMismatch(self):
        self.helperCheckParallelVerboseMismatch('testCheckParallelVerboseMismatch', 'none')

    def testCheckParallelVerboseMismatchHPSS(self):
        self.conditional_hpss_skip()
        self.helperCheckParallelVerboseMismatch('testCheckParallelVerboseMismatchHPSS', HPSS_ARCHIVE)

    def testCheckParallelKeepTars(self):
        self.helperCheckParallelKeepTars('testCheckParallelKeepTars', 'none')

    def testCheckParallelKeepTarsHPSS(self):
        self.conditional_hpss_skip()
        self.helperCheckParallelKeepTars('testCheckParallelKeepTarsHPSS', HPSS_ARCHIVE)

if __name__ == '__main__':
    unittest.main()
