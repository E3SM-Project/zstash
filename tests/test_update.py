import os
import unittest
from base import *


class TestUpdate(TestZstash):
    """
    Test `zstash --update`.
    """
    # x = on, no mark = off, b = both on and off tested
    # option | Update | UpdateDryRun | UpdateKeep | TestZstash.add_files (used in multiple tests)|
    # --hpss    |x|x|x|x|
    # --dry-run | |x| | |
    # --keep    | | |x|b|
    # -v        | | | |b|
   
    def helperUpdate(self, test_name, hpss_path, zstash_path=ZSTASH_PATH):
        """
        Test `zstash update`.
        """
        self.hpss_path = hpss_path
        use_hpss = self.setupDirs(test_name)
        self.create(use_hpss, zstash_path)
        print_starred('Running update on the newly created directory, nothing should happen')
        self.assertWorkspace()
        os.chdir(self.test_dir)
        cmd = '{}zstash update -v --hpss={}'.format(zstash_path, self.hpss_path)
        output, err = run_cmd(cmd)
        os.chdir(TOP_LEVEL)
        self.check_strings(cmd, output + err, ['Nothing to update'], ['ERROR'])

    def helperUpdateDryRun(self, test_name, hpss_path, zstash_path=ZSTASH_PATH):
        """
        Test `zstash update --dry-run`.
        """
        self.hpss_path = hpss_path
        use_hpss = self.setupDirs(test_name)
        self.create(use_hpss, zstash_path)
        print_starred('Testing update with an actual change')
        self.assertWorkspace()
        if not os.path.exists('{}/dir2'.format(self.test_dir)):
            os.mkdir('{}/dir2'.format(self.test_dir))
        write_file('{}/dir2/file2.txt'.format(self.test_dir), 'file2 stuff')
        write_file('{}/dir/file1.txt'.format(self.test_dir), 'file1 stuff with changes')

        os.chdir(self.test_dir)
        cmd = '{}zstash update --dry-run --hpss={}'.format(zstash_path, self.hpss_path)
        output, err = run_cmd(cmd)
        os.chdir(TOP_LEVEL)
        expected_present = ['List of files to be updated',
             'dir/file1.txt',
             'dir2/file2.txt']
        # Make sure none of the old files or directories are moved.
        expected_absent = ['ERROR', 'file0', 'file_empty', 'empty_dir',
                           'INFO: Creating new tar archive']
        self.check_strings(cmd, output + err, expected_present, expected_absent)

    def helperUpdateKeep(self, test_name, hpss_path, zstash_path=ZSTASH_PATH):
        """
        Test `zstash update --keep`.
        """
        self.hpss_path = hpss_path
        use_hpss = self.setupDirs(test_name)
        # Not keeping the tar from `create`.
        self.create(use_hpss, zstash_path)
        self.add_files(use_hpss, zstash_path, keep=True)
        files = os.listdir('{}/{}'.format(self.test_dir, self.cache))
        if use_hpss:
            expected_files = ['index.db', '000003.tar', '000004.tar', '000001.tar', '000002.tar']
        else:
            expected_files = ['index.db', '000003.tar', '000004.tar', '000000.tar', '000001.tar', '000002.tar']
        if not compare(files, expected_files):
            error_message = 'The zstash cache does not contain expected files.\nIt has: {}'.format(files)
            self.stop(error_message)
        os.chdir(TOP_LEVEL)
        
    def testUpdate(self):
        self.helperUpdate('testUpdate', 'none')    

    def testUpdateHPSS(self):
        self.conditional_hpss_skip()
        self.helperUpdate('testUpdateHPSS', HPSS_ARCHIVE)

    def testUpdateDryRun(self):
        self.helperUpdateDryRun('testUpdateDryRun', 'none')

    def testUpdateDryRunHPSS(self):
        self.conditional_hpss_skip()
        self.helperUpdateDryRun('testUpdateDryRunHPSS', HPSS_ARCHIVE)

    def testUpdateKeep(self):
        self.helperUpdateKeep('testUpdateKeep', 'none')    

    def testUpdateKeepHPSS(self):
        self.conditional_hpss_skip()
        self.helperUpdateKeep('testUpdateKeepHPSS', HPSS_ARCHIVE)

if __name__ == '__main__':
    unittest.main()
