import os
import shutil
import unittest
from base import *


class TestExtractParallel(TestZstash):
    """
    Test `zstash extract` in parallel.
    """
    # `zstash extract` is tested in TestExtract and TestExtractParallel.
    # x = on, no mark = off, b = both on and off tested
    # option | ExtractVerbose | Extract | ExtractCache | ExtractWildcard | ExtractParallel |
    # --hpss    |x|x|x|x|x|
    # --workers | | | | |x|
    # --cache   | | |x| | |
    # --keep    | |x| | | |
    # -v        |x| | | |b|

    def helperExtractParallel(self, test_name, hpss_path, zstash_path=ZSTASH_PATH):
        """
        Test `zstash extract` in parallel.
        """
        self.hpss_path = hpss_path
        use_hpss = self.setupDirs(test_name)
        self.create(use_hpss, zstash_path)
        self.add_files(use_hpss, zstash_path)
        self.extract(use_hpss, zstash_path)
        print_starred('Deleting the extracted files and doing it again in parallel.')
        self.assertWorkspace()
        shutil.rmtree(self.test_dir)
        os.mkdir(self.test_dir)
        os.chdir(self.test_dir)
        if not use_hpss:
            shutil.copytree('{}/{}/{}'.format(TOP_LEVEL, self.backup_dir, self.cache), self.copy_dir)
        cmd = '{}zstash extract -v --hpss={} --workers=3'.format(zstash_path, self.hpss_path)
        output, err = run_cmd(cmd)
        os.chdir(TOP_LEVEL)
        expected_present = ['Extracting file0.txt', 'Extracting file0_hard.txt', 'Extracting file0_soft.txt',
                            'Extracting file_empty.txt', 'Extracting dir/file1.txt', 'Extracting empty_dir',
                            'Extracting dir2/file2.txt', 'Extracting file3.txt', 'Extracting file4.txt',
                            'Extracting file5.txt']
        if use_hpss:
            expected_present.append('Transferring file from HPSS')
        expected_absent = ['ERROR', 'Not extracting']
        self.check_strings(cmd, output + err, expected_present, expected_absent)
        # Checking that the printing was done in order.
        tar_order = []
        console_output = output + err
        for word in console_output.replace('\n', ' ').split(' '):
            if '.tar' in word:
                word = word.replace('{}/'.format(self.cache), '')
                tar_order.append(word)
        if tar_order != sorted(tar_order):
            error_message = 'The tars were printed in this order: {}\nWhen it should have been in this order: {}'.format(
                tar_order, sorted(tar_order))
            self.stop(error_message)

        # Run again, without verbose option.
        shutil.rmtree(self.test_dir)
        os.mkdir(self.test_dir)
        os.chdir(self.test_dir)
        if not use_hpss:
            shutil.copytree('{}/{}/{}'.format(TOP_LEVEL, self.backup_dir, self.cache), self.copy_dir)
        cmd = '{}zstash extract --hpss={} --workers=3'.format(zstash_path, self.hpss_path)
        output, err = run_cmd(cmd)
        os.chdir(TOP_LEVEL)
        self.check_strings(cmd, output + err, expected_present, expected_absent)
        # Checking that the printing was done in order.
        tar_order = []
        console_output = output + err
        for word in console_output.replace('\n', ' ').split(' '):
            if '.tar' in word:
                word = word.replace('{}/'.format(self.cache), '')
                tar_order.append(word)
        if tar_order != sorted(tar_order):
            error_message = 'The tars were printed in this order: {}\nWhen it should have been in this order: {}'.format(
                tar_order, sorted(tar_order))
            self.stop(error_message)
        
    def testExtractParallel(self):
        self.helperExtractParallel('testExtractParallel', 'none')    

    def testExtractParallelHPSS(self):
        self.conditional_hpss_skip()
        self.helperExtractParallel('testExtractParallelHPSS', HPSS_ARCHIVE)

if __name__ == '__main__':
    unittest.main()
