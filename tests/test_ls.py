import os
import unittest
from base import *


class TestLs(TestZstash):
    # x = on, no mark = off, b = both on and off tested
    # option | Ls |
    # --hpss |x|
    # -l     |b|
    # -v     |b|

    def helperLs(self, test_name, hpss_path, zstash_path=ZSTASH_PATH):
        """
        Test `zstash ls`.
        """
        self.hpss_path = hpss_path
        use_hpss = self.setupDirs(test_name)
        self.create(use_hpss, zstash_path)
        self.assertWorkspace()
        if not use_hpss:
            os.chdir(self.test_dir)
        for option in ['', '-v', '-l']:
            print_starred('Testing zstash ls {}'.format(option))
            cmd = '{}zstash ls {} --hpss={}'.format(zstash_path, option, self.hpss_path)
            output, err = run_cmd(cmd)
            self.check_strings(cmd, output + err, ['file0.txt'], ['ERROR'])
        if not use_hpss:
            os.chdir(TOP_LEVEL)
        
    def testLs(self):
        self.helperLs('testLs', 'none')

    # Test that `--hpss=None` will be handled as `--hpss=none`
    def testLsNone(self):
        self.helperLs('testLsNone', 'None')

    def testLsHPSS(self):
        self.conditional_hpss_skip()
        self.helperLs('testLsHPSS', HPSS_ARCHIVE)

if __name__ == '__main__':
    unittest.main()
