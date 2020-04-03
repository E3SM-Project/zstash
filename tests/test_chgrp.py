import os
import unittest
from base import *


class TestChgrp(TestZstash):
    """
    Test `zstash chgrp`.
    """
    # x = on, no mark = off, b = both on and off tested
    # option | Chgrp |
    # -R |x|
    # -v |b|

    def helperChgrp(self, test_name, hpss_path, zstash_path=ZSTASH_PATH):
        """
        Test `zstash chgrp`.
        """
        self.hpss_path = hpss_path
        use_hpss = self.setupDirs(test_name)
        self.create(use_hpss, zstash_path)
        print('Testing chgrp')
        self.assertWorkspace()
        GROUP = 'e3sm'
        for option in ['-v', '']:
            print('Running zstash chgrp {}'.format(option))
            cmd = '{}zstash chgrp {} -R {} {}'.format(zstash_path, option, GROUP, self.hpss_path)
            output, err = run_cmd(cmd)
            if use_hpss:
                self.check_strings(cmd, output + err, [], ['ERROR'])
                print('Now check that the files are in the {} group'.format(GROUP))
                cmd = 'hsi ls -l {}'.format(self.hpss_path)
                output, err = run_cmd(cmd)
                expected_present = 'e3sm'
            else:
                expected_present = 'chgrp: HPSS is unavailable'
            self.check_strings(cmd, output + err, expected_present, ['ERROR'])

    def testChgrp(self):
        self.helperChgrp('testChgrp', 'none')    

    def testChgrpHPSS(self):
        self.conditional_hpss_skip()
        self.helperChgrp('testChgrpHPSS', HPSS_ARCHIVE)

if __name__ == '__main__':
    unittest.main()
