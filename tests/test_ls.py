import os
import unittest

from tests.base import (
    HPSS_ARCHIVE,
    TOP_LEVEL,
    ZSTASH_PATH,
    TestZstash,
    print_starred,
    run_cmd,
)


class TestLs(TestZstash):
    # x = on, no mark = off, b = both on and off tested
    # option | Ls |
    # --hpss  |x|
    # --cache |b|
    # -l      |b|
    # -v      |b|

    def helperLs(self, test_name, hpss_path, cache=None, zstash_path=ZSTASH_PATH):
        """
        Test `zstash ls`.
        """
        self.hpss_path = hpss_path
        if cache:
            # Override default cache
            self.cache = cache
            cache_option = " --cache={}".format(self.cache)
        else:
            cache_option = ""
        use_hpss = self.setupDirs(test_name)
        self.create(use_hpss, zstash_path, cache=self.cache)
        self.assertWorkspace()
        os.chdir(self.test_dir)
        for option in ["", "-v", "-l"]:
            print_starred("Testing zstash ls {}".format(option))
            cmd = "{}zstash ls{} {} --hpss={}".format(
                zstash_path, cache_option, option, self.hpss_path
            )
            output, err = run_cmd(cmd)
            self.check_strings(cmd, output + err, ["file0.txt"], ["ERROR"])
        os.chdir(TOP_LEVEL)

    def testLs(self):
        self.helperLs("testLs", "none")

    # Test that `--hpss=None` will be handled as `--hpss=none`
    def testLsNone(self):
        self.helperLs("testLsNone", "None")

    def testLsHPSS(self):
        self.conditional_hpss_skip()
        self.helperLs("testLsHPSS", HPSS_ARCHIVE)

    def testLsCache(self):
        self.helperLs("testLsCache", "none", cache="my_cache")

    def testLsCacheHPSS(self):
        self.conditional_hpss_skip()
        self.helperLs("testLsCacheHPSS", HPSS_ARCHIVE, cache="my_cache")


if __name__ == "__main__":
    unittest.main()
