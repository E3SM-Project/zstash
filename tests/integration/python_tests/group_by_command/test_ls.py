import os
import unittest

from tests.integration.python_tests.group_by_command.base import (
    HPSS_ARCHIVE,
    TOP_LEVEL,
    ZSTASH_PATH,
    TestZstash,
    print_starred,
    run_cmd,
)


class TestLs(TestZstash):
    # x = on, no mark = off, b = both on and off tested
    # option  | Ls | LsTars |
    # --hpss  |x|x|
    # --cache |b| |
    # --tars  | |b|
    # -l      |b|b|
    # -v      |b| |

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

    def helperLsTars(self, test_name, hpss_path, zstash_path=ZSTASH_PATH):
        """
        Test `zstash ls --tars`
        """
        self.hpss_path = hpss_path
        use_hpss = self.setupDirs(test_name)
        self.create(use_hpss, zstash_path)
        self.assertWorkspace()
        os.chdir(self.test_dir)

        print_starred("Testing zstash ls --tars")
        cmd = "{}zstash ls --tars --hpss={}".format(zstash_path, self.hpss_path)
        output, err = run_cmd(cmd)
        self.check_strings(cmd, output + err, ["000000.tar"], ["ERROR"])

        print_starred("Testing zstash ls --tars -l")
        cmd = "{}zstash ls --tars -l --hpss={}".format(zstash_path, self.hpss_path)
        output, err = run_cmd(cmd)
        self.check_strings(cmd, output + err, ["000000.tar"], ["ERROR"])

        os.chdir(TOP_LEVEL)

    def helperLsTarsUpdate(self, test_name, hpss_path, zstash_path=ZSTASH_PATH):
        """
        Test `zstash ls --tars` when the database was initially created without the tars table
        """
        self.hpss_path = hpss_path
        use_hpss = self.setupDirs(test_name)
        # Create without a tar table -- simulate a user updating an existing database
        self.create(use_hpss, zstash_path, no_tars_md5=True)
        self.assertWorkspace()
        os.chdir(self.test_dir)

        print_starred("Testing zstash ls")
        cmd = "{}zstash ls --hpss={}".format(zstash_path, self.hpss_path)
        output, err = run_cmd(cmd)
        # tars should not be listed
        self.check_strings(
            cmd, output + err, [], ["tars table does not exist", ".tar", "ERROR"]
        )

        print_starred("Testing zstash ls --tars")
        cmd = "{}zstash ls --tars --hpss={}".format(zstash_path, self.hpss_path)
        output, err = run_cmd(cmd)
        # tars should not be listed
        self.check_strings(
            cmd, output + err, ["tars table does not exist"], [".tar", "ERROR"]
        )

        os.chdir(TOP_LEVEL)
        # Updating should create the tars table
        self.add_files(use_hpss, zstash_path)
        os.chdir(self.test_dir)

        print_starred("Testing zstash ls --tars")
        cmd = "{}zstash ls --tars --hpss={}".format(zstash_path, self.hpss_path)
        output, err = run_cmd(cmd)
        # tar should be listed now
        self.check_strings(cmd, output + err, ["000001.tar"], ["ERROR"])

        print_starred("Testing zstash ls --tars -l")
        cmd = "{}zstash ls --tars -l --hpss={}".format(zstash_path, self.hpss_path)
        output, err = run_cmd(cmd)
        self.check_strings(cmd, output + err, ["000001.tar"], ["ERROR"])

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

    def testLsTars(self):
        self.helperLsTars("testLsTars", "none")

    def testLsTarsHPSS(self):
        self.conditional_hpss_skip()
        self.helperLsTars("testLsTarsHPSS", HPSS_ARCHIVE)

    def testLsTarsUpdate(self):
        self.helperLsTarsUpdate("testLsTarsUpdate", "none")

    def testLsTarsUpdateHPSS(self):
        self.conditional_hpss_skip()
        self.helperLsTarsUpdate("testLsTarsUpdateHPSS", HPSS_ARCHIVE)


if __name__ == "__main__":
    unittest.main()
