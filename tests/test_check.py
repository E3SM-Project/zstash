import os
import shutil
import unittest

from tests.base import (
    HPSS_ARCHIVE,
    TOP_LEVEL,
    ZSTASH_PATH,
    TestZstash,
    compare,
    print_in_box,
    print_starred,
    run_cmd,
    write_file,
)

# https://bugs.python.org/issue43743
# error: Module has no attribute "_USE_CP_SENDFILE"
shutil._USE_CP_SENDFILE = False  # type: ignore


class TestCheck(TestZstash):
    """
    Test `zstash check`.
    """

    # `zstash check` is tested in TestCheck and TestCheckParallel.
    # x = on, no mark = off, b = both on and off tested
    # option | Check | CheckMismatch | CheckKeepTars | CheckTars | CheckParallel | CheckParallelVerboseMismatch | CheckParallelKeepTars | CheckParallelTars |
    # --hpss    |x|x|x|x|x|x| |x|
    # --workers | | | | |x|x|x|x|
    # --cache   |b| | | | | | | |
    # --keep    | | | | | | |b| |
    # --tars    | | | |x| | | |x|
    # -v        |b|x| | |b|x| | |

    def helperCheck(self, test_name, hpss_path, cache=None, zstash_path=ZSTASH_PATH):
        """
        Test `zstash check`.
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
        self.add_files(use_hpss, zstash_path, cache=self.cache)
        print_starred("Testing the checking functionality")
        self.assertWorkspace()
        os.chdir(self.test_dir)
        cmd = "{}zstash check{} --hpss={}".format(
            zstash_path, cache_option, self.hpss_path
        )
        output, err = run_cmd(cmd)
        expected_present = [
            "Checking file0.txt",
            "Checking file0_hard.txt",
            "Checking file0_soft.txt",
            "Checking file_empty.txt",
            "Checking dir/file1.txt",
            "Checking empty_dir",
            "Checking dir2/file2.txt",
            "Checking file3.txt",
            "Checking file4.txt",
            "Checking file5.txt",
        ]
        expected_absent = ["ERROR"]
        self.check_strings(cmd, output + err, expected_present, expected_absent)
        cmd = "{}zstash check{} -v --hpss={}".format(
            zstash_path, cache_option, self.hpss_path
        )
        output, err = run_cmd(cmd)
        self.check_strings(cmd, output + err, expected_present, expected_absent)
        os.chdir(TOP_LEVEL)

    def helperCheckVerboseMismatch(self, test_name, hpss_path, zstash_path=ZSTASH_PATH):
        """
        Test `zstash check` with MD5 mismatch errors.
        """
        self.hpss_path = hpss_path
        use_hpss = self.setupDirs(test_name)
        self.create(use_hpss, zstash_path)
        self.add_files(use_hpss, zstash_path)
        self.extract(use_hpss, zstash_path)
        print_starred("Causing MD5 mismatch errors and checking the files.")
        self.assertWorkspace()
        os.chdir(self.test_dir)
        shutil.copy(
            "{}/index.db".format(self.cache), "{}/index_old.db".format(self.cache)
        )
        print("Messing up the MD5 of all of the files with an even id.")
        sqlite_cmd = [
            "sqlite3",
            "{}/index.db".format(self.cache),
            "UPDATE files SET md5 = 0 WHERE id % 2 = 0;",
        ]
        run_cmd(sqlite_cmd)
        zstash_cmd = "{}zstash check -v --hpss={}".format(zstash_path, self.hpss_path)
        output, err = run_cmd(zstash_cmd)
        # These files have an even `id` in the sqlite3 table.
        expected_present = [
            "md5 mismatch for: dir/file1.txt",
            "md5 mismatch for: file3.txt",
            "ERROR: 000001.tar",
            "ERROR: 000004.tar",
            "ERROR: 000002.tar",
        ]
        # These files have an odd `id` in the sqlite3 table.
        expected_absent = [
            "ERROR: 000000.tar",
            "ERROR: 000003.tar",
            "ERROR: 000005.tar",
        ]
        self.check_strings(zstash_cmd, output + err, expected_present, expected_absent)
        # Put the original index.db back.
        os.remove("{}/index.db".format(self.cache))
        shutil.copy(
            "{}/index_old.db".format(self.cache), "{}/index.db".format(self.cache)
        )
        os.chdir(TOP_LEVEL)

    def testCheck(self):
        self.helperCheck("testCheck", "none")

    def testCheckHPSS(self):
        self.conditional_hpss_skip()
        self.helperCheck("testCheckHPSS", HPSS_ARCHIVE)

    def testCheckCache(self):
        self.helperCheck("testCheckCache", "none", cache="my_cache")

    def testCheckCacheHPSS(self):
        self.conditional_hpss_skip()
        self.helperCheck("testCheckCacheHPSS", HPSS_ARCHIVE, cache="my_cache")

    def testCheckVerboseMismatch(self):
        self.helperCheckVerboseMismatch("testCheckVerboseMismatch", "none")

    def testCheckVerboseMismatchHPSS(self):
        self.conditional_hpss_skip()
        self.helperCheckVerboseMismatch("testCheckVerboseMismatchHPSS", HPSS_ARCHIVE)

    def testCheckTars(self):
        helperCheckTars(self, "testCheckTars", "none")

    def testCheckTarsHPSS(self):
        self.conditional_hpss_skip()
        helperCheckTars(self, "testCheckTarsHPSS", HPSS_ARCHIVE)

    def testCheckKeepTars(self):
        """
        Test that `zstash check` does not delete tars if `--hpss=none`.
        """
        print_in_box("testKeepTars")
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        os.mkdir(self.test_dir)
        write_file("{}/file1.txt".format(self.test_dir), "")
        write_file("{}/file2.txt".format(self.test_dir), "")
        self.hpss_path = "none"
        zstash_path = ZSTASH_PATH
        # Run `zstash create`
        run_cmd(
            "{}zstash create --hpss={} {}".format(
                zstash_path, self.hpss_path, self.test_dir
            )
        )
        files = os.listdir("{}/{}/".format(self.test_dir, self.cache))
        if not compare(files, ["000000.tar", "index.db"]):
            error_message = (
                "The zstash cache does not contain expected files.\nIt has: {}".format(
                    files
                )
            )
            self.stop(error_message)
        os.chdir(self.test_dir)
        # Delete txt files
        run_cmd("rm file1.txt file2.txt")
        # Run `zstash extract`
        output, err = run_cmd(
            "{}zstash extract --hpss={}".format(zstash_path, self.hpss_path)
        )
        # Run `zstash check`
        output, err = run_cmd(
            "{}zstash check --hpss={}".format(zstash_path, self.hpss_path)
        )
        self.assertEqualOrStop(
            output + err,
            'INFO: Opening tar archive {}/000000.tar\nINFO: Checking file1.txt\nINFO: Checking file2.txt\nINFO: No failures detected when checking the files. If you have a log file, run "grep -i Exception <log-file>" to double check.\n'.format(
                self.cache
            ),
        )
        # Check that tar and db files were not deleted
        files = os.listdir("{}/".format(self.cache))
        if not compare(files, ["000000.tar", "index.db"]):
            error_message = (
                "The zstash cache does not contain expected files.\nIt has: {}".format(
                    files
                )
            )
            self.stop(error_message)
        # Check that tar file is read-only
        # https://stackoverflow.com/questions/1861836/checking-file-permissions-in-linux-with-python
        stat = os.stat("{}/000000.tar".format(self.cache))
        oct_mode = str(oct(stat.st_mode))[-3:]
        # https://en.wikipedia.org/wiki/Chmod#Numerical_permissions
        # Write mode is permitted when any of 2,3,6,7 are included
        # That is, in binary, the numbers with middle digit of 1: 010, 011, 110, 111.
        invalid_permissions = [2, 3, 6, 7]
        # https://stackoverflow.com/questions/3697432/how-to-find-list-intersection
        # Get all characters from `oct_mode` that are also in the `invalid_permissions` list.
        intersection = [n for n in oct_mode if int(n) in invalid_permissions]
        if intersection:
            error_message = "oct_mode={} includes {}".format(oct_mode, intersection)
            self.stop(error_message)
        os.chdir(TOP_LEVEL)


def helperCheckTars(
    tester, test_name, hpss_path, worker_str="", zstash_path=ZSTASH_PATH
):
    """
    Test `zstash check --tars`
    """
    tester.hpss_path = hpss_path
    use_hpss = tester.setupDirs(test_name)
    tester.create(use_hpss, zstash_path)
    tester.add_files(use_hpss, zstash_path)

    tester.assertWorkspace()
    os.chdir(tester.test_dir)

    # Starting at 000001 until the end
    zstash_cmd = '{}zstash check --hpss={} --tars="000001-"{}'.format(
        zstash_path, tester.hpss_path, worker_str
    )
    output, err = run_cmd(zstash_cmd)
    expected_present = [
        "INFO: Opening tar archive zstash/000001.tar",
        "INFO: Opening tar archive zstash/000002.tar",
        "INFO: Opening tar archive zstash/000003.tar",
        "INFO: Opening tar archive zstash/000004.tar",
        "INFO: No failures detected when checking the files.",
    ]
    expected_absent = [
        "INFO: Opening tar archive zstash/000000.tar",
    ]
    tester.check_strings(zstash_cmd, output + err, expected_present, expected_absent)
    # Starting from the beginning to 00003 (included)
    zstash_cmd = '{}zstash check --hpss={} --tars="-000003"{}'.format(
        zstash_path, tester.hpss_path, worker_str
    )
    output, err = run_cmd(zstash_cmd)
    expected_present = [
        "INFO: Opening tar archive zstash/000000.tar",
        "INFO: Opening tar archive zstash/000001.tar",
        "INFO: Opening tar archive zstash/000002.tar",
        "INFO: Opening tar archive zstash/000003.tar",
        "INFO: No failures detected when checking the files.",
    ]
    expected_absent = [
        "INFO: Opening tar archive zstash/000004.tar",
    ]
    tester.check_strings(zstash_cmd, output + err, expected_present, expected_absent)
    # Specific range
    zstash_cmd = '{}zstash check --hpss={} --tars="000001-000003"{}'.format(
        zstash_path, tester.hpss_path, worker_str
    )
    output, err = run_cmd(zstash_cmd)
    expected_present = [
        "INFO: Opening tar archive zstash/000001.tar",
        "INFO: Opening tar archive zstash/000002.tar",
        "INFO: Opening tar archive zstash/000003.tar",
        "INFO: No failures detected when checking the files.",
    ]
    expected_absent = [
        "INFO: Opening tar archive zstash/000000.tar",
        "INFO: Opening tar archive zstash/000004.tar",
    ]
    tester.check_strings(zstash_cmd, output + err, expected_present, expected_absent)
    # Selected tar files
    zstash_cmd = '{}zstash check --hpss={} --tars="000001,000003"{}'.format(
        zstash_path, tester.hpss_path, worker_str
    )
    output, err = run_cmd(zstash_cmd)
    expected_present = [
        "INFO: Opening tar archive zstash/000001.tar",
        "INFO: Opening tar archive zstash/000003.tar",
        "INFO: No failures detected when checking the files.",
    ]
    expected_absent = [
        "INFO: Opening tar archive zstash/000000.tar",
        "INFO: Opening tar archive zstash/000002.tar",
        "INFO: Opening tar archive zstash/000004.tar",
    ]
    tester.check_strings(zstash_cmd, output + err, expected_present, expected_absent)
    # Mix and match
    zstash_cmd = (
        '{}zstash check --hpss={} --tars="000001-00002,000003,000004-"{}'.format(
            zstash_path, tester.hpss_path, worker_str
        )
    )
    output, err = run_cmd(zstash_cmd)
    expected_present = [
        "INFO: Opening tar archive zstash/000001.tar",
        "INFO: Opening tar archive zstash/000002.tar",
        "INFO: Opening tar archive zstash/000003.tar",
        "INFO: Opening tar archive zstash/000004.tar",
        "INFO: No failures detected when checking the files.",
    ]
    expected_absent = [
        "INFO: Opening tar archive zstash/000000.tar",
    ]
    tester.check_strings(zstash_cmd, output + err, expected_present, expected_absent)
    # Ending with nonexistent tar
    zstash_cmd = '{}zstash check --hpss={} --tars="000001-00007"{}'.format(
        zstash_path, tester.hpss_path, worker_str
    )
    output, err = run_cmd(zstash_cmd)
    expected_present = [
        "INFO: Opening tar archive zstash/000001.tar",
        "INFO: Opening tar archive zstash/000002.tar",
        "INFO: Opening tar archive zstash/000003.tar",
        "INFO: Opening tar archive zstash/000004.tar",
        "INFO: No failures detected when checking the files.",
    ]
    expected_absent = [
        "INFO: Opening tar archive zstash/000000.tar",
        "INFO: Opening tar archive zstash/000005.tar",
        "INFO: Opening tar archive zstash/000006.tar",
        "INFO: Opening tar archive zstash/000007.tar",
    ]
    tester.check_strings(zstash_cmd, output + err, expected_present, expected_absent)
    # Ending with nonexistent tar
    zstash_cmd = '{}zstash check --hpss={} --tars="000001-00003"{} file'.format(
        zstash_path, tester.hpss_path, worker_str
    )
    output, err = run_cmd(zstash_cmd)
    expected_present = ["ValueError: If --tars is used, <files> should not be listed."]
    expected_absent = [
        "INFO: Opening tar archive zstash/000000.tar",
        "INFO: Opening tar archive zstash/000001.tar",
        "INFO: Opening tar archive zstash/000002.tar",
        "INFO: Opening tar archive zstash/000003.tar",
        "INFO: Opening tar archive zstash/000004.tar",
        "INFO: No failures detected when checking the files.",
    ]
    tester.check_strings(zstash_cmd, output + err, expected_present, expected_absent)

    os.chdir(TOP_LEVEL)


if __name__ == "__main__":
    unittest.main()
