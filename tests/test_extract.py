import os
import shutil
import unittest

from tests.base import (
    HPSS_ARCHIVE,
    TOP_LEVEL,
    ZSTASH_PATH,
    TestZstash,
    compare,
    print_starred,
    run_cmd,
)

# https://bugs.python.org/issue43743
# error: Module has no attribute "_USE_CP_SENDFILE"
shutil._USE_CP_SENDFILE = False  # type: ignore


class TestExtract(TestZstash):
    """
    Test `zstash extract`.
    """

    # `zstash extract` is tested in TestExtract and TestExtractParallel.
    # x = on, no mark = off, b = both on and off tested
    # option | ExtractVerbose | Extract | ExtractCache | ExtractTars | ExtractParallel | ExtractParallelTars |
    # --hpss    |x|x|x|x|x|x|
    # --workers | | | | |x|x|
    # --cache   | | |x| | | |
    # --keep    | |x| | | | |
    # --tars    | | | |x| |x|
    # -v        |x| | | |b| |

    def helperExtractVerbose(self, test_name, hpss_path, zstash_path=ZSTASH_PATH):
        """
        Test `zstash extract -v`.
        """
        self.hpss_path = hpss_path
        use_hpss = self.setupDirs(test_name)
        self.create(use_hpss, zstash_path)
        self.add_files(use_hpss, zstash_path)
        self.extract(use_hpss, zstash_path)
        print_starred("Testing that nothing happens when extracting a second time")
        self.assertWorkspace()
        os.chdir(self.test_dir)
        cmd = "{}zstash extract -v --hpss={}".format(zstash_path, self.hpss_path)
        output, err = run_cmd(cmd)
        if use_hpss:
            # Check that self.copy_dir only contains `index.db`.
            if not compare(os.listdir(self.copy_dir), ["index.db"]):
                error_message = (
                    "The zstash directory should not have any tars.\nIt has: {}".format(
                        os.listdir(self.copy_dir)
                    )
                )
                self.stop(error_message)
        os.chdir(TOP_LEVEL)
        expected_present = [
            "Not extracting file0.txt",
            "Not extracting file0_hard.txt",
            "Not extracting file_empty.txt",
            "Not extracting dir/file1.txt",
            "Not extracting dir2/file2.txt",
            "Not extracting file3.txt",
            "Not extracting file4.txt",
            "Not extracting file5.txt",
        ]
        expected_absent = [
            "Not extracting file0_soft.txt",  # It's okay to extract the symlinks.
            "ERROR",
        ]
        if use_hpss:
            # It's okay to extract empty dirs.
            expected_absent.append("Not extracting empty_dir")
        self.check_strings(cmd, output + err, expected_present, expected_absent)

        msg = "Deleting the extracted files and doing it again, "
        msg += "while making sure the tars are kept."
        print(msg)
        shutil.rmtree(self.test_dir)
        os.mkdir(self.test_dir)
        os.chdir(self.test_dir)
        if not use_hpss:
            shutil.copytree(
                "{}/{}/{}".format(TOP_LEVEL, self.backup_dir, self.cache), self.copy_dir
            )
        cmd = "{}zstash extract -v --hpss={} --keep".format(zstash_path, self.hpss_path)
        output, err = run_cmd(cmd)
        # Check that self.copy_dir contains all expected files
        if not compare(
            os.listdir(self.copy_dir),
            [
                "index.db",
                "000000.tar",
                "000001.tar",
                "000002.tar",
                "000003.tar",
                "000004.tar",
            ],
        ):
            error_message = "The zstash directory does not contain expected files.\nIt has: {}".format(
                os.listdir(self.copy_dir)
            )
            self.stop(error_message)
        os.chdir(TOP_LEVEL)
        expected_present = [
            "Extracting file0.txt",
            "Extracting file0_hard.txt",
            "Extracting file0_soft.txt",
            "Extracting file_empty.txt",
            "Extracting dir/file1.txt",
            "Extracting empty_dir",
            "Extracting dir2/file2.txt",
            "Extracting file3.txt",
            "Extracting file4.txt",
            "Extracting file5.txt",
        ]
        if use_hpss:
            expected_present.append("Transferring file from HPSS")
        expected_absent = ["ERROR", "Not extracting"]
        self.check_strings(cmd, output + err, expected_present, expected_absent)

    def helperExtractKeep(self, test_name, hpss_path, zstash_path=ZSTASH_PATH):
        """
        Test `zstash extract` with `--keep`.
        """
        self.hpss_path = hpss_path
        use_hpss = self.setupDirs(test_name)
        self.create(use_hpss, zstash_path)
        self.add_files(use_hpss, zstash_path)
        self.extract(use_hpss, zstash_path)
        msg = "Deleting the extracted files and doing it again without verbose option, "
        msg += "while making sure the tars are kept."
        print_starred(msg)
        self.assertWorkspace()
        shutil.rmtree(self.test_dir)
        os.mkdir(self.test_dir)
        os.chdir(self.test_dir)
        if not use_hpss:
            shutil.copytree(
                "{}/{}/{}".format(TOP_LEVEL, self.backup_dir, self.cache), self.copy_dir
            )
        cmd = "{}zstash extract --hpss={} --keep".format(zstash_path, self.hpss_path)
        output, err = run_cmd(cmd)
        if not compare(
            os.listdir(self.cache),
            [
                "index.db",
                "000000.tar",
                "000001.tar",
                "000002.tar",
                "000003.tar",
                "000004.tar",
            ],
        ):
            error_message = "The zstash directory does not contain expected files.\nIt has: {}".format(
                os.listdir(self.cache)
            )
            self.stop(error_message)
        os.chdir(TOP_LEVEL)
        expected_present = [
            "Extracting file0.txt",
            "Extracting file0_hard.txt",
            "Extracting file0_soft.txt",
            "Extracting file_empty.txt",
            "Extracting dir/file1.txt",
            "Extracting empty_dir",
            "Extracting dir2/file2.txt",
            "Extracting file3.txt",
            "Extracting file4.txt",
            "Extracting file5.txt",
        ]
        if use_hpss:
            expected_present.append("Transferring file from HPSS")
        expected_absent = ["ERROR", "Not extracting"]
        self.check_strings(cmd, output + err, expected_present, expected_absent)

    def helperExtractCache(self, test_name, hpss_path, zstash_path=ZSTASH_PATH):
        """
        Test `zstash extract --cache`.
        """
        self.hpss_path = hpss_path
        self.cache = "my_cache"
        use_hpss = self.setupDirs(test_name)
        if not use_hpss:
            self.copy_dir = self.cache
        self.create(use_hpss, zstash_path, cache=self.cache)
        self.add_files(use_hpss, zstash_path, cache=self.cache)
        self.extract(use_hpss, zstash_path, cache=self.cache)
        files = os.listdir("{}/{}".format(self.test_dir, self.cache))
        if use_hpss:
            expected_files = ["index.db"]
        else:
            expected_files = [
                "index.db",
                "000003.tar",
                "000004.tar",
                "000000.tar",
                "000001.tar",
                "000002.tar",
            ]
        if not compare(files, expected_files):
            error_message = (
                "The zstash cache does not contain expected files.\nIt has: {}".format(
                    files
                )
            )
            self.stop(error_message)

    def testExtractVerbose(self):
        self.helperExtractVerbose("testExtractVerbose", "none")

    def testExtractVerboseHPSS(self):
        self.conditional_hpss_skip()
        self.helperExtractVerbose("testExtractVerboseHPSS", HPSS_ARCHIVE)

    def testExtractKeep(self):
        self.helperExtractKeep("testExtractKeep", "none")

    def testExtractKeepHPSS(self):
        self.conditional_hpss_skip()
        self.helperExtractKeep("testExtractKeepHPSS", HPSS_ARCHIVE)

    def testExtractCache(self):
        self.helperExtractCache("testExtractCache", "none")

    def testExtractCacheHPSS(self):
        self.conditional_hpss_skip()
        self.helperExtractCache("testExtractCacheHPSS", HPSS_ARCHIVE)

    def testExtractTars(self):
        helperExtractTars(self, "testExtractTars", "none")

    def testExtractTarsHPSS(self):
        self.conditional_hpss_skip()
        helperExtractTars(self, "testExtractTarsHPSS", HPSS_ARCHIVE)


def helperExtractTars(
    tester, test_name, hpss_path, worker_str="", zstash_path=ZSTASH_PATH
):
    """
    Test `zstash extract --tars`
    """
    tester.hpss_path = hpss_path
    use_hpss = tester.setupDirs(test_name)
    tester.create(use_hpss, zstash_path)
    tester.add_files(use_hpss, zstash_path)

    os.rename(tester.test_dir, tester.backup_dir)
    os.mkdir(tester.test_dir)
    os.chdir(tester.test_dir)
    if not use_hpss:
        shutil.copytree(
            "{}/{}/{}".format(TOP_LEVEL, tester.backup_dir, tester.cache),
            tester.copy_dir,
        )

    zstash_cmd = (
        '{}zstash extract --hpss={} --tars="000001-00002,000003,000004-"{}'.format(
            zstash_path, tester.hpss_path, worker_str
        )
    )
    output, err = run_cmd(zstash_cmd)
    expected_present = [
        "INFO: Opening tar archive zstash/000001.tar",
        "INFO: Opening tar archive zstash/000002.tar",
        "INFO: Opening tar archive zstash/000003.tar",
        "INFO: Opening tar archive zstash/000004.tar",
        "INFO: No failures detected when extracting the files.",
    ]
    expected_absent = [
        "INFO: Opening tar archive zstash/000000.tar",
    ]
    tester.check_strings(zstash_cmd, output + err, expected_present, expected_absent)
    os.chdir(TOP_LEVEL)


if __name__ == "__main__":
    unittest.main()
