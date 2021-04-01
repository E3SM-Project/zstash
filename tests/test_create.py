import os
import unittest

from tests.base import (
    HPSS_ARCHIVE,
    TOP_LEVEL,
    ZSTASH_PATH,
    TestZstash,
    compare,
    print_starred,
    run_cmd,
    write_file,
)


class TestCreate(TestZstash):
    """
    Test `zstash create`.
    """

    # x = on, no mark = off, b = both on and off tested
    # option | CreateVerbose | CreateExcludeDir | CreateExcludeFile | CreateKeep | CreateCache | TestZstash.create (used in multiple tests) | TestCheckParallel.testKeepTarsWithPreviouslySetHPSS |
    # --exclude | |x|x| | | | |
    # --maxsize | | | | | | |x|
    # --keep    | | | |x| |b| |
    # --cache   | | | | |x| | |
    # -v        |x| | | | | | |

    def helperCreateVerbose(self, test_name, hpss_path: str, zstash_path=ZSTASH_PATH):
        """
        Test `zstash create -v`.
        """
        self.hpss_path: str = hpss_path
        use_hpss = self.setupDirs(test_name)
        self.create(use_hpss, zstash_path, verbose=True)

    def writeExtraFiles(self):
        """
        Write extra files for `zstash --exclude`.
        """
        os.mkdir("{}/exclude_dir".format(self.test_dir))
        os.mkdir("{}/not_exclude_dir".format(self.test_dir))
        write_file("{}/exclude_dir/file_a.txt".format(self.test_dir), "file_a stuff")
        write_file(
            "{}/not_exclude_dir/file_b.txt".format(self.test_dir), "file_b_stuff"
        )
        write_file(
            "{}/not_exclude_dir/file_c.txt".format(self.test_dir), "file_c stuff"
        )

    def helperCreateExcludeDir(self, test_name, hpss_path, zstash_path=ZSTASH_PATH):
        """
        Test `zstash --exclude`, excluding a directory.
        """
        self.hpss_path = hpss_path
        use_hpss = self.setupDirs(test_name)
        if use_hpss:
            description_str = "Adding files to HPSS"
        else:
            description_str = "Adding files to local archive"
        print_starred(description_str)
        self.assertWorkspace()
        self.writeExtraFiles()
        excluded_files = "exclude_dir/"
        cmd = "{}zstash create --exclude={} --hpss={} {}".format(
            zstash_path, excluded_files, self.hpss_path, self.test_dir
        )
        output, err = run_cmd(cmd)
        expected_present = [
            "Archiving not_exclude_dir/file_b.txt",
            "Archiving not_exclude_dir/file_c.txt",
        ]
        if use_hpss:
            expected_present += ["Transferring file to HPSS"]
        else:
            expected_present += ["put: HPSS is unavailable"]
        expected_absent = ["ERROR", "Archiving exclude_dir/file_a.txt"]
        self.check_strings(cmd, output + err, expected_present, expected_absent)

    def helperCreateExcludeFile(self, test_name, hpss_path, zstash_path=ZSTASH_PATH):
        """
        Test `zstash --exclude`, excluding a file.
        """
        self.hpss_path = hpss_path
        use_hpss = self.setupDirs(test_name)
        if use_hpss:
            description_str = "Adding files to HPSS"
        else:
            description_str = "Adding files to local archive"
        print_starred(description_str)
        self.assertWorkspace()
        self.writeExtraFiles()
        excluded_files = "not_exclude_dir/file_b.txt"
        cmd = "{}zstash create --exclude={} --hpss={} {}".format(
            zstash_path, excluded_files, self.hpss_path, self.test_dir
        )
        output, err = run_cmd(cmd)
        expected_present = [
            "Archiving exclude_dir/file_a.txt",
            "Archiving not_exclude_dir/file_c.txt",
        ]
        if use_hpss:
            expected_present += ["Transferring file to HPSS"]
        else:
            expected_present += ["put: HPSS is unavailable"]
        expected_absent = ["ERROR", "Archiving not_exclude_dir/file_b.txt"]
        self.check_strings(cmd, output + err, expected_present, expected_absent)

    def helperCreateKeep(self, test_name, hpss_path, zstash_path=ZSTASH_PATH):
        """
        Test `zstash create --keep`.
        """
        self.hpss_path = hpss_path
        use_hpss = self.setupDirs(test_name)
        self.create(use_hpss, zstash_path, keep=True)
        files = os.listdir("{}/{}".format(self.test_dir, self.cache))
        if not compare(files, ["index.db", "000000.tar"]):
            error_message = (
                "The zstash cache does not contain expected files.\nIt has: {}".format(
                    files
                )
            )
            self.stop(error_message)
        os.chdir(TOP_LEVEL)

    def helperCreateCache(self, test_name, hpss_path, zstash_path=ZSTASH_PATH):
        """
        Test `zstash create --cache=my_cache`.
        """
        self.hpss_path = hpss_path
        self.cache = "my_cache"
        use_hpss = self.setupDirs(test_name)
        self.create(use_hpss, zstash_path, cache=self.cache)
        files = os.listdir("{}/{}".format(self.test_dir, self.cache))
        if "index.db" not in files:
            error_message = (
                "The zstash cache does not contain expected files.\nIt has: {}".format(
                    files
                )
            )
            self.stop(error_message)

    def testCreateVerbose(self):
        self.helperCreateVerbose("testCreateVerbose", "none")

    def testCreateVerboseHPSS(self):
        self.conditional_hpss_skip()
        self.helperCreateVerbose("testCreateVerboseHPSS", HPSS_ARCHIVE)

    # No need to include a with-HPSS version.
    def testCreateExcludeDir(self):
        self.helperCreateExcludeDir("testCreateExcludeDir", "none")

    # No need to include a with-HPSS version.
    def testCreateExcludeFile(self):
        self.helperCreateExcludeFile("testCreateExcludeFile", "none")

    def testCreateKeep(self):
        self.helperCreateKeep("testCreateKeep", "none")

    def testCreateKeepHPSS(self):
        self.conditional_hpss_skip()
        self.helperCreateKeep("testCreateKeepHPSS", HPSS_ARCHIVE)

    def testCreateCache(self):
        self.helperCreateCache("testCreateCache", "none")

    def testCreateCacheHPSS(self):
        self.conditional_hpss_skip()
        self.helperCreateCache("testCreateCacheHPSS", HPSS_ARCHIVE)


if __name__ == "__main__":
    unittest.main()
