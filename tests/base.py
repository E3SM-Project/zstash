"""
Run the test suite with `python -m unittest tests/test_*.py`

To run an individual test, run something like `python -m unittest tests.test_extract.TestExtract.testExtractRetries`

If running on Cori, it is preferable to run from $CSCRATCH rather than
/global/homes. Running from the latter may result in a
'Resource temporarily unavailable' error.

If running on Compy, it is necessary to run test_globus.py from a
sub-directory of /compyfs rather than /qfs.
"""

import os
import shutil
import subprocess
import unittest
from collections import Counter

# https://bugs.python.org/issue43743
# error: Module has no attribute "_USE_CP_SENDFILE"
shutil._USE_CP_SENDFILE = False  # type: ignore

# The directory `zstash` is in. Set this if `zstash` is not in your PATH.
ZSTASH_PATH = ""

# Top level directory. Should end in `zstash`.
# This is used to ensure we are changing into the correct subdirectories and parent directories.
TOP_LEVEL = os.getcwd()

# Skip all HPSS tests. Decreases test runtime.
SKIP_HPSS = False

# Default HPSS archive name.
HPSS_ARCHIVE = "zstash_test"


def write_file(name, contents):
    """
    Write contents to a file named `name`.
    """
    with open(name, "w") as f:
        f.write(contents)


def run_cmd(cmd):
    """
    Run a command. Then print and return the stdout and stderr.
    """
    print("+ {}".format(cmd))
    # `cmd` must be a list
    if isinstance(cmd, str):
        cmd = cmd.split()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # TODO: user input prompts don't make it through here. It just hangs.
    # Is it possible to check if we already have consents granted and then not need to prompt?
    # Or, we could make a flag/command line option to prompt for consents or not.
    output, err = p.communicate()

    # When running in Python 3, the output of subprocess.Popen.communicate()
    # is a bytes object. We need to convert it to a string.
    # Type annotation is not necessary since the if statements check the instance.
    if isinstance(output, bytes):
        output = output.decode("utf-8")  # type: ignore
    if isinstance(err, bytes):
        err = err.decode("utf-8")  # type: ignore

    print(output)
    print(err, flush=True)
    return output, err


def compare(s, t):
    """
    # Compare content of two (unordered lists)
    # https://stackoverflow.com/questions/7828867/how-to-efficiently-compare-two-unordered-lists-not-sets-in-python
    """
    return Counter(s) == Counter(t)


def print_in_box(string):
    """
    Print with stars above and below.
    """
    print("*" * 40)
    print(string)
    print("*" * 40)


def print_starred(string):
    """
    Print with stars on each side.
    """
    print("***", string, "***")


class TestZstash(unittest.TestCase):
    """
    Base test class.
    """

    def setUp(self):
        """
        Set up a test. This is run before every test method.
        """
        os.chdir(TOP_LEVEL)
        self.hpss_path = None
        self.cache = "zstash"
        self.test_dir = "zstash_test"
        self.backup_dir = "zstash_test_backup"
        self.copy_dir = "zstash"

    def tearDown(self):
        """
        Tear down a test. This is run after every test method.

        After the script has failed or completed, remove all created files, even those on the HPSS repo.
        """
        os.chdir(TOP_LEVEL)
        print("Removing test files, both locally and at the HPSS repo")
        # self.cache may appear in any of these directories,
        # but should not appear at the same level as these.
        # Therefore, there is no need to explicitly remove it.
        for d in [self.test_dir, self.backup_dir]:
            if os.path.exists(d):
                shutil.rmtree(d)
        if self.hpss_path and self.hpss_path.lower() != "none":
            cmd = "hsi rm -R {}".format(self.hpss_path)
            run_cmd(cmd)

    def conditional_hpss_skip(self):
        skip_str = "Skipping HPSS tests."
        if SKIP_HPSS:
            self.skipTest("SKIP_HPSS is True. {}".format(skip_str))
        elif os.system("which hsi") != 0:
            self.skipTest("This system does not have hsi. {}".format(skip_str))

    def stop(self, error_message):
        """
        Report error and fail.
        """
        print_in_box(error_message)
        print("Current directory={}".format(os.getcwd()))
        os.chdir(TOP_LEVEL)
        print("New current directory={}".format(os.getcwd()))
        self.fail(error_message)
        # self.tearDown() will get called after this.

    def assertWorkspace(self):
        """
        Assert that the code is being run from the correct directory.
        """
        # To run the tests, we should be at the top of the repository.
        ls_results = os.listdir()
        if ("zstash" in ls_results) and (os.path.isdir("zstash")):
            if ("tests" in ls_results) and (os.path.isdir("tests")):
                return
        self.stop(
            f"Current directory={os.getcwd()} does not contain zstash and tests directories"
        )

    def assertEqualOrStop(self, actual, expected):
        """
        Assert values are equal. If not, then report error message and fail.
        """
        if actual != expected:
            self.stop("{} != {}".format(actual, expected))

    def check_strings(self, command, output, expected_present, expected_absent):
        """
        Check that `output` from `command` contains all strings in
        `expected_present` and no strings in `expected_absent`.
        """
        error_messages = []
        for string in expected_present:
            if string not in output:
                error_message = (
                    "This was supposed to be found, but was not: {}.".format(string)
                )
                error_messages.append(error_message)
        for string in expected_absent:
            if string in output:
                error_message = (
                    "This was not supposed to be found, but was: {}.".format(string)
                )
                error_messages.append(error_message)
        if error_messages:
            error_message = "Command=`{}`. Errors={}".format(command, error_messages)
            print_in_box(error_message)
            self.stop(error_message)

    def setupDirs(self, test_name):
        """
        Set up directories for testing.
        """
        if self.hpss_path:
            hpss_path = self.hpss_path
        else:
            raise ValueError("Invalid self.hpss_path={}".format(self.hpss_path))
        if hpss_path.lower() == "none":
            use_hpss = False
        else:
            use_hpss = True
        print_in_box(test_name)
        print_starred("Setup")
        self.assertWorkspace()
        # Create files and directories
        print("Creating files.")
        os.mkdir(self.test_dir)
        os.mkdir("{}/empty_dir".format(self.test_dir))
        os.mkdir("{}/dir".format(self.test_dir))

        write_file("{}/file0.txt".format(self.test_dir), "file0 stuff")
        write_file("{}/file_empty.txt".format(self.test_dir), "")
        write_file("{}/dir/file1.txt".format(self.test_dir), "file1 stuff")

        # Symbolic (soft) link (points to a file name which points to an inode)
        if not os.path.lexists("{}/file0_soft.txt".format(self.test_dir)):
            # https://stackoverflow.com/questions/54825010/why-does-os-symlink-uses-path-relative-to-destination
            # `os.symlink(pointed_to, soft_link)` will set `soft_link` to
            # look for `pointed_to` in `soft_link`'s directory.
            # Therefore, os.symlink('file1', 'dir/soft_link1') will soft link dir/soft_link1 to dir/file1.
            # But os.symlink('dir/file1`, 'dir/soft_link1') will soft link dir/soft_link1 to dir/dir/file1.

            # Create symbolic link pointing to test_dir/file0.txt named test_dir/file0_soft.txt
            os.symlink("file0.txt", "{}/file0_soft.txt".format(self.test_dir))

        # Bad symbolic (soft) link (points to a file name which points to an inode)
        if not os.path.lexists("{}/file0_soft_bad.txt".format(self.test_dir)):
            # Create symbolic link pointing to test_dir/file0_that_doesnt_exist.txt
            # named test_dir/file0_soft_bad.txt
            os.symlink(
                "file0_that_doesnt_exist.txt",
                "{}/file0_soft_bad.txt".format(self.test_dir),
            )

        # Hard link (points to an inode directly)
        if not os.path.lexists("{}/file0_hard.txt".format(self.test_dir)):
            # `os.link(first_pointer, second_pointer)` will set `second_pointer` to point to the
            # same inode as `first_pointer`.
            # Therefore, here we do want to list the relative paths for both.

            # Create hard link pointing to the same inode as test_dir/file0.txt,
            # named test_dir/file0_hard.txt
            os.link(
                "{}/file0.txt".format(self.test_dir),
                "{}/file0_hard.txt".format(self.test_dir),
            )
        return use_hpss

    def create(
        self,
        use_hpss,
        zstash_path,
        keep=False,
        cache=None,
        verbose=False,
        no_tars_md5=False,
    ):
        """
        Run `zstash create`.
        """
        if use_hpss:
            description_str = "Adding files to HPSS"
        else:
            description_str = "Adding files to local archive"
        print_starred(description_str)
        self.assertWorkspace()
        keep_option = " --keep" if keep else ""
        if cache:
            cache_option = " --cache={}".format(cache)
        else:
            cache_option = ""
        v_option = " -v" if verbose else ""
        no_tars_md5_option = " --no_tars_md5" if no_tars_md5 else ""
        cmd = "{}zstash create{}{}{}{} --hpss={} {}".format(
            zstash_path,
            keep_option,
            cache_option,
            v_option,
            no_tars_md5_option,
            self.hpss_path,
            self.test_dir,
        )
        output, err = run_cmd(cmd)
        if use_hpss:
            expected_present = ["Transferring file to HPSS"]
        else:
            expected_present = ["put: HPSS is unavailable"]
        expected_absent = ["ERROR"]
        if verbose:
            expected_present += ["DEBUG:"]
        else:
            expected_absent += ["DEBUG:"]
        self.check_strings(cmd, output + err, expected_present, expected_absent)

    def add_files(self, use_hpss, zstash_path, keep=False, cache=None):
        """
        Add files to the archive.
        """
        print_starred("Testing update with an actual change")
        self.assertWorkspace()
        if not os.path.exists("{}/dir2".format(self.test_dir)):
            os.mkdir("{}/dir2".format(self.test_dir))
        write_file("{}/dir2/file2.txt".format(self.test_dir), "file2 stuff")
        write_file("{}/dir/file1.txt".format(self.test_dir), "file1 stuff with changes")

        os.chdir(self.test_dir)
        keep_option = " --keep" if keep else ""
        if cache:
            cache_option = " --cache={}".format(cache)
        else:
            cache_option = ""
        cmd = "{}zstash update -v{}{} --hpss={}".format(
            zstash_path, keep_option, cache_option, self.hpss_path
        )
        output, err = run_cmd(cmd)
        os.chdir(TOP_LEVEL)
        if use_hpss:
            expected_present = ["Transferring file to HPSS"]
        else:
            expected_present = ["put: HPSS is unavailable"]
        expected_present += ["INFO: Creating new tar archive"]
        # Make sure none of the old files or directories are moved.
        expected_absent = ["ERROR", "file0", "file_empty", "empty_dir"]
        self.check_strings(cmd, output + err, expected_present, expected_absent)

        print("Adding more files to the HPSS archive.")
        self.assertWorkspace()
        write_file("{}/file3.txt".format(self.test_dir), "file3 stuff")
        os.chdir(self.test_dir)
        cmd = "{}zstash update{}{} --hpss={}".format(
            zstash_path, keep_option, cache_option, self.hpss_path
        )
        run_cmd(cmd)
        os.chdir(TOP_LEVEL)
        write_file("{}/file4.txt".format(self.test_dir), "file4 stuff")
        os.chdir(self.test_dir)
        cmd = "{}zstash update{}{} --hpss={}".format(
            zstash_path, keep_option, cache_option, self.hpss_path
        )
        run_cmd(cmd)
        os.chdir(TOP_LEVEL)
        write_file("{}/file5.txt".format(self.test_dir), "file5 stuff")
        os.chdir(self.test_dir)
        cmd = "{}zstash update{}{} --hpss={}".format(
            zstash_path, keep_option, cache_option, self.hpss_path
        )
        run_cmd(cmd)
        os.chdir(TOP_LEVEL)

    def extract(self, use_hpss, zstash_path, cache=None):
        """
        Extract files from the archive. This renames `self.test_dir` to `self.backup_dir`.
        """
        print_starred("Testing the extract functionality")
        self.assertWorkspace()
        os.rename(self.test_dir, self.backup_dir)
        os.mkdir(self.test_dir)
        os.chdir(self.test_dir)
        if not use_hpss:
            shutil.copytree(
                "{}/{}/{}".format(TOP_LEVEL, self.backup_dir, self.cache), self.copy_dir
            )
        if cache:
            cache_option = " --cache={}".format(cache)
        else:
            cache_option = ""
        cmd = "{}zstash extract{} --hpss={}".format(
            zstash_path, cache_option, self.hpss_path
        )
        output, err = run_cmd(cmd)
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
        expected_absent = ["ERROR"]
        if use_hpss:
            expected_absent.append("Not extracting")
        self.check_strings(cmd, output + err, expected_present, expected_absent)


if __name__ == "__main__":
    unittest.main()
