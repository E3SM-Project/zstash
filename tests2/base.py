"""
Run the test suite with `python -m unittest tests2/test_*.py`

tests2/ is a successor testing directory to tests/
All new tests should be written in tests2/
tests/ groups testing by zstash command (e.g., `create`, `extract`)
tests2/ groups testing by more logical workflows that test multiple zstash commands.

The goal of tests2/ is to be able to follow the commands as if you were just reading a bash script.
"""

import os
import shutil
import stat
import subprocess
import unittest
from typing import List, Tuple

# https://bugs.python.org/issue43743
# error: Module has no attribute "_USE_CP_SENDFILE"
shutil._USE_CP_SENDFILE = False  # type: ignore

# Top level directory.
# This should be the zstash repo itself. It should thus end in `zstash`.
# This is used to ensure we are changing into the correct subdirectories and parent directories.
TOP_LEVEL = os.getcwd()


def create_directories(dir_names: List[str]):
    for dir in dir_names:
        os.mkdir(dir)


def write_files(name_content_tuples: List[Tuple[str, str]]):
    for name, contents in name_content_tuples:
        with open(name, "w") as f:
            f.write(contents)


def create_links(link_tuples: List[Tuple[str, str]], do_symlink: bool = True):
    if do_symlink:
        for pointed_to, soft_link in link_tuples:
            # soft_link will point to pointed_to, which is a file name which itself points to a inode.
            os.symlink(pointed_to, soft_link)
    else:
        for first_pointer, second_pointer in link_tuples:
            # first_pointer and second_pointer will both point to the same inode.
            os.link(first_pointer, second_pointer)


def run_cmd(cmd):
    """
    Run a command. Then print and return the stdout and stderr.
    """
    print("+ {}".format(cmd))
    # `cmd` must be a list
    if isinstance(cmd, str):
        cmd = cmd.split()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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


def print_in_box(string):
    """
    Print with stars above and below.
    """
    print("*" * 40)
    print(string)
    print("*" * 40)


class TestZstash(unittest.TestCase):
    """
    Base test class.
    """

    def setUp(self):
        """
        Set up a test. This is run before every test method.
        """
        os.chdir(TOP_LEVEL)
        # The directory we'll be working in.
        self.work_dir = "zstash_work_dir"
        # The HPSS path
        self.hpss_path = None
        # The mtime to compare back to, to make sure we're not modifying the source directory.
        self.mtime_start = None

    def tearDown(self):
        """
        Tear down a test. This is run after every test method.

        After the script has failed or completed, remove all created files, even those on the HPSS repo.
        """
        os.chdir(TOP_LEVEL)
        print("Removing test files, both locally and at the HPSS repo")
        for d in [self.work_dir]:
            if os.path.exists(d):
                shutil.rmtree(d)
        if self.hpss_path and self.hpss_path.lower() != "none":
            cmd = "hsi rm -R {}".format(self.hpss_path)
            run_cmd(cmd)

    def assert_source_unchanged(self):
        """
        Assert that the source directory has not been changed.
        """
        mtime_current = os.stat(f"{TOP_LEVEL}/{self.work_dir}/zstash_src")[
            stat.ST_MTIME
        ]
        if self.mtime_start != mtime_current:
            self.stop(
                f"Source directory was modified! {self.mtime_start} != {mtime_current}"
            )

    def assert_file_first_line(self, file_name, expected):
        with open(file_name) as f:
            output = f.readline()
            self.assertEqual(output, expected)

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

    def check_strings(
        self,
        command: str,
        output: str,
        expected_present: List[str],
        expected_absent: List[str],
    ):
        """
        Check that `output` from `command` contains all strings in
        `expected_present` and no strings in `expected_absent`.
        """
        error_messages = []
        for string in expected_present:
            if string not in output:
                error_message = f"This was supposed to be found, but was not: {string}."
                error_messages.append(error_message)
        for string in expected_absent:
            if string in output:
                error_message = f"This was not supposed to be found, but was: {string}."
                error_messages.append(error_message)
        if error_messages:
            error_message = f"ERROR: Command=`{command}`. Errors={error_messages}"
            print_in_box(error_message)
            self.stop(error_message)

    def setup_dirs(self, include_broken_symlink=True):
        """
        Set up directories for testing.
        """
        create_directories(
            [
                self.work_dir,
                f"{self.work_dir}/zstash_src/",
                f"{self.work_dir}/zstash_src/empty_dir",
                f"{self.work_dir}/zstash_src/dir1",
                f"{self.work_dir}/zstash_src/dir2",
                f"{self.work_dir}/zstash_not_src",
                f"{self.work_dir}/zstash_extracted",
            ]
        )
        write_files(
            [
                (f"{self.work_dir}/zstash_src/file0.txt", "file0 stuff"),
                (f"{self.work_dir}/zstash_src/file_empty.txt", ""),
                (f"{self.work_dir}/zstash_src/dir1/file1.txt", "file1 stuff"),
                (
                    f"{self.work_dir}/zstash_not_src/file_not_included.txt",
                    "file_not_included stuff",
                ),
                (
                    f"{self.work_dir}/zstash_not_src/this_will_be_deleted.txt",
                    "deleted stuff",
                ),
            ]
        )
        create_links(
            [
                # https://stackoverflow.com/questions/54825010/why-does-os-symlink-uses-path-relative-to-destination
                # `os.symlink(pointed_to, soft_link)` will set `soft_link` to
                # look for `pointed_to` in `soft_link`'s directory.
                # Therefore, os.symlink('original_file', 'dir/soft_link') will soft link dir/soft_link to dir/original_file.
                # But os.symlink('dir/original_file`, 'dir/soft_link') will soft link dir/soft_link to dir/dir/original_file!
                # That is, the link's directory will always be used as the base path for the original file.
                # 1) Link to a file in the same subdirectory
                ("file0.txt", f"{self.work_dir}/zstash_src/file0_soft.txt"),
                # There is a way around this, though: use an absolute path.
                # 2) Link to a file in a different subdirectory
                (
                    f"{TOP_LEVEL}/{self.work_dir}/zstash_src/dir1/file1.txt",
                    f"{self.work_dir}/zstash_src/dir2/file1_soft.txt",
                ),
                # 3) Link to a file outside the directory to be archived
                (
                    f"{TOP_LEVEL}/{self.work_dir}/zstash_not_src/file_not_included.txt",
                    f"{self.work_dir}/zstash_src/file_not_included_soft.txt",
                ),
            ]
        )
        # We can do steps 1-3 above but for hard links:
        create_links(
            [
                # Note that here, we do need to include the relative path for both.
                (
                    f"{self.work_dir}/zstash_src/file0.txt",
                    f"{self.work_dir}/zstash_src/file0_hard.txt",
                ),
                (
                    f"{TOP_LEVEL}/{self.work_dir}/zstash_src/dir1/file1.txt",
                    f"{self.work_dir}/zstash_src/dir2/file1_hard.txt",
                ),
                (
                    f"{TOP_LEVEL}/{self.work_dir}/zstash_not_src/file_not_included.txt",
                    f"{self.work_dir}/zstash_src/file_not_included_hard.txt",
                ),
                # Also include a broken hard link
                (
                    f"{self.work_dir}/zstash_not_src/this_will_be_deleted.txt",
                    f"{self.work_dir}/zstash_src/original_was_deleted_hard.txt",
                ),
            ],
            do_symlink=False,
        )
        if include_broken_symlink:
            os.symlink(
                f"{self.work_dir}/zstash_not_src/this_will_be_deleted.txt",
                f"{self.work_dir}/zstash_src/original_was_deleted_soft.txt",
            )
        os.remove(f"{self.work_dir}/zstash_not_src/this_will_be_deleted.txt")
        self.mtime_start = os.stat(f"{TOP_LEVEL}/{self.work_dir}/zstash_src")[
            stat.ST_MTIME
        ]


if __name__ == "__main__":
    unittest.main()
