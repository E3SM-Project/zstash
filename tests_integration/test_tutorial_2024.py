# Test workflow similar to the 2024 tutorial.
# https://github.com/E3SM-Project/zstash/blob/add-tutorial-materials/tutorial_materials/zstash_demo.md


import os

from tests_integration.utils import TestZstash, run_cmd


class TestTutorial2024(TestZstash):

    def test_tutorial_2024(self):
        # This test can only be run on NERSC, using NERSC HPSS.
        self.conditional_hpss_skip()
        self.setup_dirs()
        os.chdir(f"{self.work_dir}")

        files_to_include = "dir1/*.txt"  # Should match file1.txt
        cmd = f"zstash create --hpss={self.hpss_dir} --cache={self.cache_dir} --include={files_to_include} {self.dir_to_archive}"
        output, err = run_cmd(cmd)
        expected_present = [
            "Creating new tar archive",
            "Archiving dir1/file1.txt",
            "Transferring file to HPSS",
            "Completed archive file",
        ]
        expected_absent = [
            "ERROR",
            "Archiving file0.txt",
            "Archiving file_empty.txt",
            "Archiving file0_soft.txt",
            "Archiving dir2/file1_soft.txt",
            "Archiving file_not_included_soft.txt",
            "Archiving file0_hard.txt",
            "Archiving dir2/file1_hard.txt",
            "Archiving file_not_included_hard.txt",
            "Archiving original_was_deleted_hard.txt",
        ]
        self.check_strings(cmd, output + err, expected_present, expected_absent)

        os.mkdir("check_output")
        os.chdir("check_output")
        cmd = f"zstash check --hpss={self.hpss_dir}"
        output, err = run_cmd(cmd)
        expected_present = [
            "Transferring file from HPSS",
            "Opening tar archive",
            "Checking dir1/file1.txt",
            "No failures detected when checking the files",
        ]
        expected_absent = [
            "ERROR",
            "Checking file0.txt",
            "Checking file_empty.txt",
            "Checking file0_soft.txt",
            "Checking dir2/file1_soft.txt",
            "Checking file_not_included_soft.txt",
            "Checking file0_hard.txt",
            "Checking dir2/file1_hard.txt",
            "Checking file_not_included_hard.txt",
            "Checking original_was_deleted_hard.txt",
        ]
        self.check_strings(cmd, output + err, expected_present, expected_absent)
        os.chdir(f"{self.work_dir}")

        os.mkdir("ls_output")
        os.chdir("ls_output")
        cmd = f"zstash ls --hpss={self.hpss_dir}"
        output, err = run_cmd(cmd)
        expected_present = [
            "dir1/file1.txt",
        ]
        expected_absent = [
            "ERROR",
            "file0.txt",
            "file_empty.txt",
            "file0_soft.txt",
            "dir2/file1_soft.txt",
            "file_not_included_soft.txt",
            "file0_hard.txt",
            "dir2/file1_hard.txt",
            "file_not_included_hard.txt",
            "original_was_deleted_hard.txt",
        ]
        self.check_strings(cmd, output + err, expected_present, expected_absent)
        os.chdir(f"{self.work_dir}")

        os.mkdir("extract_output")
        os.chdir("extract_output")
        cmd = f"zstash extract --hpss={self.hpss_dir}"
        output, err = run_cmd(cmd)
        expected_present = [
            "Transferring file from HPSS",
            "Opening tar archive",
            "Extracting dir1/file1.txt",
            "No failures detected when extracting the files",
        ]
        expected_absent = [
            "ERROR",
            "Extracting file0.txt",
            "Extracting file_empty.txt",
            "Extracting file0_soft.txt",
            "Extracting dir2/file1_soft.txt",
            "Extracting file_not_included_soft.txt",
            "Extracting file0_hard.txt",
            "Extracting dir2/file1_hard.txt",
            "Extracting file_not_included_hard.txt",
            "Extracting original_was_deleted_hard.txt",
        ]
        self.check_strings(cmd, output + err, expected_present, expected_absent)
        os.chdir(f"{self.work_dir}")
