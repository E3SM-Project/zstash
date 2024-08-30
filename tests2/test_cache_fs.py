import os
import unittest

from tests2.base import TOP_LEVEL, TestZstash, run_cmd


class TestCacheFs(TestZstash):
    """
    Test this parameter combination:
    cache, follow_symlinks
    """

    def test_hpss_none_fs_on(self):
        # Run test cases described at https://github.com/E3SM-Project/zstash/discussions/346
        # Post-343, using `follow_symlinks`
        # Cases:
        # internal symlink (in same dir, in different dir), external symlink
        # internal hard link (in same dir, in different dir), external hard link, broken hard link
        self.setup_dirs(include_broken_symlink=False)
        os.chdir(f"{TOP_LEVEL}/{self.work_dir}/zstash_src/")

        # Test zstash_src before create
        self.assertTrue(os.path.islink("file0_soft.txt"))
        self.assertTrue(os.path.islink("dir2/file1_soft.txt"))
        self.assertTrue(os.path.islink("file_not_included_soft.txt"))
        self.assertFalse(os.path.islink("file0_hard.txt"))
        self.assertFalse(os.path.islink("dir2/file1_hard.txt"))
        self.assertFalse(os.path.islink("file_not_included_hard.txt"))
        self.assertFalse(os.path.islink("original_was_deleted_hard.txt"))

        os.chdir(f"{TOP_LEVEL}/{self.work_dir}/")
        cmd = f"zstash create --hpss=none --cache={TOP_LEVEL}/{self.work_dir}/test_cache --follow-symlinks zstash_src"
        run_cmd(cmd)
        os.chdir(f"{TOP_LEVEL}/{self.work_dir}/zstash_src/")

        # Test zstash_src after create
        # Running `create` should not alter the source directory.
        self.assertTrue(os.path.islink("file0_soft.txt"))
        self.assertTrue(os.path.islink("dir2/file1_soft.txt"))
        self.assertTrue(os.path.islink("file_not_included_soft.txt"))
        self.assertFalse(os.path.islink("file0_hard.txt"))
        self.assertFalse(os.path.islink("dir2/file1_hard.txt"))
        self.assertFalse(os.path.islink("file_not_included_hard.txt"))
        self.assertFalse(os.path.islink("original_was_deleted_hard.txt"))

        os.chdir("../zstash_extracted")
        cmd = (
            f"zstash extract --hpss=none --cache={TOP_LEVEL}/{self.work_dir}/test_cache"
        )
        run_cmd(cmd)

        # Test extraction from zstash_archive
        # The extracted directory should have actual files, not links
        self.assertFalse(os.path.islink("file0_soft.txt"))
        self.assertFalse(os.path.islink("dir2/file1_soft.txt"))
        self.assertFalse(os.path.islink("file_not_included_soft.txt"))
        self.assertFalse(os.path.islink("file0_hard.txt"))
        self.assertFalse(os.path.islink("dir2/file1_hard.txt"))
        self.assertFalse(os.path.islink("file_not_included_hard.txt"))
        self.assertFalse(os.path.islink("original_was_deleted_hard.txt"))

        # Test file content
        self.assert_file_first_line("file0_soft.txt", "file0 stuff")
        self.assert_file_first_line("dir2/file1_soft.txt", "file1 stuff")
        self.assert_file_first_line(
            "file_not_included_soft.txt", "file_not_included stuff"
        )
        self.assert_file_first_line("file0_hard.txt", "file0 stuff")
        self.assert_file_first_line("dir2/file1_hard.txt", "file1 stuff")
        self.assert_file_first_line(
            "file_not_included_hard.txt", "file_not_included stuff"
        )
        self.assert_file_first_line("original_was_deleted_hard.txt", "deleted stuff")

        os.chdir("..")
        self.assert_source_unchanged()

    def test_hpss_none_fs_on_broken_symlink(self):
        # Run test cases described at https://github.com/E3SM-Project/zstash/discussions/346
        # Post-343, using `follow_symlinks`
        # Cases:
        # broken symlink
        self.setup_dirs(include_broken_symlink=True)
        os.chdir(f"{TOP_LEVEL}/{self.work_dir}/zstash_src/")

        # Test zstash_src before create
        self.assertTrue(os.path.islink("original_was_deleted_soft.txt"))

        os.chdir(f"{TOP_LEVEL}/{self.work_dir}/")
        cmd = f"zstash create --hpss=none --cache={TOP_LEVEL}/{self.work_dir}/test_cache --follow-symlinks zstash_src"
        run_cmd(cmd)
        os.chdir(f"{TOP_LEVEL}/{self.work_dir}/zstash_src/")

        # Test zstash_src after create
        # Running `create` should not alter the source directory.
        self.assertTrue(os.path.islink("original_was_deleted_soft.txt"))

        os.chdir("../zstash_extracted")
        cmd = (
            f"zstash extract --hpss=none --cache={TOP_LEVEL}/{self.work_dir}/test_cache"
        )
        _, err = run_cmd(cmd)
        # This is ultimately caused by:
        # `Exception: Archive creation failed due to broken symlink.`
        # But that doesn't seem to propagate to `err`
        self.check_strings(
            cmd, err, ["FileNotFoundError: There was nothing to extract."], []
        )
        self.assert_source_unchanged()

    def test_hpss_none_fs_off(self):
        #
        self.setup_dirs(include_broken_symlink=False)
        os.chdir(f"{TOP_LEVEL}/{self.work_dir}/zstash_src/")

        # Test zstash_src before create
        self.assertTrue(os.path.islink("file0_soft.txt"))
        self.assertTrue(os.path.islink("dir2/file1_soft.txt"))
        self.assertTrue(os.path.islink("file_not_included_soft.txt"))
        self.assertFalse(os.path.islink("file0_hard.txt"))
        self.assertFalse(os.path.islink("dir2/file1_hard.txt"))
        self.assertFalse(os.path.islink("file_not_included_hard.txt"))
        self.assertFalse(os.path.islink("original_was_deleted_hard.txt"))

        os.chdir(f"{TOP_LEVEL}/{self.work_dir}/")
        cmd = f"zstash create --hpss=none --cache={TOP_LEVEL}/{self.work_dir}/test_cache zstash_src"
        run_cmd(cmd)
        os.chdir(f"{TOP_LEVEL}/{self.work_dir}/zstash_src/")

        # Test zstash_src after create
        # Running `create` should not alter the source directory.
        self.assertTrue(os.path.islink("file0_soft.txt"))
        self.assertTrue(os.path.islink("dir2/file1_soft.txt"))
        self.assertTrue(os.path.islink("file_not_included_soft.txt"))
        self.assertFalse(os.path.islink("file0_hard.txt"))
        self.assertFalse(os.path.islink("dir2/file1_hard.txt"))
        self.assertFalse(os.path.islink("file_not_included_hard.txt"))
        self.assertFalse(os.path.islink("original_was_deleted_hard.txt"))

        os.chdir("../zstash_extracted")
        cmd = (
            f"zstash extract --hpss=none --cache={TOP_LEVEL}/{self.work_dir}/test_cache"
        )
        run_cmd(cmd)

        # Test extraction from zstash_archive
        self.assertTrue(os.path.islink("file0_soft.txt"))  # DIFFERENT from fs_on
        self.assertTrue(os.path.islink("dir2/file1_soft.txt"))  # DIFFERENT from fs_on
        self.assertTrue(
            os.path.islink("file_not_included_soft.txt")
        )  # DIFFERENT from fs_on
        self.assertFalse(os.path.islink("file0_hard.txt"))
        self.assertFalse(os.path.islink("dir2/file1_hard.txt"))
        self.assertFalse(os.path.islink("file_not_included_hard.txt"))
        self.assertFalse(os.path.islink("original_was_deleted_hard.txt"))

        # Test file content
        self.assert_file_first_line("file0_soft.txt", "file0 stuff")
        self.assert_file_first_line("dir2/file1_soft.txt", "file1 stuff")
        self.assert_file_first_line(
            "file_not_included_soft.txt", "file_not_included stuff"
        )
        self.assert_file_first_line("file0_hard.txt", "file0 stuff")
        self.assert_file_first_line("dir2/file1_hard.txt", "file1 stuff")
        self.assert_file_first_line(
            "file_not_included_hard.txt", "file_not_included stuff"
        )
        self.assert_file_first_line("original_was_deleted_hard.txt", "deleted stuff")

        os.chdir("..")
        self.assert_source_unchanged()

    def test_hpss_none_fs_off_broken_symlink(self):
        self.setup_dirs(include_broken_symlink=True)
        os.chdir(f"{TOP_LEVEL}/{self.work_dir}/zstash_src/")

        # Test zstash_src before create
        self.assertTrue(os.path.islink("original_was_deleted_soft.txt"))

        os.chdir(f"{TOP_LEVEL}/{self.work_dir}/")
        cmd = f"zstash create --hpss=none --cache={TOP_LEVEL}/{self.work_dir}/test_cache zstash_src"
        run_cmd(cmd)
        os.chdir(f"{TOP_LEVEL}/{self.work_dir}/zstash_src/")

        # Test zstash_src after create
        # Running `create` should not alter the source directory.
        self.assertTrue(os.path.islink("original_was_deleted_soft.txt"))

        os.chdir("../zstash_extracted")
        cmd = (
            f"zstash extract --hpss=none --cache={TOP_LEVEL}/{self.work_dir}/test_cache"
        )
        run_cmd(cmd)
        # With fs off, this command completes successfully.

        # Test extraction from zstash_archive
        self.assertTrue(os.path.islink("original_was_deleted_soft.txt"))

        # Test file content
        self.assertFalse(os.path.isfile("original_was_deleted_soft.txt"))

        os.chdir("..")
        self.assert_source_unchanged()


if __name__ == "__main__":
    unittest.main()
