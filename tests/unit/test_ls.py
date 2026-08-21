import datetime
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from zstash.ls import ls_tars_database
from zstash.settings import FilesRow


class TestLsTarsDatabase(unittest.TestCase):
    def setUp(self):
        # Create an in-memory SQLite database with files and tars tables
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.tmp.name
        self.tmp.close()

        con = sqlite3.connect(self.db_path)
        cur = con.cursor()
        cur.execute(
            "CREATE TABLE tars (id INTEGER, name TEXT, size INTEGER, md5 TEXT)"
        )
        cur.execute(
            "INSERT INTO tars VALUES (1, '000000.tar', 10240, 'abc123')"
        )
        cur.execute(
            "INSERT INTO tars VALUES (2, '000001.tar', 20480, 'def456')"
        )
        cur.execute(
            "INSERT INTO tars VALUES (3, '000002.tar', 30720, 'ghi789')"
        )
        con.commit()
        con.close()

    def tearDown(self):
        os.unlink(self.db_path)

    def _make_args(self, long: bool = False):
        from unittest.mock import MagicMock

        args = MagicMock()
        args.long = long
        return args

    def test_ls_tars_database_filtered(self):
        """Only tars containing matched files should be returned."""
        args = self._make_args()
        with patch("zstash.ls.get_db_filename", return_value=self.db_path):
            result = ls_tars_database(args, "zstash", ["000000.tar", "000002.tar"])

        names = [r.name for r in result]
        self.assertIn("000000.tar", names)
        self.assertIn("000002.tar", names)
        self.assertNotIn("000001.tar", names)

    def test_ls_tars_database_unfiltered(self):
        """When tar_names is None, all tars should be returned."""
        args = self._make_args()
        with patch("zstash.ls.get_db_filename", return_value=self.db_path):
            result = ls_tars_database(args, "zstash", None)

        names = [r.name for r in result]
        self.assertIn("000000.tar", names)
        self.assertIn("000001.tar", names)
        self.assertIn("000002.tar", names)

    def test_tar_names_from_file_matches(self):
        """The unique tar names extracted from FilesRow matches are correct."""
        now = datetime.datetime(2024, 1, 1)
        row1 = FilesRow((1, "archive/file_1850.nc", 100, now, "md5a", "000000.tar", 0))
        row2 = FilesRow((2, "archive/file_1851.nc", 100, now, "md5b", "000000.tar", 512))
        row3 = FilesRow((3, "archive/file_1900.nc", 100, now, "md5c", "000005.tar", 0))

        matches = [row1, row2, row3]
        tar_names = sorted(set(m.tar for m in matches))
        self.assertEqual(tar_names, ["000000.tar", "000005.tar"])


if __name__ == "__main__":
    unittest.main()
