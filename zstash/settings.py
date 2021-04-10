from __future__ import absolute_import, print_function

import datetime
import logging
import os.path
from typing import Optional, Tuple


# Class to hold configuration
class Config(object):
    path: Optional[str] = None
    hpss: Optional[str] = None
    maxsize: Optional[int] = None
    keep: Optional[bool] = None


def get_db_filename(cache: str) -> str:
    # Database filename
    return os.path.join(cache, "index.db")


# Block size
BLOCK_SIZE: int = 1024 * 1014

# Default sub-directory to hold cache
DEFAULT_CACHE: str = "zstash"

# Time tolerance (in seconds) for file modification time
TIME_TOL: float = 1.0

# Initialize config
config: Config = Config()

# Initialize logger
logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)
logger: logging.Logger = logging.getLogger(__name__)

# Type aliases
TupleFilesRow = Tuple[int, str, int, datetime.datetime, Optional[str], str, int]
# No corresponding class needed for this tuple.
TupleFilesRowNoId = Tuple[str, int, datetime.datetime, Optional[str], str, int]


# Corresponding class to make accessing variables easier
class FilesRow(object):
    def __init__(self, t: TupleFilesRow):
        self.identifier: int = t[0]
        self.name: str = t[1]
        self.size: int = t[2]
        self.mtime: datetime.datetime = t[3]
        self.md5: Optional[str] = t[4]
        self.tar: str = t[5]
        self.offset: int = t[6]

    def to_tuple(self) -> TupleFilesRow:
        return (
            self.identifier,
            self.name,
            self.size,
            self.mtime,
            self.md5,
            self.tar,
            self.offset,
        )
