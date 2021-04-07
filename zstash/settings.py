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
# TODO: would it be better to have these in utils.py?

# A row of the 'files' table:
# id     -- integer
# name   -- text
# size   -- integer
# mtime  -- timestamp
# md5    -- text
# tar    -- text
# offset -- integer
FilesRow = Tuple[int, str, int, datetime.datetime, str, str, int]
FilesRowOptionalHash = Tuple[int, str, int, datetime.datetime, Optional[str], str, int]
FilesRowNoId = Tuple[str, int, datetime.datetime, Optional[str], str, int]
# TODO: make these `namedtuple`s
