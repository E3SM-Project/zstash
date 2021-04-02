from __future__ import absolute_import, print_function

import logging
import os.path
from typing import Optional


# Class to hold configuration
class Config(object):
    path: Optional[str] = None
    hpss: Optional[str] = None
    maxsize: Optional[int] = None
    keep: Optional[bool] = None


def get_db_filename(cache):
    # Database filename
    return os.path.join(cache, "index.db")


# Block size
BLOCK_SIZE = 1024 * 1014

# Default sub-directory to hold cache
DEFAULT_CACHE = "zstash"

# Time tolerance (in seconds) for file modification time
TIME_TOL = 1.0

# Initialize config
config = Config()

# Initialize logger
logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)
