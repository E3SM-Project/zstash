from __future__ import print_function, absolute_import

import os.path
import logging

# Class to hold configuration
class Config(object):
    path = None
    hpss = None
    maxsize = None
    keep = None

def get_db_filename(cache):
    # Database filename 
    return os.path.join(cache, 'index.db')

# Block size
BLOCK_SIZE = 1024*1014

# Default sub-directory to hold cache
DEFAULT_CACHE = 'zstash'

# Time tolerance (in seconds) for file modification time
TIME_TOL = 1.0

# Initialize config
config = Config()

# Initialize logger
logger = logging.basicConfig(format='%(levelname)s: %(message)s',
                        level=logging.INFO)
logger = logging.getLogger(__name__)

