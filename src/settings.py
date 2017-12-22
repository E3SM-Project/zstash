import os.path


# Class to hold configuration
class Config(object):
    path = None
    hpss = None
    maxsize = None
    keep = None


# Block size
BLOCK_SIZE = 1024*1014

# Sub-directory to hold cache
CACHE = 'zstash'

# Database filename
DB_FILENAME = os.path.join(CACHE, 'index.db')

# Initialize config
config = Config()
