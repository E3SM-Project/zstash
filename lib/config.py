"""
A config object for Zstash
"""


class Config(object):
    """
    Class to hold configuration
    """

    def __init__(self, **kwargs):
        self._path = kwargs.get('path')
        self._hpss = kwargs.get('hpss')
        self._maxsize = kwargs.get('maxsize')
        self._keep = kwargs.get('keep')
        self._cache = kwargs.get('cache')
        self._db_filename = kwargs.get('db_filename')
        self._connection = kwargs.get('connection')
        self._cursor = kwargs.get('cursor')
        self._block_size = kwargs.get('block_size')
    
    def items(self):
        return {
            'path': self._path,
            'hpss': self._hpss,
            'maxsize': self._maxsize,
            'keep': self._keep,
            'cache': self._cache,
            'db_filename': self._db_filename,
            'block_size': self._block_size
        }.items()

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, path):
        self._path = path

    @property
    def hpss(self):
        return self._hpss

    @hpss.setter
    def hpss(self, hpss):
        self._hpss = hpss

    @property
    def maxsize(self):
        return self._maxsize

    @maxsize.setter
    def maxsize(self, maxsize):
        self._maxsize = maxsize

    @property
    def keep(self):
        return self._keep

    @keep.setter
    def keep(self, keep):
        self._keep = keep

    @property
    def cache(self):
        return self._cache

    @cache.setter
    def cache(self, cache):
        self._cache = cache

    @property
    def db_filename(self):
        return self._db_filename

    @db_filename.setter
    def db_filename(self, db_filename):
        self._db_filename = db_filename

    @property
    def connection(self):
        return self._connection

    @connection.setter
    def connection(self, connection):
        self._connection = connection

    @property
    def cursor(self):
        return self._cursor

    @cursor.setter
    def cursor(self, cursor):
        self._cursor = cursor

    @property
    def block_size(self):
        return self._block_size

    @block_size.setter
    def block_size(self, block_size):
        self._block_size = block_size
