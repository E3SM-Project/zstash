from __future__ import absolute_import, print_function

import datetime
import logging
from typing import Optional, Tuple

# Block size
BLOCK_SIZE: int = 1024 * 1014

# Time tolerance (in seconds) for file modification time
TIME_TOL: float = 1.0

# Initialize logger
logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)
logger: logging.Logger = logging.getLogger(__name__)

# Type aliases
TupleFilesRow = Tuple[int, str, int, datetime.datetime, Optional[str], str, int]
TupleTarsRow = Tuple[int, str, int, str]
# No corresponding class needed for these tuples.
TupleFilesRowNoId = Tuple[str, int, datetime.datetime, Optional[str], str, int]
TupleTarsRowNoId = Tuple[str, int, Optional[str]]


# Corresponding classes to make accessing variables easier
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


class TarsRow(object):
    def __init__(self, t: TupleTarsRow):
        self.identifier: int = t[0]
        self.name: str = t[1]
        self.size: int = t[2]
        self.md5: str = t[3]

    def to_tuple(self) -> TupleTarsRow:
        return (self.identifier, self.name, self.size, self.md5)
