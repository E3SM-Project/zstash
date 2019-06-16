from __future__ import print_function, absolute_import

import logging
from . import extract

def check():
    """
    Check that the files in a given HPSS archive are valid.
    """
    # This basically just goes through the process of extracting the files,
    # but doesn't actually save the output.
    extract.extract(keep_files=False)
