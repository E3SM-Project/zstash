from __future__ import absolute_import, print_function

from . import extract


def check():
    """
    Check that the files in a given HPSS archive are valid.
    """
    # This basically just goes through the process of extracting the files,
    # but doesn't actually save the output.
    extract.extract(do_extract_files=False)
