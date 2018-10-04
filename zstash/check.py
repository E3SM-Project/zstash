import logging
import extract

def check():
    """
    Check that the files in a given HPSS archive are valid.
    """
    # This basically just goes through the process of extracting the files,
    # but doesn't actually save the output.
    logging.debug('Checking the files in the HPSS archive.')
    extract.extract(keep_files=False)
