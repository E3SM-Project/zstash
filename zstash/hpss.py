from __future__ import absolute_import, print_function

import os.path
import subprocess
from typing import List, Optional

from six.moves.urllib.parse import urlparse

from .globus import globus_transfer
from .settings import logger
from .transfer_tracking import GlobusConfig, TransferBatch, TransferManager
from .utils import run_command, ts_utc

# ---------------------------------------------------------------------------
# Internal helpers for each transfer variant
# ---------------------------------------------------------------------------


def _local_put(
    file_path: str,
    cache: str,
    is_index: bool,
) -> None:
    """
    Handle hpss='none' for a put: do nothing for the index DB or for tar files
    when keep=True.  For tar files (keep=False implied by caller), remove write
    permissions so the local archive is read-only.
    """
    if is_index:
        # Nothing to do; the DB is always kept locally.
        return

    # Remove write permissions from the tar file so the local-only archive
    # behaves like an immutable store.
    logger.info("put (local): removing write permissions from {}".format(file_path))

    display_cmd: List[str] = "stat --format '%a' {}".format(file_path).split()
    original_mode: bytes = subprocess.check_output(display_cmd).strip()
    logger.info("{!r} original mode={!r}".format(file_path, original_mode))

    subprocess.check_output("chmod ugo-w {}".format(file_path).split())

    new_mode: bytes = subprocess.check_output(display_cmd).strip()
    logger.info("{!r} new mode={!r}".format(file_path, new_mode))


def _hsi_transfer(hpss: str, file_path: str, transfer_type: str) -> None:
    """Transfer a single file to or from HPSS using the hsi command-line tool."""
    if transfer_type == "put":
        transfer_word, transfer_command = "to", "put"
    else:
        transfer_word, transfer_command = "from", "get"

    _, name = os.path.split(file_path)
    logger.info("Transferring file {} HPSS: {}".format(transfer_word, file_path))

    command = 'hsi -q "cd {}; {} {}"'.format(hpss, transfer_command, name)
    error_str = "Transferring file {} HPSS: {}".format(transfer_word, name)
    run_command(command, error_str)


def _globus_put_or_get(
    hpss: str,
    file_path: str,
    transfer_type: str,
    keep: bool,
    non_blocking: bool,
    is_index: bool,
    transfer_manager: TransferManager,
) -> None:
    """
    Transfer a file using the Globus Transfer Service, then delete local tar
    files for any batches whose transfer has succeeded.
    """
    url = urlparse(hpss)
    endpoint: str = url.netloc
    url_path: str = url.path
    path, name = os.path.split(file_path)

    if not keep and not is_index:
        # Track this tar for deletion once its Globus transfer succeeds.
        transfer_manager.batches[-1].local_paths_to_delete.append(file_path)
        logger.debug(
            f"{ts_utc()}: Tracking {file_path} for deletion after transfer; "
            f"batch now has {len(transfer_manager.batches[-1].local_paths_to_delete)} file(s)"
        )

    # hsi requires us to be in the directory containing the file.
    cwd = os.getcwd()
    if path:
        if transfer_type == "get" and not os.path.isdir(path):
            os.makedirs(path)
        os.chdir(path)

    logger.info(f"{ts_utc()}: globus_transfer() -> name={name!r}")
    globus_transfer(
        transfer_manager, endpoint, url_path, name, transfer_type, non_blocking
    )
    logger.info(f"{ts_utc()}: globus_transfer() returned for name={name!r}")

    if path:
        os.chdir(cwd)

    if transfer_type == "put" and not keep:
        transfer_manager.delete_successfully_transferred_files()


def _hsi_put_or_get(
    hpss: str,
    file_path: str,
    transfer_type: str,
    keep: bool,
    is_index: bool,
    transfer_manager: TransferManager,
) -> None:
    """
    Transfer a file using the hsi command-line tool, then delete local tar
    files for any batches whose transfer has succeeded.
    """
    path, _ = os.path.split(file_path)

    if not keep and not is_index:
        transfer_manager.batches[-1].local_paths_to_delete.append(file_path)
        logger.debug(
            f"{ts_utc()}: Tracking {file_path} for deletion after transfer; "
            f"batch now has {len(transfer_manager.batches[-1].local_paths_to_delete)} file(s)"
        )

    cwd = os.getcwd()
    if path:
        if transfer_type == "get" and not os.path.isdir(path):
            os.makedirs(path)
        os.chdir(path)

    _hsi_transfer(hpss, file_path, transfer_type)

    if path:
        os.chdir(cwd)

    if transfer_type == "put" and not keep:
        transfer_manager.delete_successfully_transferred_files()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def hpss_transfer(
    hpss: str,
    file_path: str,
    transfer_type: str,
    cache: str,
    keep: bool = False,
    non_blocking: bool = False,
    is_index: bool = False,
    transfer_manager: Optional[TransferManager] = None,
):
    if not transfer_manager:
        transfer_manager = TransferManager()

    url = urlparse(hpss)
    scheme = url.scheme

    # Ensure there is an open batch to add files to.  A new batch is needed
    # when none exists yet, or when the last batch has already been submitted
    # (task_id is set), meaning we are starting a new submission window.
    if not transfer_manager.batches or transfer_manager.batches[-1].task_id:
        new_batch = TransferBatch()
        new_batch.is_globus = scheme == "globus"
        transfer_manager.batches.append(new_batch)
        logger.debug(
            f"{ts_utc()}: Created new TransferBatch "
            f"(total batches: {len(transfer_manager.batches)})"
        )

    if hpss == "none":
        logger.info("{}: HPSS is unavailable".format(transfer_type))
        if transfer_type == "put":
            _local_put(file_path, cache, is_index)
        # get with hpss='none' means the file is already local; nothing to do.
        return

    if scheme == "globus":
        if not transfer_manager.globus_config:
            transfer_manager.globus_config = GlobusConfig()
        _globus_put_or_get(
            hpss,
            file_path,
            transfer_type,
            keep,
            non_blocking,
            is_index,
            transfer_manager,
        )
    else:
        _hsi_put_or_get(
            hpss, file_path, transfer_type, keep, is_index, transfer_manager
        )


def hpss_put(
    hpss: str,
    file_path: str,
    cache: str,
    transfer_manager: TransferManager,
    keep: bool = True,
    non_blocking: bool = False,
    is_index: bool = False,
):
    """Put a file to the HPSS archive."""
    hpss_transfer(
        hpss,
        file_path,
        "put",
        cache,
        keep,
        non_blocking,
        is_index,
        transfer_manager,
    )


def hpss_get(
    hpss: str,
    file_path: str,
    cache: str,
    transfer_manager: Optional[TransferManager] = None,
):
    """Get a file from the HPSS archive."""
    url = urlparse(hpss)
    if not transfer_manager:
        transfer_manager = TransferManager()
    if url.scheme == "globus" and not transfer_manager.globus_config:
        transfer_manager.globus_config = GlobusConfig()
    hpss_transfer(
        hpss, file_path, "get", cache, False, transfer_manager=transfer_manager
    )


def hpss_chgrp(hpss: str, group: str, recurse: bool = False):
    """Change the group of the HPSS archive."""
    if hpss == "none":
        logger.info("chgrp: HPSS is unavailable")
    else:
        recurse_str = "-R " if recurse else ""
        command = "hsi chgrp {}{} {}".format(recurse_str, group, hpss)
        error_str = "Changing group of HPSS archive {} to {}".format(hpss, group)
        run_command(command, error_str)
