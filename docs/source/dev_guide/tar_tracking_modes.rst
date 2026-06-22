##################
Tar Tracking Modes
##################

This page explains how zstash creates, tracks, transfers, and cleans up tar
files in the three supported ``--hpss`` modes:

* ``none``
* a direct HPSS path
* a Globus URL

Common lifecycle
================

Regardless of mode, ``zstash create`` and ``zstash update`` follow the same
high-level pattern:

1. Walk the source tree and decide which files belong in the next tar.
2. Create the tar locally in the cache directory.
3. Record file-level metadata in ``files`` and, when enabled, tar-level
   metadata in ``tars``.
4. Transfer the tar if the archive uses HPSS or Globus.
5. Transfer ``index.db`` after the tar work is complete.

The archive database is the source of truth for both file membership and tar
metadata.

Mode: ``--hpss=none``
=====================

In local-only mode, zstash still builds the archive exactly the same way, but
there is no remote transfer step.

* tar files are created under the local cache, usually ``zstash/``
* each tar is left in place after it is closed
* zstash removes write permission from completed tar files so they are less
  likely to be changed accidentally
* ``index.db`` remains local alongside the tar files

This is the simplest mode because the local cache is the archive.

Mode: direct HPSS path
======================

When ``--hpss`` points to an HPSS directory, zstash uses ``hsi`` for transfers.

* each tar is still created locally first
* after a tar is closed, zstash uploads it to the target HPSS directory
* if ``--keep`` is not set, the local tar can be deleted after a successful
  transfer
* ``index.db`` is uploaded after the tar work completes, but it is not tracked
  for deletion like tar files are

This mode is the most direct path when the current machine already has HPSS
access.

Mode: Globus URL
================

When ``--hpss`` is a ``globus://`` URL, zstash still creates each tar locally
first, but transfer tracking becomes more explicit.

* each completed tar is added to a transfer batch
* the batch is associated with Globus task state instead of an immediate
  ``hsi put``
* local tar files are kept until Globus reports success for the corresponding
  transfer task
* ``globus_finalize()`` submits any remaining pending transfer data, waits for
  completion, and then removes successfully transferred tar files when
  ``--keep`` is not set
* ``index.db`` is transferred after the tar workflow, just as in the direct
  HPSS mode

This mode is what allows zstash to archive to HPSS from machines that do not
have direct HPSS access.

Summary table
=============

.. list-table::
   :header-rows: 1

   * - ``--hpss`` value
     - Transfer mechanism
     - When local tar can be removed
     - Where the archive lives
   * - ``none``
     - no transfer
     - never removed automatically
     - local cache only
   * - HPSS path
     - ``hsi``
     - immediately after successful transfer, unless ``--keep``
     - local cache plus remote HPSS directory
   * - Globus URL
     - Globus transfer task
     - after Globus success and finalization, unless ``--keep``
     - local cache plus remote Globus-backed destination

Graphviz source
===============

The following ``.dot`` file summarizes the relationships between local tar
creation, database updates, and transfer handling in each mode:

.. literalinclude:: /_static/code_snippets/tar_tracking_modes.dot
   :language: dot
