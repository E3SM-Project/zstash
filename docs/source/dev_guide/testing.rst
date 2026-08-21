#######
Testing
#######

This page summarizes the test process described in ``tests/README.md``.

Test layout
===========

The repository separates machine-independent tests from machine-specific
integration tests:

* ``tests/unit/`` contains pytest-based tests for pure functions
* ``tests/integration/python_tests/group_by_command/`` contains unittest-based
  command-oriented integration tests
* ``tests/integration/python_tests/group_by_workflow/`` contains unittest-based
  end-to-end workflow tests
* ``tests/integration/bash_tests/run_from_any/`` contains bash-driven tests
  that can be run from any machine
* ``tests/integration/bash_tests/run_from_perlmutter/`` contains tests that
  need Perlmutter or direct HPSS access
* ``tests/integration/bash_tests/run_from_chrysalis/`` contains tests that need
  Chrysalis and Globus-related setup
* ``tests/utils/`` contains shared test helpers

Recommended baseline workflow
=============================

For a normal development change, start with the machine-independent checks from
the repository root

  .. code-block:: bash

    rm -rf build
    conda clean --all --y
    conda env create -f conda/dev.yml -n zstash_dev_test
    conda activate zstash_dev_test
    pre-commit run --all-files
    python -m pip install .
    pytest tests/unit/test_*.py
    python -m unittest tests/integration/python_tests/group_by_command/test_*.py
    python -m unittest tests/integration/python_tests/group_by_workflow/test_*.py

Example of expected output when all tests pass

  .. code-block:: bash

    # pytest tests/unit/test_*.py
    # 1 passed in 0.19s

    # python -m unittest tests/integration/python_tests/group_by_command/test_*.py
    # Ran 69 tests in 327.570s
    # OK

    # python -m unittest tests/integration/python_tests/group_by_workflow/test_*.py
    # Ran 4 tests in 2.666s
    # OK

Some integration tests are skipped automatically on systems that do not have
``hsi`` or HPSS access.

Unit tests
==========

The ``tests/unit/`` directory holds pytest-based tests targeting isolated,
pure-Python logic — no HPSS, Globus, or filesystem side-effects required.

Current unit test files
-----------------------

``test_hpss.py``
   Exercises the ``get_files_to_archive_with_stats`` helper and the logic that
   compares on-disk files against the zstash SQLite database. Key scenarios
   covered:

   * Scanning a directory and returning size/mtime pairs for each file.
   * Building the ``archived_files`` dictionary from database rows, keeping the
     entry with the most recent modification time when duplicates exist.
   * Detecting **new** files (present on disk but absent from the database).
   * Detecting **modified** files whose size or modification time differs from
     the archived record, using a configurable ``TIME_TOL`` tolerance (default
     1 second).
   * Verifying that files within the tolerance window are *not* re-archived.

``test_utils.py``
   Covers ``zstash.utils.run_command`` subprocess behavior:

   * When the command starts with ``hsi``, the function strips
     ``LD_LIBRARY_PATH`` and ``LD_PRELOAD`` from the child environment to
     avoid linker conflicts, while preserving other variables such as ``HOME``.
   * For all other commands the loader variables are passed through unchanged.

Run the unit suite from the repository root

  .. code-block:: bash

    pytest tests/unit/test_*.py

Python integration tests
========================

The ``tests/integration/python_tests/`` directory contains ``unittest``-based
tests that invoke zstash commands and inspect their output.  They are split
into two sub-directories:

``group_by_command/``
   Each test file focuses on a single zstash sub-command (e.g. ``create``,
   ``update``, ``extract``, ``check``).  This makes it straightforward to run
   only the tests relevant to the command you have changed.

``group_by_workflow/``
   End-to-end tests that exercise multi-step workflows: create an archive,
   update it with new or changed files, extract files, and verify integrity.

Run both groups

  .. code-block:: bash

    python -m unittest tests/integration/python_tests/group_by_command/test_*.py
    python -m unittest tests/integration/python_tests/group_by_workflow/test_*.py

Tests that require ``hsi`` or HPSS are automatically skipped when those tools
are unavailable.

Bash-based integration tests
============================

The bash tests are grouped by the machine or environment they require.

Run from any machine
--------------------

The ``run_from_any/`` directory contains bash tests that can be exercised
on any machine, although Globus authentication is part of the
workflow.

Before running these tests, review the instructions at the bottom of
``globus_auth.bash``, then authenticate and run the Globus tar-deletion test

  .. code-block:: bash

    cd tests/integration/bash_tests/run_from_any/
    # Review globus_auth.bash and run with the appropriate parameters:
    ./globus_auth.bash <parameters>
    ./test_globus_tar_deletion.bash <parameters>

Run from Perlmutter
-------------------

The ``run_from_perlmutter/`` directory contains tests that depend on direct
HPSS access and Perlmutter-specific paths. Update any hardcoded paths to
match your username before running.

Steps

  .. code-block:: bash

    cd tests/integration/bash_tests/run_from_perlmutter/

    # Symlink-following test (edit paths for your username first)
    time ./follow_symlinks.sh
    # real 0m31.851s — No errors

    # HPSS update test
    time ./test_update_non_empty_hpss.bash
    # real 0m10.062s — No errors

    # Globus ls test
    # 1. Log into globus.org
    # 2. In File Manager, add both endpoints:
    #    - NERSC Perlmutter
    #    - Globus Tutorial Collection 1
    time ./test_ls_globus.bash   # You may be prompted to paste an auth-code
    # real 0m40.297s — No errors

Run from Chrysalis
------------------

The ``run_from_chrysalis/`` directory contains tests that depend on Chrysalis,
Globus setup, and in some cases explicit cleanup of previous authentication
state before rerunning.

Steps

  .. code-block:: bash

    cd tests/integration/bash_tests/run_from_chrysalis/

    # If not already done:
    # 1. Log into globus.org
    # 2. In File Manager add both endpoints:
    #    - LCRC Improv DTN
    #    - NERSC Perlmutter

    # --- database_corruption.bash ---
    # To reset completely before this test:
    #   Revoke consents: https://auth.globus.org/v2/web/consents
    #   > Globus Endpoint Performance Monitoring > rescind all
    #
    # Set up the required remote state (one-time):
    rm ~/.zstash_globus_tokens.json
    mkdir zstash_demo
    echo 'file0 stuff' > zstash_demo/file0.txt
    # NERSC_PERLMUTTER_ENDPOINT=6bdc7956-fc0f-4ad2-989c-7aa5ee643a79
    zstash create \
      --hpss=globus://6bdc7956-fc0f-4ad2-989c-7aa5ee643a79//global/homes/<user>/zstash/tests/test_database_corruption_setup23 \
      zstash_demo
    # Paste the auth-code when prompted.  This pre-authentication means the
    # database_corruption test itself will not stop for user input.
    rm -rf zstash_demo/
    #
    # Pick a unique_id to avoid collisions with a previous run, or delete the
    # remote directory on Perlmutter first:
    #   rm -rf /global/homes/<user>/zstash/tests/test_database_corruption_<unique_id>
    #
    # Edit paths for your username, then run:
    time ./database_corruption.bash <unique_id>
    # Success count: 25
    # Fail count: 0
    # real 6m43.994s

    # --- symlinks.sh ---
    # Edit paths for your username first.
    time ./symlinks.sh
    # real 0m1.346s — No errors

GitHub Actions
==============

GitHub Actions runs the machine-independent test suite in
``.github/workflows/build_workflow.yml``:

* ``pytest tests/unit/test_*.py``
* ``python -m unittest tests/integration/python_tests/group_by_command/test_*.py``
* ``python -m unittest tests/integration/python_tests/group_by_workflow/test_*.py``

That workflow is the baseline CI safety net, while the machine-specific bash
tests remain primarily manual.

Testing for a release
=====================

First, run on Chrysalis:

Steps

  .. code-block:: bash

    cd zstash
    pytest tests/unit/test_*.py
    python -m unittest tests/integration/python_tests/group_by_command/test_*.py
    python -m unittest tests/integration/python_tests/group_by_workflow/test_*.py

    cd tests/integration/bash_tests/run_from_any/
    ./globus_auth.bash unique_id chrysalis path_to_repo chrysalis_dst_basedir perlmutter_dst_basedir hpss_dst_basedir compy_dst_basedir
    ./test_globus_tar_deletion.bash unique_id path_to_repo dst_basedir LCRC_IMPROV_DTN_ENDPOINT

    cd -
    cd tests/integration/bash_tests/run_from_chrysalis/
    # You should still have Globus set up from the globus auth test.
    time ./database_corruption.bash unique_id # NOTE: you will have to change out paths for your username
    time ./symlinks.sh # NOTE: you will have to change out paths for your username

Then, run on Perlmutter:

Steps

  .. code-block:: bash

    cd zstash
    pytest tests/unit/test_*.py
    python -m unittest tests/integration/python_tests/group_by_command/test_*.py
    python -m unittest tests/integration/python_tests/group_by_workflow/test_*.py

    cd -
    cd tests/integration/bash_tests/run_from_perlmutter/
    time ./follow_symlinks.sh # NOTE: you will have to change out paths for your username
    time ./test_update_non_empty_hpss.bash
    # Log into globus.org
    # Log into endpoints (NERSC Perlmutter, Globus Tutorial Collection 1) at globus.org: File Manager > Add the endpoints in the "Collection" fields
    time ./test_ls_globus.bash # NOTE: You may be asked to paste an auth-code

Lastly, run on Compy:

Steps

  .. code-block:: bash

    cd zstash
    pytest tests/unit/test_*.py
    python -m unittest tests/integration/python_tests/group_by_command/test_*.py
    python -m unittest tests/integration/python_tests/group_by_workflow/test_*.py
