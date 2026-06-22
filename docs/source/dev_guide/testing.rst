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
the repository root::

   rm -rf build
   conda clean --all --y
   conda env create -f conda/dev.yml -n zstash_dev_test
   conda activate zstash_dev_test
   pre-commit run --all-files
   python -m pip install .
   pytest tests/unit/test_*.py
   python -m unittest tests/integration/python_tests/group_by_command/test_*.py
   python -m unittest tests/integration/python_tests/group_by_workflow/test_*.py

Some integration tests are skipped automatically on systems that do not have
``hsi`` or HPSS access.

Bash-based integration tests
============================

The bash tests are grouped by the machine or environment they require.

Run from any machine
--------------------

The ``run_from_any`` directory contains bash tests that can be exercised
without a specific facility, although Globus authentication may still be part
of the workflow. The README specifically calls out reviewing the instructions in
``globus_auth.bash`` before running the related scripts.

Run from Perlmutter
-------------------

The ``run_from_perlmutter`` directory contains tests that depend on direct HPSS
access and Perlmutter-specific paths.

Run from Chrysalis
------------------

The ``run_from_chrysalis`` directory contains tests that depend on Chrysalis,
Globus setup, and in some cases explicit cleanup of previous authentication
state before rerunning.

Choosing the right scope
========================

Use the smallest test scope that matches the change:

* unit tests for isolated logic changes
* Python integration tests for command behavior
* bash and machine-specific tests for HPSS- or Globus-specific workflows
* release testing for end-to-end release validation; see
  :doc:`release_testing`

GitHub Actions
==============

GitHub Actions runs the machine-independent test suite in
``.github/workflows/build_workflow.yml``:

* ``pytest tests/unit/test_*.py``
* ``python -m unittest tests/integration/python_tests/group_by_command/test_*.py``
* ``python -m unittest tests/integration/python_tests/group_by_workflow/test_*.py``

That workflow is the baseline CI safety net, while the machine-specific bash
tests remain primarily manual.
