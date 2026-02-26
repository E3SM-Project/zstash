# Agent Guidelines for zstash

This file provides guidance for AI coding agents working on the
[zstash](https://github.com/E3SM-Project/zstash) repository.

## Project overview

**zstash** is an HPSS long-term archiving tool for E3SM (Energy Exascale Earth
System Model). It bundles files into standard tar archives for efficient storage -- typically, but not always, on the NERSC HPSS tape system. Globus can be used to transfer data between machines. A SQLite index database (`index.db`) tracks every archived file with its path, size, mtime,
md5 checksum, tar name, and offset.

Key commands: `zstash create`, `zstash update`, `zstash check`, `zstash extract`, `zstash ls`, `zstash version`.

Documentation: <https://docs.e3sm.org/zstash/>

## Key Design Decisions

- Files are **never split** across tar archives.
- Tar files are created locally first, then transferred.
- The index database (`index.db`) is the single source of truth for what is
  archived and where.
- `zstash update` always creates new tar files; it never appends to existing
  ones (by design, to avoid corruption risk).
- Checksums (md5) are computed on-the-fly during both create and extract.

## Repository layout

```
zstash/          # Main Python package
  main.py        # CLI entry point
  create.py      # zstash create
  update.py      # zstash update
  check.py       # zstash check
  extract.py     # zstash extract
  ls.py          # zstash ls
  hpss.py / hpss_utils.py      # HPSS (hsi) transfer
  globus.py / globus_utils.py  # Globus transfer
  settings.py    # Global settings / config
  utils.py       # Shared utilities
  parallel.py    # Parallel worker support
tests/
  unit/          # Pure-function tests (pytest)
  integration/
    python_tests/
      group_by_command/   # unittest, one file per command
      group_by_workflow/  # unittest, end-to-end workflows
    bash_tests/           # Bash scripts; some require HPSS/Globus access
docs/source/     # Sphinx documentation (reStructuredText)
conda/dev.yml    # Development conda environment
setup.cfg        # pre-commit (e.g., flake8, isort, mypy) configuration
```

## Tech stack

- **Python** ≥ 3.11, < 3.14
- **SQLite** (`sqlite3` stdlib) for the index database
- **globus-sdk** ≥ 3.15, < 4.0 for Globus transfers
- **six** ≥ 1.16 for compatibility helpers
- Build / packaging: `setup.py` + `setup.cfg`

## Code style

- **Formatter**: `black` (v25.1.0) — run automatically via pre-commit.
- **Import order**: `isort` (v6.0.1) with `multi_line_output=3`.
- **Linter**: `flake8` (v7.3.0), max line length **119**, config in `setup.cfg`.
- **Type checker**: `mypy` (v1.18.2), `check_untyped_defs = True`.
- Comments should explain *why*, not *what*.
- Use Python type annotations in virtually all cases.
- Error handling: for `None` checks on values that must be non-`None`, use `TypeError` with descriptive messages.

All checks are enforced via `pre-commit` — **every commit must pass
`pre-commit run --all-files`**.

## Setting up a development environment

```bash
# First, activate conda.
# Then:
rm -rf build
conda clean --all --y
conda env create -f conda/dev.yml -n env-name
conda activate env-name
pre-commit run --all-files
python -m pip install .
```

Individual `pre-commit` hooks: `trailing-whitespace`, `end-of-file-fixer`,
`check-yaml`, `black`, `isort`, `flake8`, `mypy`. Run with `pre-commit run <hook_id>`

## Testing

The Python tests can be run as follows:
```bash
# Unit tests (fast, no HPSS/Globus required)
pytest tests/unit/test_*.py

# Integration tests (machine-independent)
python -m unittest tests/integration/python_tests/group_by_command/test_*.py
python -m unittest tests/integration/python_tests/group_by_workflow/test_*.py
```

Some integration tests are skipped automatically when `hsi`/HPSS is not
available (i.e., off of Perlmutter). Bash tests under `tests/integration/bash_tests/` may require
a specific machine and/or Globus credentials.

When to add tests:
- Adding new features or internal functions
- A bug is found

When to modify tests:
- Modifying features or internal functions.

When NOT to modify tests:
- Making non-functional code changes. For example, changes to support new versions of Python should not change behavior and thus tests should not be changed (even if implementation changes are required).

## Git Workflow

- `main` must always be deployable.
- All changes are made through **feature branches**.
- Rebase onto `main` (never merge) to resolve conflicts.
- For small changes, use "Squash and merge", to reduce the number of extraneous commits.
- For big changes, use "Create a merge commit", after squashing commits into distinct chunks of work.
- Open a PR early for discussion; merge only after CI passes and PR is approved.
- Never commit directly to `main`.
- Changes should be as minimal as possible to achieve the objective. Avoid over-engineering.

## Adding Dependencies

New dependencies go in `conda/dev.yml`. Note the change in PR description and discuss with the team before adding new packages.

They must be added to `conda/dev.yml` (not just as `import` statements), and tool version changes must be kept in sync between `conda/dev.yml` and `.pre-commit-config.yaml`.
