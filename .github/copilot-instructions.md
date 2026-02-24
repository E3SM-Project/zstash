# GitHub Copilot Instructions for zstash

## Project Overview

**zstash** is an HPSS long-term archiving tool for E3SM (Energy Exascale Earth
System Model). It bundles files into standard tar archives for efficient storage
on tape systems (NERSC HPSS, ALCF HPSS) or via Globus. An SQLite index
database (`index.db`) tracks every archived file with its path, size, mtime,
md5 checksum, tar name, and offset.

Key commands: `create`, `update`, `check`, `extract`, `ls`, `version`.

Documentation: <https://docs.e3sm.org/zstash/>

## Repository Layout

```
zstash/          # Main Python package
  main.py        # CLI entry point
  create.py      # zstash create
  update.py      # zstash update
  check.py       # zstash check
  extract.py     # zstash extract
  ls.py          # zstash ls
  hpss.py / hpss_utils.py      # HPSS (hsi) transport
  globus.py / globus_utils.py  # Globus transport
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
setup.cfg        # flake8, isort, mypy configuration
```

## Tech Stack

- **Python** ≥ 3.11, < 3.14
- **SQLite** (`sqlite3` stdlib) for the index database
- **globus-sdk** ≥ 3.15, < 4.0 for Globus transfers
- **six** ≥ 1.16 for compatibility helpers
- Build / packaging: `setup.py` + `setup.cfg`

## Code Style

- **Formatter**: `black` (v25.1.0) — run automatically via pre-commit.
- **Import order**: `isort` (v6.0.1) with `multi_line_output=3`.
- **Linter**: `flake8` (v7.3.0), max line length **119**, config in `setup.cfg`.
- **Type checker**: `mypy` (v1.18.2), `check_untyped_defs = True`.
- Comments should explain *why*, not *what* (the code shows how).
- Use Python type annotations where helpful.
- Error handling: use `TypeError` with descriptive messages for `None` checks
  on values that must be non-`None`.

All checks are enforced via `pre-commit` — **every commit must pass
`pre-commit run --all-files`**.

## Development Setup

```bash
conda env create -f conda/dev.yml
conda activate zstash_dev
pre-commit install
pip install .
```

When updating tool versions, keep `conda/dev.yml` and `.pre-commit-config.yaml`
in sync.

## Testing

```bash
# Unit tests (fast, no HPSS/Globus required)
pytest tests/unit/test_*.py

# Integration tests (machine-independent)
python -m unittest tests/integration/python_tests/group_by_command/test_*.py
python -m unittest tests/integration/python_tests/group_by_workflow/test_*.py
```

Some integration tests are skipped automatically when `hsi`/HPSS is not
available. Bash tests under `tests/integration/bash_tests/` may require
Perlmutter, Chrysalis, or Globus credentials.

## Git Workflow

- `main` must always be deployable.
- All changes are made through **fork-based feature branches**.
- Rebase onto `main` (never merge) to resolve conflicts.
- **Squash and rebase** before merging; each squashed commit must pass CI.
- Open a PR early for discussion; merge only after CI passes and PR is approved.
- Never commit directly to `main`.

## Adding Dependencies

New runtime dependencies go in `conda/dev.yml`. Note the change in PR
description and discuss with the team before adding new packages.
