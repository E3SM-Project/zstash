# Agent Guidelines for zstash

This file provides guidance for AI coding agents (e.g., GitHub Copilot
Workspace, OpenHands, etc.) working on the
[zstash](https://github.com/E3SM-Project/zstash) repository.

---

## Project Summary

**zstash** is an HPSS long-term archiving tool for the
[E3SM](https://e3sm.org/) project. It bundles files into sequential, fixed-size
tar archives (`000000.tar`, `000001.tar`, …) and keeps an SQLite index database
(`index.db`) that records every file's path, size, mtime, md5 checksum, tar
name, and byte offset for fast retrieval.

Supported storage back-ends: NERSC HPSS (`hsi`), ALCF HPSS, local disk
(`--hpss=none`), and any Globus endpoint.

Full documentation: <https://docs.e3sm.org/zstash/>

---

## Repository Layout

```
zstash/                  # Main Python package
  main.py                # Argument parsing & CLI dispatch
  create.py              # `zstash create`
  update.py              # `zstash update`
  check.py               # `zstash check`
  extract.py             # `zstash extract`
  ls.py                  # `zstash ls`
  hpss.py                # HPSS transport (hsi subprocess)
  hpss_utils.py          # HPSS helper utilities
  globus.py              # Globus SDK transport
  globus_utils.py        # Globus helper utilities
  settings.py            # Global settings dataclass / defaults
  utils.py               # Shared utility functions
  parallel.py            # Parallel-worker helpers
tests/
  unit/                  # Pure-function tests — run with pytest
  integration/
    python_tests/
      group_by_command/  # unittest; one test module per zstash command
      group_by_workflow/ # unittest; end-to-end workflow tests
    bash_tests/
      run_from_any/      # Machine-independent bash tests
      run_from_chrysalis/# Require Chrysalis file system / Globus
      run_from_perlmutter/ # Require Perlmutter / hsi
  utils/                 # Shared test utilities
docs/source/             # Sphinx documentation (reStructuredText)
conda/dev.yml            # Conda development environment spec
setup.cfg                # flake8 / isort / mypy configuration
setup.py                 # Package build script
.pre-commit-config.yaml  # Pre-commit hook definitions
```

---

## Environment Setup

```bash
# 1. Create and activate the dev conda environment
conda env create -f conda/dev.yml
conda activate zstash_dev

# 2. Install pre-commit hooks
pre-commit install

# 3. Install zstash in editable/development mode
pip install .
```

To update the environment after changes to `conda/dev.yml`:

```bash
conda env update -f conda/dev.yml --prune
pip install .
```

---

## Running Checks Before Every Commit

**No commit should be made without passing pre-commit checks:**

```bash
pre-commit run --all-files
```

Individual hooks (available IDs: `trailing-whitespace`, `end-of-file-fixer`,
`check-yaml`, `black`, `isort`, `flake8`, `mypy`):

```bash
pre-commit run <hook_id>
```

---

## Running Tests

### Machine-independent (required for every PR)

```bash
# Unit tests (fast, no HPSS/Globus required)
pytest tests/unit/test_*.py

# Integration tests — command-level
python -m unittest tests/integration/python_tests/group_by_command/test_*.py

# Integration tests — workflow-level
python -m unittest tests/integration/python_tests/group_by_workflow/test_*.py
```

Tests that require `hsi`/HPSS are skipped automatically on machines without
HPSS access.

### Machine-specific bash tests

```bash
# Any machine (requires Globus authentication first)
cd tests/integration/bash_tests/run_from_any/
./globus_auth.bash <args>
./test_globus_tar_deletion.bash <args>

# Perlmutter only
cd tests/integration/bash_tests/run_from_perlmutter/
./follow_symlinks.sh
./test_update_non_empty_hpss.bash
./test_ls_globus.bash

# Chrysalis only
cd tests/integration/bash_tests/run_from_chrysalis/
./database_corruption.bash <unique_id>
./symlinks.sh
```

See `tests/README.md` for full details and expected output.

---

## Code Style

| Tool    | Version  | Purpose                        |
|---------|----------|--------------------------------|
| `black` | 25.1.0   | Code formatting                |
| `isort` | 6.0.1    | Import sorting                 |
| `flake8`| 7.3.0    | Linting (max line length: 119) |
| `mypy`  | 1.18.2   | Optional static type checking  |

Key rules:
- Follow **Black** style (enforced automatically).
- Max line length is **119** characters (configured in `setup.cfg`).
- Use **type annotations** where helpful; `mypy` runs with
  `check_untyped_defs = True`.
- Write comments that explain *why*, not *what* — the code shows how.
- Use `TypeError` with descriptive messages for `None` checks on values
  expected to be non-`None`.
- New dependencies must be added to `conda/dev.yml` (not just as `import`
  statements), and tool version changes must be kept in sync between
  `conda/dev.yml` and `.pre-commit-config.yaml`.

---

## Git / PR Workflow

1. **Never commit directly to `main`.**
2. Create a feature branch from `main` on your fork.
3. Rebase onto upstream `main` (never merge) to keep history clean.
4. Ensure `pre-commit run --all-files` passes before pushing.
5. Open a PR early for discussion; CI (GitHub Actions) must pass.
6. **Squash and rebase** commits before merging; each squashed commit must
   pass CI independently.
7. Use "Squash and merge" for small changes, "Create a merge commit" for
   large/multi-commit changes.

CI runs three jobs on every PR:
1. `pre-commit-hooks` — formatting, linting, type checking.
2. `build` — installs the package and runs the machine-independent test suite
   on Python 3.11, 3.12, and 3.13.
3. `build-docs` — builds the Sphinx documentation.

---

## Documentation

- Source lives in `docs/source/` (reStructuredText / Sphinx).
- Build locally: `cd docs && make html`
- Published at <https://docs.e3sm.org/zstash/> via the `gh-pages` branch.
- Include documentation updates in the same PR as code changes.

---

## Key Design Decisions

- Files are **never split** across tar archives.
- Tar files are created locally first, then transferred to HPSS/Globus.
- The index database (`index.db`) is the single source of truth for what is
  archived and where.
- `zstash update` always creates new tar files; it never appends to existing
  ones (by design, to avoid corruption risk).
- Checksums (md5) are computed on-the-fly during both create and extract.
