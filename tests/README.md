# zstash Test Suite

## The test directory structure

```
tests/
  integration/ # Tests that call zstash from the command line
    bash_tests/ # Test zstash commands via bash scripts
      run_from_chrysalis/ # Run these from Chrysalis (these use Globus and/or require the Chrysalis file system)
      run_from_perlmutter/ # Run these from Perlmutter (these use `hsi` directly and/or require the Perlmutter file system)
    python_tests/ # Test zstash commands via Python unittest wrappers
      group_by_command # Tests organized by command
      group_by_workflow # Tests organized by workflow
  unit/ # Tests of pure functions (uses pytest, not unittest)
  utils/ # Utilities for testing
```

## Testing example for Perlmutter

```bash
rm -rf build
conda clean --all --y
conda env create -f conda/dev.yml -n zstash_dev_20251017_test1
conda activate zstash_dev_20251017_test1
pre-commit run --all-files
python -m pip install .
pytest tests/unit/test_*.py
# 1 passed in 0.19s
python -m unittest tests/integration/python_tests/group_by_command/test_*.py
# Ran 69 tests in 327.570s
# OK
python -m unittest tests/integration/python_tests/group_by_workflow/test_*.py
# Ran 4 tests in 2.666s
# OK

cd tests/integration/bash_tests/run_from_any/
# Review the directions at the bottom of globus_auth.bash
# You will need to modify the file and run several times.

cd tests/integration/bash_tests/run_from_perlmutter/
time ./follow_symlinks.sh # NOTE: you will have to change out paths for your username
# real	0m31.851s
# No errors
time ./test_update_non_empty_hpss.bash
# real	0m10.062s
# No errors

# Log into globus.org
# Log into endpoints (NERSC Perlmutter, Globus Tutorial Collection 1) at globus.org: File Manager > Add the endpoints in the "Collection" fields
time ./test_ls_globus.bash
# real	0m26.930s
# No errors
```

## Testing example for Chrysalis

```bash
rm -rf build
conda clean --all --y
conda env create -f conda/dev.yml -n zstash_dev_20251017_test1
conda activate zstash_dev_20251017_test1
pre-commit run --all-files
python -m pip install .
pytest tests/unit/test_*.py
# 1 passed in 0.84s
python -m unittest tests/integration/python_tests/group_by_command/test_*.py
# Ran 69 tests in 110.139s
# OK (skipped=32)
# NOTE: Some tests are skipped because Chrysalis doesn't have direct `hsi`/HPSS access
python -m unittest tests/integration/python_tests/group_by_workflow/test_*.py
# Ran 4 tests in 6.889s
# OK

cd tests/integration/bash_tests/run_from_any/
# Review the directions at the bottom of globus_auth.bash
# You will need to modify the file and run several times.

cd tests/integration/bash_tests/run_from_chrysalis/
# If not done above, do the following:
# Log into globus.org
# Log into endpoints (LCRC Improv DTN, NERSC Perlmutter) at globus.org: File Manager > Add the endpoints in the "Collection" fields

# In all cases, do:
# Then, increment `try_num` below to avoid using an old directory.
# Alternatively, start fresh by deleting the directory on Perlmutter:
# `rm -rf /global/homes/f/forsyth/zstash/tests/test_database_corruption_try{try_num}`
time ./database_corruption.bash try_num # NOTE: you will have to change out paths for your username
# Success count: 25
# Fail count: 0
# real	6m43.994s

time ./symlinks.sh # NOTE: you will have to change out paths for your username
# real	0m1.346s
# No errors

cd blocking_test_scripts
# Review README_TEST_BLOCKING
# This uses "12 piControl ocean monthly files, 49 GB",
# so processing may take a long time.
# TODO (later PR): Confirm this test works
```

## Testing with GitHub Actions

GitHub Actions runs the tests according to `.github/workflows/build_workflow.yml`:
```
      # Run machine-independent tests
      - name: Run Tests
        run: |
          pytest tests/unit/test_*.py
          python -m unittest tests/integration/python_tests/group_by_command/test_*.py
          python -m unittest tests/integration/python_tests/group_by_workflow/test_*.py
```
