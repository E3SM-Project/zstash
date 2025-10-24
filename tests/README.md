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

## Testing examples

### Machine Independent

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
# NOTE: Some tests will be skipped on systems without hsi/HPSS access
python -m unittest tests/integration/python_tests/group_by_workflow/test_*.py
# Ran 4 tests in 2.666s
# OK

cd tests/integration/bash_tests/run_from_any/
# Review the directions at the bottom of globus_auth.bash
# Run `./globus_auth.bash` with the appropriate parameters.
```

### Perlmutter-specific

```bash
cd tests/integration/bash_tests/run_from_perlmutter/
time ./follow_symlinks.sh # NOTE: you will have to change out paths for your username
# real	0m31.851s
# No errors
time ./test_update_non_empty_hpss.bash
# real	0m10.062s
# No errors

# Log into globus.org
# Log into endpoints (NERSC Perlmutter, Globus Tutorial Collection 1) at globus.org: File Manager > Add the endpoints in the "Collection" fields
time ./test_ls_globus.bash # NOTE: You may be asked to paste an auth-code
# real	0m40.297s
# No errors
```

### Chrysalis-specific

```bash
cd tests/integration/bash_tests/run_from_chrysalis/
# If not done above, do the following:
# Log into globus.org
# Log into endpoints (LCRC Improv DTN, NERSC Perlmutter) at globus.org: File Manager > Add the endpoints in the "Collection" fields

# Reset completely:
# Revoke consents: https://auth.globus.org/v2/web/consents > Globus Endpoint Performance Monitoring > rescind all
# Run the following lines to set up the database_corruption test
# Alternative option: Rerun the `test_single_auth_code ${path_to_repo} NERSC_PERLMUTTER_ENDPOINT ${perlmutter_dst_dir}` line from `globus_auth.bash`
rm ~/.zstash_globus_tokens.json
mkdir zstash_demo; echo 'file0 stuff' > zstash_demo/file0.txt
# NERSC_PERLMUTTER_ENDPOINT=6bdc7956-fc0f-4ad2-989c-7aa5ee643a79
zstash create --hpss=globus://6bdc7956-fc0f-4ad2-989c-7aa5ee643a79//global/homes/f/forsyth/zstash/tests/test_database_corruption_setup23 zstash_demo
# You'll have to paste an auth code here, but NOT during the database_corruption test.
rm -rf zstash_demo/
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
