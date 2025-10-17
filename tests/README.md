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

Example:
```bash
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
cd tests/integration/bash_tests/run_from_perlmutter/
time ./follow_symlinks.sh
# real	0m31.851s
# No errors
time ./test_update_non_empty_hpss.bash
# real	0m10.062s
# No errors
time ./test_ls_globus.bash
# real	0m26.930s
# No errors
```

## Testing example for Chrysalis
