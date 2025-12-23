# Performance Profiling Results

## Test Configuration
- **Work Directory**: /lcrc/group/e3sm/ac.forsyth2/zstash_performance/profile_update_20251222_after_changes
- **Source Directory**: /lcrc/group/e3sm/ac.forsyth2/E3SMv2/v2.LR.historical_0201/
- **Many Small Files**: build/
- **Few Large Files**: init/
- **Mix**: run/

## Performance Metrics

| Test Case | Create Dir | Update Dir | HPSS | File Gathering (Time/Memory) | Database Comparison (Time/Memory) | Add Files (Time/Memory) | Total Time |
|-----------|------------|------------|------|------------------------------|-----------------------------------|-------------------------|------------|
| test_create0_update1_hpss0 | build/ | init/ | none | 6.27s / 2.58MB | 0.23s / 2.46MB | 28.34s / 33.60MB | 0m36.178s |
| test_create0_update1_hpss1 | build/ | init/ | globus | 4.27s / 2.58MB | 0.24s / 2.46MB | 102.67s / 33.60MB | 2m0.986s |
| test_create0_update2_hpss0 | build/ | run/ | none | 4.31s / 2.60MB | 0.24s / 2.46MB | 43.56s / 33.68MB | 0m49.498s |
| test_create0_update2_hpss1 | build/ | run/ | globus | 5.32s / 2.60MB | 0.25s / 2.46MB | 226.25s / 33.68MB | 4m5.803s |
| test_create1_update0_hpss0 | init/ | build/ | none | 3.48s / 2.58MB | 0.02s / 0.07MB | 63.42s / 39.71MB | 1m9.522s |
| test_create1_update0_hpss1 | init/ | build/ | globus | 3.46s / 2.58MB | 0.05s / 0.07MB | 86.04s / 39.71MB | 1m45.198s |
| test_create1_update2_hpss0 | init/ | run/ | none | 0.00s / 0.13MB | 0.00s / 0.01MB | 42.56s / 33.68MB | 0m43.014s |
| test_create1_update2_hpss1 | init/ | run/ | globus | 0.00s / 0.13MB | 0.00s / 0.01MB | 86.17s / 33.68MB | 1m38.072s |
| test_create2_update0_hpss0 | run/ | build/ | none | 3.36s / 2.60MB | 0.06s / 0.10MB | 62.90s / 39.71MB | 1m10.506s |
| test_create2_update0_hpss1 | run/ | build/ | globus | 3.80s / 2.60MB | 0.02s / 0.10MB | 88.34s / 39.71MB | 1m47.698s |
| test_create2_update1_hpss0 | run/ | init/ | none | 0.00s / 0.14MB | 0.00s / 0.04MB | 27.63s / 33.60MB | 0m27.947s |
| test_create2_update1_hpss1 | run/ | init/ | globus | 0.00s / 0.14MB | 0.00s / 0.04MB | 69.70s / 33.60MB | 1m22.806s |
