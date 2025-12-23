# Performance Profiling Results

## Test Configuration
- **Work Directory**: /lcrc/group/e3sm/ac.forsyth2/zstash_performance/profile_update_20251222
- **Source Directory**: /lcrc/group/e3sm/ac.forsyth2/E3SMv2/v2.LR.historical_0201/
- **Many Small Files**: build/
- **Few Large Files**: init/
- **Mix**: run/

## Performance Metrics

| Test Case | Create Dir | Update Dir | HPSS | File Gathering (Time/Memory) | Database Comparison (Time/Memory) | Add Files (Time/Memory) | Total Time |
|-----------|------------|------------|------|------------------------------|-----------------------------------|-------------------------|------------|
| test_create0_update1_hpss0 | build/ | init/ | none | 0.27s / 1.99MB | 8.90s / 0.00MB | 27.08s / 33.60MB | 0m37.463s |
| test_create0_update1_hpss1 | build/ | init/ | globus | 0.25s / 1.99MB | 10.60s / 0.00MB | 80.38s / 33.60MB | 1m45.768s |
| test_create0_update2_hpss0 | build/ | run/ | none | 0.33s / 2.01MB | 9.91s / 0.01MB | 44.07s / 33.68MB | 0m55.759s |
| test_create0_update2_hpss1 | build/ | run/ | globus | 0.29s / 2.01MB | 10.64s / 0.01MB | 188.62s / 33.68MB | 3m34.381s |
| test_create1_update0_hpss0 | init/ | build/ | none | 0.40s / 1.99MB | 6.00s / 0.06MB | 58.49s / 39.60MB | 1m8.566s |
| test_create1_update0_hpss1 | init/ | build/ | globus | 0.42s / 1.99MB | 6.38s / 0.06MB | 85.69s / 39.60MB | 1m48.248s |
| test_create1_update2_hpss0 | init/ | run/ | none | 0.00s / 0.04MB | 0.02s / 0.01MB | 42.69s / 33.68MB | 0m43.102s |
| test_create1_update2_hpss1 | init/ | run/ | globus | 0.00s / 0.04MB | 0.02s / 0.01MB | 176.05s / 33.68MB | 3m8.159s |
| test_create2_update0_hpss0 | run/ | build/ | none | 0.39s / 2.01MB | 6.35s / 0.06MB | 62.33s / 39.60MB | 1m13.135s |
| test_create2_update0_hpss1 | run/ | build/ | globus | 0.30s / 2.01MB | 5.34s / 0.06MB | 84.63s / 39.60MB | 1m45.531s |
| test_create2_update1_hpss0 | run/ | init/ | none | 0.00s / 0.04MB | 0.02s / 0.00MB | 27.87s / 33.60MB | 0m28.145s |
| test_create2_update1_hpss1 | run/ | init/ | globus | 0.00s / 0.04MB | 0.02s / 0.00MB | 131.36s / 33.60MB | 2m23.624s |
