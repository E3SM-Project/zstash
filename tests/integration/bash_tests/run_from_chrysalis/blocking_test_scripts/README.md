
This document outlines the procedures conducted to test the zstash bloclking
and non-blocking behavior.

Note: As it was intended to test blocking with regard to archive tar-creations
vs Globus transfers, it was convenient to have both source and destination be
the same Globus endpoint. Effectively, we are employing Globus merely to move
tar archive files from one directory to another on the same file system.

The core intent in implementing zstash blocking is to address a potential
"low-disk" condition, where tar-files created to archive source files could
add substantially to the disk load. To avoid disk exhaustion, when "blocking"
(`--non-blocking` is absent on the command line), tar-file creation will
pause to wait for the previous tarfile globus transfer to complete, so that
the local copy can be deleted before the next tar-file is created.

I. File System Setup
====================

As one may want, or need to re-conduct testing under varied conditions, the
test script `test_zstash_blocking.sh` will establish the following directory structure in the operator's current
working directory:

```
[CWD]/src_data/
# Contains files to be tar-archived.
# One can experiment with different sizes of files to trigger behaviors.

[CWD]/src_data/zstash/
# Default location of tarfiles produced.
# This directory is created automatically by zstash unless
# "--cache" indicates an alternate location.

[CWD]/dst_data/
# Destination for Globus transfer of archives.

[CWD]/tmp_cache/
# [Optional] alternative location for tar-file generation.
```

Note: It may be convenient to create a "hold" directory to store files of
various sizes that can be easily produced by running the supplied scripts.

```
gen_data.sh
gen_data_runner.sh
```

The files to be used for a given test must be moved or copied to the src_data
directory before a test is initiated.

Note: It never hurts to run the supplied script `reset_test.sh` before a test run. This will delete any archives in the src_data/zstash
cache and the receiving dst_data directories, and delete the src_data/zstash
directory itself if it exists. This ensures a clean restart for testing.
The raw data files placed into src_data are not affected.

II. Running the Test Script
===========================

The test script "test_zstash_blocking.sh" accepts two positional parameters:

```
test_zstash_blocking.sh (BLOCKING|NON_BLOCKING)
```

If `BLOCKING` is selected, zstash will run in default mode, waiting for
each tar file to complete transfer before generating another tar file.

If `NON_BLOCKING` is selected, the zstash flag `--non-blocking` is supplied
to the zstash command line, and tar files continue to be created in parallel
to running Globus transfers.

It is suggested that you run the test script with

```
./test_zstash_blocking.sh (BLOCKING|NON_BLOCKING) 2>&1 | tee your_logfile
```

so that your command prompt returns and you can monitor progress with `snapshot.sh`, which will provide a view of both the tarfile cache and the destination
directory for delivered tar files. It is also suggested that you name your
logfile to reflect the date, and whether BLOCKING or not was specified. Example:
```bash
./test_zstash_blocking.sh BLOCKING 2>&1 | tee test_zstash_blocking_20251020.log
```

FINAL NOTE: In the zstash code, the tar file `MINSIZE` parameter is taken
to be (int) multiples of 1 GB. During testing, this had been changed to
"multiple of 100K" for rapid testing.  It may be useful to expose this as
a command line parameter for debugging purposes.
