# First, we have to set up Globus according to https://github.com/E3SM-Project/zstash/discussions/329
# Log in to Globus
# Authenticate LCRC Improv DTN
# Authenticate NERSC Perlmutter
source /lcrc/soft/climate/e3sm-unified/load_latest_e3sm_unified_chrysalis.sh
cd /home/ac.forsyth2/ez
mkdir zstash_dirs
cd zstash_dirs/
mkdir zstash_demo; echo 'file0 stuff' > zstash_demo/file0.txt
zstash create --hpss=globus://15288284-7006-4041-ba1a-6b52501e49f1/~/manual_run zstash_demo
# globus_sdk.services.transfer.errors.TransferAPIError: ('POST', 'https://transfer.api.globus.org/v0.10/endpoint/15288284-7006-4041-ba1a-6b52501e49f1/autoactivate?if_expires_in=600', None, 400, 'ClientError.AuthenticationFailed', 'No credentials supplied', 'msYY54WXq')
rm ~/.globus-native-apps.cfg
zstash create --hpss=globus://15288284-7006-4041-ba1a-6b52501e49f1/~/manual_run zstash_demo
# Auth Code prompt appears twice


cd /lcrc/group/e3sm/ac.forsyth2/E3SMv2_test/zstash_extractions
du -sh v2.NARRM.historical_0151/
# That's 22 GB. Let's try to compress it with zstash.
cd v2.NARRM.historical_0151/tests

# From https://docs.e3sm.org/zstash/_build/html/main/usage.html:
# `--maxsize MAXSIZE`` specifies the maximum size (in GB) for tar files. The default is 256 GB. Zstash will create tar files that are smaller than MAXSIZE except when individual input files exceed MAXSIZE (as individual files are never split up between different tar files).
# `--non-blocking` Zstash will submit a Globus transfer and immediately create a subsequent tarball. That is, Zstash will not wait until the transfer completes to start creating a subsequent tarball. On machines where it takes more time to create a tarball than transfer it, each Globus transfer will have one file. On machines where it takes less time to create a tarball than transfer it, the first transfer will have one file, but the number of tarballs in subsequent transfers will grow finding dynamically the most optimal number of tarballs per transfer. NOTE: zstash is currently always non-blocking.

# Make maxsize 1 GB. This will create a new tar after every 1 GB of data.
zstash create -v --hpss=globus://nersc/home/f/forsyth/test_290_v1 --maxsize 1 .

# DEBUG: Closing tar archive 000000.tar
# INFO: Creating new tar archive 000001.tar

# In a different window:
ls /lcrc/group/e3sm/ac.forsyth2/E3SMv2_test/zstash_extractions/v2.NARRM.historical_0151/tests/zstash
# 000000.tar  000001.tar  000002.tar  000003.tar  000004.tar  000005.tar  index.db

# So, we can clearly see the tars are being created immediately.
# On the Globus website, test_290_v1 000000 transfer is complete.
# And test_290_v1 000001 transfer is in progress.

# This is the `--non-blocking` behavior, even though we did not specify it.

# Now, with changes in this PR:
conda activate zstash_dev_issue_290
cd /lcrc/group/e3sm/ac.forsyth2/E3SMv2_test/zstash_extractions/v2.NARRM.historical_0151/tests
rm -rf zstash
zstash create -v --hpss=globus://nersc/home/f/forsyth/test_290_v2 --maxsize 1 --non-blocking .
# DEBUG: Closing tar archive 000000.tar
# INFO: Creating new tar archive 000001.tar
# In a different window:
ls /lcrc/group/e3sm/ac.forsyth2/E3SMv2_test/zstash_extractions/v2.NARRM.historical_0151/tests/zstash
# 000000.tar  000001.tar  index.db
# # On the Globus website, test_290_v1 000000 transfer is complete.
# And test_290_v1 000001 transfer is in progress.
ls /lcrc/group/e3sm/ac.forsyth2/E3SMv2_test/zstash_extractions/v2.NARRM.historical_0151/tests/zstash
# 000000.tar  000001.tar  000002.tar  000003.tar  000004.tar  000005.tar  index.db
ls /lcrc/group/e3sm/ac.forsyth2/E3SMv2_test/zstash_extractions/v2.NARRM.historical_0151/tests/zstash
000000.tar  000002.tar  000004.tar  000006.tar  000008.tar  00000a.tar
000001.tar  000003.tar  000005.tar  000007.tar  000009.tar  index.db
# Command completed on the command line
# But on Globus website:
# Completed -- test_290_v2 000000, test_290_v2 000001, test_290_v2 index
