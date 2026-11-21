#!/bin/bash

# base_dir=`pwd`
# base_dir=`realpath $base_dir`
BASE_DIR="/home/ac.forsyth2/ez/zstash/tests/utils/test_blocking"

# Set up Globus Endpoint UUIDs ################################################

# Selectable Endpoint UUIDs
ACME1_GCSv5_UUID=6edb802e-2083-47f7-8f1c-20950841e46a
LCRC_IMPROV_DTN_UUID=15288284-7006-4041-ba1a-6b52501e49f1
NERSC_HPSS_UUID=9cd89cfd-6d04-11e5-ba46-22000b92c6ec

SRC_UUID=$LCRC_IMPROV_DTN_UUID
DST_UUID=$LCRC_IMPROV_DTN_UUID

# Test assertion functions ####################################################
check_log_has()
{
    local expected_grep="${1}"
    local log_file="${2}"
    grep -q "${expected_grep}" ${log_file}
    if [ $? != 0 ]; then
        echo "Expected grep '${expected_grep}' not found in ${log_file}. Test failed."
        exit 2
    fi
}

check_log_does_not_have()
{
    local not_expected_grep="${1}"
    local log_file="${2}"
    grep "${not_expected_grep}" ${log_file}
    if [ $? == 0 ]; then
        echo "Not-expected grep '${expected_grep}' was found in ${log_file}. Test failed."
        exit 2
    fi
}

# Helper functions ############################################################
make_test_dirs() {
    # 12 piControl ocean monthly files, 49 GB
    SRC_DATA=$BASE_DIR/src_data
    DST_DATA=$BASE_DIR/dst_data

    mkdir -p $SRC_DATA $DST_DATA

    echo "src_data: $SRC_DATA"
    echo "dst_data: $DST_DATA"
}

generate_test_data() {
    i=1
    len=1000000 # in bytes
    while [[ $i -lt 12 ]]; do
        out=$SRC_DATA/small_0${i}_1M
        head -c $len </dev/urandom >$out
        i=$((i+1))
    done
}

snapshot() {
    echo "dst_data:"
    ls -l $DST_DATA

    echo ""
    echo "src_data/zstash:"
    ls -l $SRC_DATA/zstash
}

remove_test_dirs() {
    echo "Attempting to remove $SRC_DATA/zstash/ and $DST_DATA/*"
    # SRC_DATA
    if [[ -z "$SRC_DATA" ]]; then
        echo "Error: SRC_DATA must be defined to delete its zstash subdirectory."
    else
        rm -rf "$SRC_DATA/zstash/"
    fi
    # DST_DATA
    if [[ -z "$DST_DATA" ]]; then
        echo "Error: DST_DATA must be defined to delete it."
    else
        rm -f "$DST_DATA/*"
    fi
}

# Run tests ###################################################################

# Make maxsize 1 GB. This will create a new tar after every 1 GB of data.
# (Since individual files are 4 GB, we will get 1 tarfile per datafile.)
MAXSIZE=1  # GB

remove_test_dirs # Start fresh

echo "TEST: NON_BLOCKING"
make_test_dirs
generate_test_data
case_name="zstash_create_non_blocking"
time zstash create -v --hpss=globus://$DST_UUID/$DST_DATA --maxsize ${MAXSIZE} --non-blocking $SRC_DATA 2>&1 | tee ${case_name}.log
check_log_does_not_have "A transfer with identical paths has not yet completed" ${case_name}.log
snapshot
remove_test_dirs

# echo "TEST: BLOCKING"
# make_test_dirs
# generate_test_data
# time zstash create -v --hpss=globus://$DST_UUID/$DST_DATA --maxsize ${MAXSIZE} $SRC_DATA
# snapshot
# remove_test_dirs

echo "Testing Completed"
echo "Go to https://app.globus.org/activity to confirm Globus transfers completed successfully."
# TODO:
# Currently getting dst_data index and dst_data 000000 still in progress afer test completes in ~5 seconds...
exit 0
