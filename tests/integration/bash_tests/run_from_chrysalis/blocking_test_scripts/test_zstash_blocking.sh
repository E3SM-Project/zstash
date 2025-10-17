#!/bin/bash

if [[ $# -lt 1 ]]; then
    echo "Usage:  text_zstash_blocking.sh (BLOCKING|NON_BLOCKING)"
    echo "  One of \"BLOCKING\" or \"NON_BLOCKING\" must be supplied as the"
    echo "  first parameter."
    exit 0
fi

NON_BLOCKING=1

if [[ $1 == "BLOCKING" ]]; then
    NON_BLOCKING=0
elif [[ $1 == "NON_BLOCKING" ]]; then
    NON_BLOCKING=1
else
    echo "ERROR: Must supply \"BLOCKING\" or \"NON_BLOCKING\" as 1st argument."
    exit 0
fi


base_dir=`pwd`
base_dir=`realpath $base_dir`


# See if we are running the zstash we THINK we are:
echo "CALLING zstash version"
zstash version
echo ""

# Selectable Endpoint UUIDs
ACME1_GCSv5_UUID=6edb802e-2083-47f7-8f1c-20950841e46a
LCRC_IMPROV_DTN_UUID=15288284-7006-4041-ba1a-6b52501e49f1
NERSC_HPSS_UUID=9cd89cfd-6d04-11e5-ba46-22000b92c6ec

# 12 piControl ocean monthly files, 49 GB
SRC_DATA=$base_dir/src_data
DST_DATA=$base_dir/dst_data

SRC_UUID=$LCRC_IMPROV_DTN_UUID
DST_UUID=$LCRC_IMPROV_DTN_UUID

# Optional
TMP_CACHE=$base_dir/tmp_cache

mkdir -p $SRC_DATA $DST_DATA $TMP_CACHE

# Make maxsize 1 GB. This will create a new tar after every 1 GB of data.
# (Since individual files are 4 GB, we will get 1 tarfile per datafile.)

if [[ $NON_BLOCKING -eq 1 ]]; then
    echo "TEST: NON_BLOCKING:"
    zstash create -v --hpss=globus://$DST_UUID/$DST_DATA --maxsize 1 --non-blocking $SRC_DATA
else
    echo "TEST: BLOCKING:"
    zstash create -v --hpss=globus://$DST_UUID/$DST_DATA --maxsize 1 $SRC_DATA
    # zstash create -v --hpss=globus://$DST_UUID --maxsize 1 --non-blocking --cache $TMP_CACHE $SRC_DATA
fi

echo "Testing Completed"

exit 0
