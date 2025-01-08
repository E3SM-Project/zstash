#!/bin/bash

if [[ $# -lt 2 ]]; then
    echo "Usage: gen_data.sh <bytes> <outputfile>"
    exit 0
fi

len=$1
out=$2

head -c $len </dev/urandom >$out
