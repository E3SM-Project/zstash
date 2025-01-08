#!/bin/bash

i=1

while [[ $i -lt 12 ]]; do
    ./gen_data.sh 1000000 small_0${i}_1M
    i=$((i+1))
done
