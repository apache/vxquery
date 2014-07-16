#!/bin/bash
BYTE_COUNT=2000000

# loop over the files.
for i in `seq 0 9`
do
    # Attempting to add files to the cache.
    echo "cache file: /tmp/zero${i}"
    for l in `seq 0 4`
    do
        if [ ! -f "/tmp/zero${i}" ]
        then
            echo "Creating..."
            dd if=/dev/zero of=/tmp/zero${i} count=${BYTE_COUNT} 2> /dev/null
        fi;
        dd if=/tmp/zero${i} of=/dev/null count=${BYTE_COUNT} 2> /dev/null
    done;
done;