#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Run all the queries and save a log. 
# First argument: Supply the folder which houses all the queries (recursive).
# Second argument: adds options to the VXQuery CLI.
#
# run_benchmark.sh ./noaa-ghcn-daily/benchmarks/local_speed_up/queries/
# run_benchmark.sh ./noaa-ghcn-daily/benchmarks/local_speed_up/queries/ "-client-net-ip-address 169.235.27.138"
# run_benchmark.sh ./noaa-ghcn-daily/benchmarks/local_speed_up/queries/ "" q03
#
REPEAT=5
IGNORE=2
FRAME_SIZE=$((8*1024))
BUFFER_SIZE=$((32*1024*1024))
JOIN_HASH_SIZE=-1

if [ -z "${1}" ]
then
    echo "Please supply a directory for query files to be found."
    exit
fi

export JAVA_OPTS="$JAVA_OPTS -server -Xmx8G -XX:+HeapDumpOnOutOfMemoryError -Djava.util.logging.config.file=./vxquery-benchmark/src/main/resources/noaa-ghcn-daily/scripts/benchmark_logging.properties"

for j in $(find ${1} -name '*q??.xq')
do
    if [ -z "${3}" ] || [[ "${j}" =~ "${3}" ]] 
    then
        date
        echo "Running query: ${j}"
        log_file="$(basename ${j}).$(date +%Y%m%d%H%M).log"
        log_base_path=$(dirname ${j/queries/query_logs})
        mkdir -p ${log_base_path}
        time sh ./vxquery-cli/target/appassembler/bin/vxq ${j} ${2} -timing -showquery -showoet -showrp -frame-size ${FRAME_SIZE} -buffer-size ${BUFFER_SIZE} -join-hash-size ${JOIN_HASH_SIZE} -repeatexec ${REPEAT} > ${log_base_path}/${log_file} 2>&1
        echo "\nBuffer Size: ${BUFFER_SIZE}" >> ${log_base_path}/${log_file}
        echo "\nFrame Size: ${FRAME_SIZE}" >> ${log_base_path}/${log_file}
        echo "\nJoin Hash Size: ${JOIN_HASH_SIZE}" >> ${log_base_path}/${log_file}
        fi;
done

if which programname >/dev/null;
then
    echo "Sending out e-mail notification."
    SUBJECT="Benchmark Tests Finished"
    EMAIL="ecarm002@ucr.edu"
    /bin/mail -s "${SUBJECT}" "${EMAIL}" <<EOM
    Completed all tests in folder ${1}.
    EOM
else
    echo "No mail command to use."
fi;
