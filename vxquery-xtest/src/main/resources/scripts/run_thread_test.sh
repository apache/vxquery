#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if [ -z "${1}" ]
then
    echo "Please supply a directory for XML files."
    exit
fi


BUFFER=$((8*1024))
# bash until loop
until [ ${BUFFER} -gt $((1024*1024*1024)) ]; do
    for n in 1 2 4
    do
        # Parse Only
        log_file="thread_parsed_${n}_${BUFFER}.csv"
    
        echo "Clearing file system cache. (hack)"
        sh ./vxquery-xtest/target/appassembler/bin/clear_file_cache.sh > /dev/null
    
        echo "Start background logging."
        dstat -cdmr --nocolor --output logs/${log_file} > /dev/null &
        PROC_ID=$!
    
        echo "Run experiment for ${n} threads."
        JAVA_OPTS=" -Xmx8G " sh ./vxquery-xtest/target/appassembler/bin/diskperformance ${1} ${n} 1 ${BUFFER}
    
        echo "Stop background logging. (Process ID: ${PROC_ID})"
        kill -9 ${PROC_ID}

    
        # Parse No Return
        log_file="thread_empty_${n}_${BUFFER}.csv"
    
        echo "Clearing file system cache. (hack)"
        sh ./vxquery-xtest/target/appassembler/bin/clear_file_cache.sh > /dev/null
    
        echo "Start background logging."
        dstat -cdmr --nocolor --output logs/${log_file} > /dev/null &
        PROC_ID=$!
    
        echo "Run experiment for ${n} threads."
        JAVA_OPTS=" -Xmx8G " sh ./vxquery-cli/target/appassembler/bin/vxq weather_data/dataset-gsn-local/queries/local_batch_scale_out/d1_p${n}/no_result.xq -buffer-size ${BUFFER} 2>&1
    
        echo "Stop background logging. (Process ID: ${PROC_ID})"
        kill -9 ${PROC_ID}

        # Full Query
        log_file="thread_full_${n}_${BUFFER}.csv"
    
        echo "Clearing file system cache. (hack)"
        sh ./vxquery-xtest/target/appassembler/bin/clear_file_cache.sh > /dev/null
    
        echo "Start background logging."
        dstat -cdmr --nocolor --output logs/${log_file} > /dev/null &
        PROC_ID=$!
    
        echo "Run experiment for ${n} threads."
        JAVA_OPTS=" -Xmx8G " sh ./vxquery-cli/target/appassembler/bin/vxq weather_data/dataset-gsn-local/queries/local_batch_scale_out/d1_p${n}/q00.xq -buffer-size ${BUFFER} 2>&1
    
        echo "Stop background logging. (Process ID: ${PROC_ID})"
        kill -9 ${PROC_ID}

    done;
    let BUFFER=BUFFER*4
done 
