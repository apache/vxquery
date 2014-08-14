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

DATASET="dataset-hcn-d2"
cluster_ip=${1}
base_weather_folder=${2}

for n in 7 6 5 3 4 2 1 0
do
    #for t in "batch_scale_out" "speed_up"
    for t in "batch_scale_out"
    #for t in "speed_up"
    do 
        for p in 2 1 0 
        do 
            for c in 4
            do 
                echo " ==== node ${n} test ${t} partition ${p} cores ${c} ===="
                sh vxquery-benchmark/src/main/resources/noaa-ghcn-daily/scripts/run_benchmark_cluster.sh ${base_weather_folder}/${DATASET}/queries/${t}/${n}nodes/d2_p${p}/ ${n} "-client-net-ip-address ${cluster_ip} -available-processors ${c}"
            done
        done
    done
done

if which programname >/dev/null;
then
    echo "Sending out e-mail notification."
    SUBJECT="Benchmark Group Tests Finished"
    EMAIL="ecarm002@ucr.edu"
    /bin/mail -s "${SUBJECT}" "${EMAIL}" <<EOM
    Completed all tests in the predefined group for ${DATASET}.
    EOM
else
    echo "No mail command to use."
fi;