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

cluster_ip=${1}

for n in 2 1 0
do
    for t in "batch_scale_out" "speed_up"
    do 
        for p in 2 1
        do 
            for c in 4
            do 
                echo " ==== node ${n} test ${t} partition ${p} cores ${c} ===="
                sh noaa-ghcn-daily/scripts/run_benchmark_cluster.sh weather_data/dataset-small-d2/queries/${t}/${n}nodes/d2_p${p}/ ${n} "-client-net-ip-address ${cluster_ip} -available-processors ${c}"
            done
        done
    done
done
