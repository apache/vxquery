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

NODES=2
REPEAT=1

# Start Hadoop
sh saved/hadoop/hadoop-1.2.1/bin/start-all.sh

# Prepare hadoop file system
hadoop fs -mkdir all
hadoop fs -mkdir all/sensors
hadoop fs -mkdir all/stations


# Upload test data
n=0
while [ ${n} -lt ${NODES} ];
do
    # Add each sensor block
    cp saved/backups/mr/all_sensors_${n}.xml.gz disk1/hadoop/upload/
    gunzip disk1/hadoop/upload/all_sensors_${n}.xml.gz
    hadoop fs -copyFromLocal disk1/hadoop/upload/all_sensors_${n}.xml all/sensors
    rm -f disk1/hadoop/upload/all_sensors_${n}.xml
    
    # Add each station block
    cp saved/backups/mr/all_stations_${n}.xml.gz disk1/hadoop/upload/
    gunzip disk1/hadoop/upload/all_stations_${n}.xml.gz
    hadoop fs -copyFromLocal disk1/hadoop/upload/all_stations_${n}.xml all/stations
    rm -f disk1/hadoop/upload/all_stations_${n}.xml
done


# Start test
sh vxquery-benchmark/src/main/resources/noaa-ghcn-daily/other_systems/mrql_scripts/run_mrql_tests.sh vxquery-benchmark/src/main/resources/noaa-ghcn-daily/other_systems/mrql/ ${NODES} ${REPEAT}


# Stop Hadoop
sh saved/hadoop/hadoop-1.2.1/bin/stop-all.sh