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

if [ -z "${1}" ]
then
    echo "Please enter the data set as the first argument."
    exit
fi

if [ -z "${2}" ]
then
    echo "Please enter the node number as the second argument."
    exit
fi

DATASET=${1}
NODES=${2}
THREADS=$((4*${NODES}))
REPEAT=1
DATA_FILES=${NODES}

# Start Hadoop
# sh saved/hadoop/hadoop-1.2.1/bin/start-all.sh
sh saved/hadoop/hadoop-2.5.1/sbin/hadoop-daemon.sh start namenode
sh saved/hadoop/hadoop-2.5.1/sbin/hadoop-daemons.sh start datanode
sh saved/hadoop/hadoop-2.5.1/sbin/yarn-daemon.sh start resourcemanager
sh saved/hadoop/hadoop-2.5.1/sbin/yarn-daemons.sh start nodemanager
sh saved/hadoop/hadoop-2.5.1/sbin/mr-jobhistory-daemon.sh start historyserver

sleep 10

# Start Flink
sh saved/flink/flink-yarn-0.6.1-incubating/bin/yarn-session.sh -n ${THREADS} -tm 1024 &
FLINK_PID=$!

# Prepare hadoop file system
hadoop fs -mkdir ${DATASET}
hadoop fs -ls 
hadoop fs -mkdir ${DATASET}/sensors
hadoop fs -mkdir ${DATASET}/stations
hadoop fs -ls ${DATASET}

hadoop balancer


# Upload test data
COUNTER=0
while [ ${COUNTER} -lt ${DATA_FILES} ];
do
    sh vxquery-benchmark/src/main/resources/noaa-ghcn-daily/other_systems/mrql_scripts/load_node_file.sh ${DATASET} ${COUNTER}
    let COUNTER=COUNTER+1 
done


# Start test
sh vxquery-benchmark/src/main/resources/noaa-ghcn-daily/other_systems/mrql_scripts/run_mrql_tests.sh vxquery-benchmark/src/main/resources/noaa-ghcn-daily/other_systems/mrql/ ${THREADS} ${REPEAT} ${DATASET}

# Stop Flink
kill ${FLINK_PID}
jobs -p
kill $(jobs -p)


# Stop Hadoop
# sh saved/hadoop/hadoop-1.2.1/bin/stop-all.sh
sh saved/hadoop/hadoop-2.5.1/sbin/mr-jobhistory-daemon.sh stop historyserver
sh saved/hadoop/hadoop-2.5.1/sbin/yarn-daemons.sh stop nodemanager
sh saved/hadoop/hadoop-2.5.1/sbin/yarn-daemon.sh stop resourcemanager
sh saved/hadoop/hadoop-2.5.1/sbin/hadoop-daemons.sh stop datanode
sh saved/hadoop/hadoop-2.5.1/sbin/hadoop-daemon.sh stop namenode
