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

echo "Loading ${NODES} node ${DATASET} data file in to cluster."

# Add each sensor block
cp saved/backups/mr/${DATASET}_sensors_${NODES}.xml.gz disk1/hadoop/
gunzip disk1/hadoop/${DATASET}_sensors_${NODES}.xml.gz
hadoop fs -copyFromLocal disk1/hadoop/${DATASET}_sensors_${NODES}.xml ${DATASET}/sensors
hadoop fs -cp ${DATASET}/sensors/${DATASET}_sensors_${NODES}.xml ${DATASET}2/sensors/${DATASET}_sensors_${NODES}.xml 
rm -f disk1/hadoop/${DATASET}_sensors_${NODES}.xml

# Add each station block
cp saved/backups/mr/${DATASET}_stations_${NODES}.xml.gz disk1/hadoop/
gunzip disk1/hadoop/${DATASET}_stations_${NODES}.xml.gz
hadoop fs -copyFromLocal disk1/hadoop/${DATASET}_stations_${NODES}.xml ${DATASET}/stations
hadoop fs -cp ${DATASET}/stations/${DATASET}_stations_${NODES}.xml ${DATASET}2/stations/${DATASET}_stations_${NODES}.xml 
rm -f disk1/hadoop/${DATASET}_stations_${NODES}.xml
