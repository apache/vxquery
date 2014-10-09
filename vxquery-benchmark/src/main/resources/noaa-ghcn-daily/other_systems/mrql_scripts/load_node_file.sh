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
    echo "Please enter the node number."
    exit
fi

echo "Loading node ${1} data file in to cluster."

# Add each sensor block
cp saved/backups/mr/all_sensors_${1}.xml.gz disk1/hadoop/
gunzip disk1/hadoop/all_sensors_${1}.xml.gz
hadoop fs -copyFromLocal disk1/hadoop/all_sensors_${1}.xml all/sensors
rm -f disk1/hadoop/all_sensors_${1}.xml

# Add each station block
cp saved/backups/mr/all_stations_${1}.xml.gz disk1/hadoop/
gunzip disk1/hadoop/all_stations_${1}.xml.gz
hadoop fs -copyFromLocal disk1/hadoop/all_stations_${1}.xml all/stations
rm -f disk1/hadoop/all_stations_${1}.xml
