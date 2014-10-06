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

export JAVA_HOME=/home/ecarm002/java/jdk1.6.0_45
REPEAT=${1}
DATASET="hcn"

for n in `seq 0 7`
#for n in 0
do
    date
    echo "Running q0${n} on ${DATASET} for MRQL."
    time for i in {1..${REPEAT}}; do ~/mrql/incubator-mrql/bin/mrql -dist -nodes 5 ~/vxquery-benchmark/src/main/resources/noaa-ghcn-daily/other_systems/mrql_${DATASET}/q0${n}.mrql >> weather_data/mrql/query_logs/${DATASET}/q0${n}.mrql.log 2>&1; done; 
done

if which programname >/dev/null;
then
    echo "Sending out e-mail notification."
    SUBJECT="MRQL Tests Finished (${DATASET})"
    EMAIL="ecarm002@ucr.edu"
    /bin/mail -s "${SUBJECT}" "${EMAIL}" <<EOM
    Completed all MRQL tests on ${DATASET}.
    EOM
else
    echo "No mail command to use."
fi;