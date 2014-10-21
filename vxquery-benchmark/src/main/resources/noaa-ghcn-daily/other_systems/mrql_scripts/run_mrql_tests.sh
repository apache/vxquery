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

# Examples
# run_mrql_tests.sh mrql_all/ 1 2

NODES=${2}
REPEAT=${3}
DATASET=${4}


# Make log folder
mkdir -p ~/disk1/weather_data/mrql/query_logs/${NODES}nodes/

for j in $(find ${1} -name '*q??.mrql')
do
    date
    echo "Running MRQL query: ${j}"
    time for i in {1..${REPEAT}}; do ~/mrql/incubator-mrql/bin/mrql -dist -nodes ${NODES} ${j} ${DATASET}/sensors/ ${DATASET}/stations/ >> ~/disk1/weather_data/mrql/query_logs/${NODES}nodes/$(basename "${j}").log 2>&1; done; 
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
