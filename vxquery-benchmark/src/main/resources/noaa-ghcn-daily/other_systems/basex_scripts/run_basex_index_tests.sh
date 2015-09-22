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

NODES=${2}
REPEAT=${3}
DATASET=${4}
EMAIL=${5}


mkdir -p ~/logs/basex_index/

time JAVA_OPTS="-Xmx8g" java -cp BaseX823.jar org.basex.BaseX

for j in $(find ${1} -name '*q??.xq')
do
    date
    echo "Running BaseX query: ${j}"
    time JAVA_OPTS="-Xmx8g" java -cp BaseX823.jar org.basex.BaseX -V -x -w -r${REPEAT} ${j} >> ~/logs/basex_index/$(basename "${j}").log 2>&1
done


if which programname >/dev/null;
then
    echo "Sending out e-mail notification."
    SUBJECT="BaseX Tests Finished (${DATASET})"
    /bin/mail -s "${SUBJECT}" "${EMAIL}" <<EOM
    Completed all BaseX tests on ${DATASET}.
    EOM
else
    echo "No mail command to use."
fi;
