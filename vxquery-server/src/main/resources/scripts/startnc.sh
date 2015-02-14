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

hostname

NODEID=$1
IPADDR=$2
CCHOST=$3
CCPORT=$4
J_OPTS=$5

#Import cluster properties
MYDIR="$(dirname -- $0)"
. ${MYDIR}/../conf/cluster.properties

# Set JAVA_HOME
export JAVA_HOME=$JAVA_HOME

# java opts added parameters
if [ ! -z "${J_OPTS}" ]
then
    JAVA_OPTS="${JAVA_OPTS} ${J_OPTS}"
    export JAVA_OPTS
fi

# java opts added parameters Server cluster.properties
if [ ! -z "${NCJAVA_OPTS}" ]
then
    JAVA_OPTS="${JAVA_OPTS} ${NCJAVA_OPTS}"
    export JAVA_OPTS
fi

VXQUERY_HOME=`pwd`

# logs dir
mkdir -p $NCLOGS_DIR

# Set up the options for the cc.
NC_OPTIONS=" -cc-host ${CCHOST} -cluster-net-ip-address ${IPADDR}  -data-ip-address ${IPADDR} -result-ip-address ${IPADDR}  -node-id ${NODEID} "
[ "${CCPORT}" ] {
	NC_OPTIONS=" ${NC_OPTIONS} -cc-port ${CCPORT} "
}

[ "${NCOPTS}" ] {
    NC_OPTIONS=" ${NC_OPTIONS} ${NCOPTS} "
}

echo "${JAVA_OPTS}" &> ${NCLOGS_DIR}/nc.log


# Launch hyracks nc
${VXQUERY_HOME}/vxquery-server/target/appassembler/bin/vxquerync ${NC_OPTIONS} &> ${NCLOGS_DIR}/nc_$(date +%Y%m%d%H%M).log &
