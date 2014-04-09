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

CCHOST=$1
IPADDR=$2
NODEID=$3


#Set JAVA_HOME
export JAVA_HOME=$JAVA_HOME

#Set JAVA_OPTS
export JAVA_OPTS=$NCJAVA_OPTS

VXQUERY_HOME=`pwd`
NCLOGS_DIR=${VXQUERY_HOME}/logs

#Remove the logs dir
rm -rf $NCLOGS_DIR
mkdir $NCLOGS_DIR


#Launch hyracks nc
${VXQUERY_HOME}/vxquery-server/target/appassembler/bin/vxquerync -cc-host $CCHOST -cluster-net-ip-address $IPADDR  -data-ip-address $IPADDR -result-ip-address $IPADDR  -node-id $NODEID &> $NCLOGS_DIR/nc.log &
