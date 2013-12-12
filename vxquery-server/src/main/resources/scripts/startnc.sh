#!/bin/bash

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
