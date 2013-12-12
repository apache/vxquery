#!/bin/bash

hostname

CCHOST=$1

#Export JAVA_HOME and JAVA_OPTS
export JAVA_HOME=$JAVA_HOME
export JAVA_OPTS=$CCJAVA_OPTS

VXQUERY_HOME=`pwd`
CCLOGS_DIR=${VXQUERY_HOME}/logs

#Remove the logs dir
rm -rf $CCLOGS_DIR
mkdir $CCLOGS_DIR


#Launch hyracks cc script without toplogy
${VXQUERY_HOME}/vxquery-server/target/appassembler/bin/vxquerycc -client-net-ip-address $CCHOST -cluster-net-ip-address $CCHOST &> $CCLOGS_DIR/cc.log &
