#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import sys, getopt, os

# Custom modules.
from cluster_actions import *

def main(argv):
    action = ""
    cluster_file_name = ""
    deploy_path = ""
    
    try:
        opts, args = getopt.getopt(argv, "a:c:d:h", ["action=", "deploy_path="])
    except getopt.GetoptError:
        print 'The file options for cluster_cli.py were not correctly specified.'
        print 'To see a full list of options try:'
        print '  $ python cluster_cli.py -h'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'Options for pushing a benchmark:'
            print '    -a (str)  The action to perform (deploy, start, or stop).'
            print '    -c        The XML cluster configuration file.'
            sys.exit()
        elif opt in ('-a', "--action"):
            # check if file exists.
            if arg in ('deploy', 'start', 'stop', 'kill'):
                action = arg
            else:
                print 'Error: Argument must be a string ("deploy", "start", "stop", or "kill") for --action (-a).'
                sys.exit()
        elif opt in ('-c', "--cluster"):
            # check if file exists.
            if os.path.exists(arg):
                cluster_file_name = arg
            else:
                print 'Error: Argument must be a file name for --cluster (-c).'
                sys.exit()
        elif opt in ('-d', "--deploy_folder"):
            # check if file exists.
            if os.path.exists(arg):
                if os.path.basename(arg) == "":
                    deploy_path = os.path.dirname(arg)
                else:
                    deploy_path = arg
            else:
                print 'Error: Argument must be a file name for --deploy_folder (-d).'
                sys.exit()

    # Required fields to run the script.
    if cluster_file_name == "" or not os.path.exists(cluster_file_name):
        print 'Error: The cluster XML file option must be supplied:  --cluster (-c).'
        sys.exit()

    # The action to take on the cluster.
    cluster = ClusterActions(cluster_file_name)    
    if action == 'start':
        cluster.start()
    elif action == 'stop':
        cluster.stop_cluster()
    elif action == 'kill':
        cluster.stop()
    elif action == 'deploy':
        if deploy_path != "":
            cluster.deploy(deploy_path)
        else:
            print 'Error: The cluster cli must have a deploy_folder option when doing the deploy action: --deploy_folder (-d).'
            sys.exit()
    else:
        print 'Error: The cluster cli must have an action option must be supplied: --action (-a).'
        sys.exit()
                
if __name__ == "__main__":
    main(sys.argv[1:])
