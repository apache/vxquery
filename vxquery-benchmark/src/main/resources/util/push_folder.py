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
import sys
import getopt
import os

# Custom modules.
from cluster_information import *

def main(argv):
    cluster_file_name = ""
    push_folder_path = ""
    push_folder_name = ""
    
    try:
        opts, args = getopt.getopt(argv, "c:f:h", ["cluster=", "folder="])
    except getopt.GetoptError:
        print 'The file options for push_benchmark.py were not correctly specified.'
        print 'To see a full list of options try:'
        print '  $ python push_benchmark.py -h'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'Options for pushing a benchmark:'
            print '    -c        The XML cluster configuration file.'
            print '    -f        The folder to push out to the cluster.'
            sys.exit()
        elif opt in ('-c', "--cluster"):
            # check if file exists.
            if os.path.exists(arg):
                cluster_file_name = arg
            else:
                print 'Error: Argument must be a file name for --cluster (-c).'
                sys.exit()
        elif opt in ('-f', "--folder"):
            # check if file exists.
            if os.path.exists(arg):
                if os.path.basename(arg) == "":
                    push_folder = os.path.dirname(arg)
                else:
                    push_folder = arg
                push_folder_path = os.path.dirname(push_folder)
                push_folder_name = os.path.basename(push_folder)
                tar_file = push_folder_name + ".tar.gz"
            else:
                print 'Error: Argument must be a file name for --folder (-f).'
                sys.exit()

    # Required fields to run the script.
    if cluster_file_name == "" or not os.path.exists(cluster_file_name):
        print 'Error: The cluster XML file option must be supplied:  --cluster (-c).'
        sys.exit()
    if push_folder_path == "" or not os.path.exists(push_folder_path):
        print 'Error: The folder path option must be supplied:  --folder (-f).'
        sys.exit()

    
    # TODO Consider using python command instead of os.system.
    print "Creating a zip file to push out."
    tar_command = "tar -zcf " + tar_file + " -C " + push_folder_path + " " + push_folder_name
    os.system(tar_command)

    # Push the information out to each server.    
    ci = ClusterInformation(cluster_file_name)
    username = ci.get_username()
    push_file(tar_file, push_folder_name, username, ci.get_master_node_ip())
    for ip in ci.get_node_ip_list():
        push_file(tar_file, push_folder_name, username, ip)

    print "Creating a zip file to push out."
    remove_command = "rm " + tar_file
    os.system(remove_command)

    
# Copy over file and make sure it brand new.
def push_file(tar_file, existing_folder, username, ip_address):
    print "Updating server: " + ip_address

    print "  - Add new file."
    remove_tar_command = "ssh -x " + username + "@" + ip_address + " 'rm " + tar_file + "'"
    os.system(remove_tar_command)
    copy_command = "scp " + tar_file + " " + username + "@" + ip_address + ":"
    os.system(copy_command)
    
    print "  - Expand new file."
    remove_folder_command = "ssh -x " + username + "@" + ip_address + " 'rm -rf " + existing_folder + "'"
    os.system(remove_folder_command)
    unpack_command = "ssh " + username + "@" + ip_address + " 'tar -zxf " + tar_file + "'"
    os.system(unpack_command)
   
    print "  - Server clean up."
    os.system(remove_tar_command)
    pass
                
if __name__ == "__main__":
    main(sys.argv[1:])
