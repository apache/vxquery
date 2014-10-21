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
import time
import subprocess
import os

from cluster_information import *

class ClusterActions:
    def __init__(self, cluster_xml_file):
        self.ci = ClusterInformation(cluster_xml_file)

    def deploy_cluster(self, base_folder):
        pass

    def deploy(self, push_folder):
        push_folder_path = os.path.dirname(push_folder)
        push_folder_name = os.path.basename(push_folder)
        tar_file = push_folder_name + ".tar.gz"

        # Make sure zip does not already exist.
        remove_command = "rm " + tar_file
        subprocess.call(remove_command, shell=True)
        time.sleep(2)
        
        # TODO Consider using python command instead of os.system.
        print "Creating a zip file to push out."
        # Change to root directory so you do not copy the directory you are in.
        tar_command = "cd /tmp; tar -zcf " + tar_file + " -C " + push_folder_path + " " + push_folder_name
        subprocess.call(tar_command, shell=True)
        move_command = "mv /tmp/" + tar_file + " ."
        subprocess.call(move_command, shell=True)

        # Cluster Controller
        machine = self.ci.get_master_node_machine()
        self.deploy_machine(machine, tar_file)
        # Node Controllers
        for machine in self.ci.get_node_machine_list():
            self.deploy_machine(machine, tar_file)

        print "Remove the push zip file."
        remove_command = "rm " + tar_file
        subprocess.call(remove_command, shell=True)
        
    def start(self):
        machine = self.ci.get_master_node_machine()
        self.start_cc(machine)
        time.sleep(5)
        self.start_all_ncs()
    
    def stop_cluster(self):
        machine = self.ci.get_master_node_machine()
        self.stop_cc_and_all_ncs(machine)
    
    def stop(self):
        self.stop_all_ncs()
        time.sleep(2)
        machine = self.ci.get_master_node_machine()
        self.stop_cc(machine)

    def start_all_ncs(self):
        cc = self.ci.get_master_node_machine()
        for machine in self.ci.get_node_machine_list():
            self.start_nc(machine, cc)
    
    def stop_all_ncs(self):
        for machine in self.ci.get_node_machine_list():
            self.stop_nc(machine)

    # --------------------------------------------------------------------------
    # Machine Specific Functions
    # --------------------------------------------------------------------------
    def deploy_machine(self, machine, tar_file):
        print "Deploy Controller."
        print "  " + machine.get_id() + " " + machine.get_ip()
        
        # Push the information out to each server.    
        print "  - Add new file."
        remove_tar_command = "rm " + tar_file + ""
        self.run_remote_command(machine.get_username(), machine.get_id(), remove_tar_command)
        copy_command = "scp " + tar_file + " " + machine.get_username() + "@" + machine.get_id() + ":"
        subprocess.call(copy_command, shell=True)
        
        print "  - Expand new file."
        base_folder = tar_file.split('.')[0]
        remove_folder_command = "rm -rf " + base_folder + ""
        self.run_remote_command(machine.get_username(), machine.get_id(), remove_folder_command)
        unpack_command = "tar -zxf " + tar_file + ""
        self.run_remote_command(machine.get_username(), machine.get_id(), unpack_command)
        # Make the bin files executable.
        chmod_command = "chmod u+x " + base_folder + "/target/appassembler/bin/vxq*"
        self.run_remote_command(machine.get_username(), machine.get_id(), chmod_command)
        chmod_command = "chmod u+x " + base_folder + "/target/appassembler/bin/*.sh"
        self.run_remote_command(machine.get_username(), machine.get_id(), chmod_command)
        
        print "  - Server clean up."
        self.run_remote_command(machine.get_username(), machine.get_id(), remove_tar_command)
        
    
    def start_cc(self, machine):
        print "Start Cluster Controller."
        print "  " + machine.get_id() + " " + machine.get_client_ip() + ":" + machine.get_client_port()
        command = "./vxquery-server/target/appassembler/bin/startcc.sh " + machine.get_client_ip() + " \"" + machine.get_client_port() + "\" \"" + machine.get_java_opts() + "\""
        self.run_remote_command(machine.get_username(), machine.get_id(), command)
    
    def start_nc(self, machine, cc):
        print "Start Node Controller."
        print "  " + machine.get_id() + " " + machine.get_ip()
        command = "./vxquery-server/target/appassembler/bin/startnc.sh " + machine.get_id() + " " + machine.get_ip() + " " + cc.get_client_ip() + " \"" + cc.get_client_port() + "\" \"" + machine.get_java_opts() + "\""
        self.run_remote_command(machine.get_username(), machine.get_id(), command)

    def stop_cc_and_all_ncs(self, machine):
        print "Stop Cluster and Node Controllers."
        print "  " + machine.get_id() + " " + machine.get_client_ip() + ":" + machine.get_client_port()
        command = "./vxquery-server/target/appassembler/bin/stopcluster.sh " + machine.get_client_ip() + " \"" + machine.get_client_port() + "\" \"" + machine.get_java_opts() + "\""
        self.run_remote_command(machine.get_username(), machine.get_id(), command)
    
    def stop_cc(self, machine):
        print "Stop Cluster Controller."
        print "  " + machine.get_id() + " " + machine.get_ip()
        command = "./vxquery-server/target/appassembler/bin/stopcc.sh " + machine.get_username()
        self.run_remote_command(machine.get_username(), machine.get_id(), command)
    
    def stop_nc(self, machine):
        print "Stop Node Controller."
        print "  " + machine.get_id() + " " + machine.get_ip()
        command = "./vxquery-server/target/appassembler/bin/stopnc.sh " + machine.get_username()
        self.run_remote_command(machine.get_username(), machine.get_id(), command)
        
    def run_remote_command(self, username, host, command):
        remote_command = "ssh -x " + username + "@" + host + " '" + command + "' "
#         print remote_command
        os.system(remote_command)

