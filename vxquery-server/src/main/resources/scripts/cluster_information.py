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
from xml.dom.minidom import parse

class ClusterInformation:
    def __init__(self, cluster_xml_file):
        self.cluster_xml_file = cluster_xml_file
        
        self.config = parse(self.cluster_xml_file)

    def get_username(self):
        return self.get_text(self.config.getElementsByTagName("username")[0])

    # Depricated
    def get_master_node_ip(self):
        master_node = self.config.getElementsByTagName("master_node")[0]
        return self.get_cluster_ip(master_node)

    # Depricated
    def get_node_ip_list(self):
        nodes = []
        for node in self.config.getElementsByTagName("node"):
            nodes.append(self.get_cluster_ip(node))
        return nodes

    def get_master_node_machine(self):
        master_node = self.config.getElementsByTagName("master_node")[0]
        id = self.get_cluster_id(master_node)
        ip = self.get_cluster_ip(master_node)
        username = self.get_username()
        return Machine(id, ip, username)

    def get_node_machine_list(self):
        nodes = []
        username = self.get_username()
        for node in self.config.getElementsByTagName("node"):
            id = self.get_cluster_id(node)
            ip = self.get_cluster_ip(node)
            nodes.append(Machine(id, ip, username))
        return nodes

    # --------------------------------------------------------------------------
    # Node Specific Functions
    # --------------------------------------------------------------------------
    def get_cluster_ip(self, node):
        return self.get_text(node.getElementsByTagName("cluster_ip")[0])

    def get_cluster_id(self, node):
        return self.get_text(node.getElementsByTagName("id")[0])

    def get_text(self, xml_node):
        rc = []
        for node in xml_node.childNodes:
            if node.nodeType == node.TEXT_NODE:
                rc.append(node.data)
        return ''.join(rc)

class Machine:
    log_path = ""
    
    def __init__(self, id, ip, username):
        self.id = id
        self.ip = ip
        self.username = username
    
    def get_id(self):
        return self.id
    
    def get_ip(self):
        return self.ip
    
    def get_username(self):
        return self.username
    
    def get_log_path(self):
        return self.log_path
    
    def set_log_path(self, path):
        self.log_path = path