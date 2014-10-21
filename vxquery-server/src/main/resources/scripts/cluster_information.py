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
        return get_tag_text(self.config, "username")

    def get_java_opts(self):
        return get_tag_text(self.config, "java_opts")

    def get_master_node_machine(self):
        master_node = self.config.getElementsByTagName("master_node")[0]
        id = NodeXmlReader.get_cluster_id(master_node)
        ip = NodeXmlReader.get_cluster_ip(master_node)
        client_ip = NodeXmlReader.get_client_ip(master_node)
        client_port = NodeXmlReader.get_client_port(master_node)
        java_opts = NodeXmlReader.get_java_opts(master_node)
        if java_opts is "":
            java_opts = self.get_java_opts()
        username = self.get_username()
        return Machine(id, ip, username, client_ip, client_port, java_opts)

    def get_node_machine_list(self):
        nodes = []
        username = self.get_username()
        for node in self.config.getElementsByTagName("node"):
            id = NodeXmlReader.get_cluster_id(node)
            ip = NodeXmlReader.get_cluster_ip(node)
            java_opts = NodeXmlReader.get_java_opts(node)
            if java_opts is "":
                java_opts = self.get_java_opts()
            nodes.append(Machine(id, ip, username, "", "", java_opts))
        return nodes

class NodeXmlReader(object):
    ''' --------------------------------------------------------------------------
     Node Specific Functions
    -------------------------------------------------------------------------- '''
    @staticmethod
    def get_cluster_id(node):
        return get_tag_text(node, "id")

    @staticmethod
    def get_cluster_ip(node):
        return get_tag_text(node, "cluster_ip")

    @staticmethod
    def get_client_ip(node):
        return get_tag_text(node, "client_ip")

    @staticmethod
    def get_client_port(node):
        return get_tag_text(node, "client_port")

    @staticmethod
    def get_java_opts(node):
        return get_tag_text(node, "java_opts")

def get_tag_text(xml_node, tag):
    values = xml_node.getElementsByTagName(tag)
    if len(values) > 0:
        return get_text(values[0])
    else:
        return ""

def get_text(xml_node):
    rc = []
    for node in xml_node.childNodes:
        if node.nodeType == node.TEXT_NODE:
            rc.append(node.data)
    return ''.join(rc)

class Machine:
    java_opts = ""
    log_path = ""
    port = ""
    
    def __init__(self, id, ip, username, client_ip="", client_port="", java_opts=""):
        self.id = id
        self.ip = ip
        self.username = username
        self.client_ip = client_ip
        self.client_port = client_port
        self.java_opts = java_opts
    
    def get_id(self):
        return self.id
    
    def get_ip(self):
        return self.ip
    
    def get_java_opts(self):
        return self.java_opts
    
    def get_client_ip(self):
        return self.client_ip
    
    def get_client_port(self):
        return self.client_port
    
    def get_username(self):
        return self.username
    
    def get_log_path(self):
        return self.log_path
    
    def set_log_path(self, path):
        self.log_path = path
