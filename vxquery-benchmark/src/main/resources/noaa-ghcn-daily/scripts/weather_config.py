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

class WeatherConfig:
    def __init__(self, config_xml_file):
        self.config_xml_file = config_xml_file
        
        self.config = parse(self.config_xml_file)

    def get_save_path(self):
        return self.get_text(self.config.getElementsByTagName("save_path")[0])

    def get_package(self):
        return self.get_text(self.config.getElementsByTagName("package")[0])

    def get_node_machine_list(self):
        nodes = []
        for node in self.config.getElementsByTagName("node"):
            id = self.get_node_name(node)
            ip = self.get_node_ip(node)
            nodes.append(Machine(id, ip))
        return nodes

    def get_dataset_list(self):
        nodes = []
        for node in self.config.getElementsByTagName("dataset"):
            name = self.get_dataset_name(node)
            save_paths = self.get_dataset_save_paths(node)
            partitions = self.get_dataset_partitions(node)
            tests = self.get_dataset_tests(node)
            nodes.append(Dataset(name, save_paths, partitions, tests))
        return nodes


    # --------------------------------------------------------------------------
    # Node Specific Functions
    # --------------------------------------------------------------------------
    def get_node_ip(self, node):
        return self.get_text(node.getElementsByTagName("cluster_ip")[0])

    def get_node_name(self, node):
        return self.get_text(node.getElementsByTagName("id")[0])

    
    # --------------------------------------------------------------------------
    # Dataset Specific Functions
    # --------------------------------------------------------------------------
    def get_dataset_name(self, node):
        return self.get_text(node.getElementsByTagName("name")[0])

    def get_dataset_save_paths(self, node):
        paths = []
        for item in node.getElementsByTagName("save_path"):
            paths.append(self.get_text(item))
        return paths

    def get_dataset_partitions(self, node):
        paths = []
        for item in node.getElementsByTagName("partitions_per_path"):
            paths.append(int(self.get_text(item)))
        return paths

    def get_dataset_tests(self, node):
        tests = []
        for item in node.getElementsByTagName("test"):
            tests.append(self.get_text(item))
        return tests

    def get_text(self, xml_node):
        rc = []
        for node in xml_node.childNodes:
            if node.nodeType == node.TEXT_NODE:
                rc.append(node.data)
        return ''.join(rc)

class Machine:
    def __init__(self, id, ip):
        self.id = id
        self.ip = ip
    
    def get_node_name(self):
        return self.id
    
    def get_node_ip(self):
        return self.ip
    
    def __repr__(self):
        return self.id + "(" + self.ip + ")"
    
class Dataset:
    def __init__(self, name, save_paths, partitions, tests):
        self.name = name
        self.save_paths = save_paths
        self.partitions = partitions
        self.tests = tests
    
    def get_name(self):
        return self.name
    
    def get_save_paths(self):
        return self.save_paths
    
    def get_partitions(self):
        return self.partitions
    
    def get_tests(self):
        return self.tests
    
    def __repr__(self):
        return self.name + ":" + str(self.save_paths) + ":" + str(self.partitions)
    
