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
import os.path
import linecache
import distutils.core
import fileinput
import socket

from weather_config import *
from weather_data_files import *

# Weather data files created to manage the conversion process.
# Allows partition and picking up where you left off.
#
# benchmark_name/
#   data/
#   queries/
#   logs/
class WeatherBenchmark:

    QUERY_REPLACEMENT_KEY = "/tmp/1.0_partition_ghcnd_all_xml/"
    QUERY_MASTER_FOLDER = "../queries/"
    QUERY_FILE_LIST = ["q00.xq", "q01.xq", "q02.xq", "q03.xq", "q04.xq", "q05.xq", "q06.xq", "q07.xq"] 
    QUERY_UTILITY_LIST = ["sensor_count.xq", "station_count.xq", "q04_sensor.xq", "q04_station.xq", "q05_sensor.xq", "q05_station.xq", "q06_tmin.xq", "q06_tmax.xq", "q07_sensor.xq", "q07_station.xq"] 
    BENCHMARK_LOCAL_TESTS = ["local_speed_up", "local_batch_scale_out"] 
    BENCHMARK_CLUSTER_TESTS = ["speed_up", "batch_scale_out"] 
    QUERY_COLLECTIONS = ["sensors", "stations"]

    SEPERATOR = "|"
    
    def __init__(self, base_paths, partitions, dataset, nodes):
        self.base_paths = base_paths
        self.partitions = partitions
        self.dataset = dataset
        self.nodes = nodes
        
    def print_partition_scheme(self, xml_save_path):
        if (len(self.base_paths) == 0):
            return
        for test in self.dataset.get_tests():
            if test in self.BENCHMARK_LOCAL_TESTS:
                self.print_local_partition_schemes(test, xml_save_path)
            elif test in self.BENCHMARK_CLUSTER_TESTS:
                self.print_cluster_partition_schemes(test, xml_save_path)
            else:
                print "Unknown test."
                exit()
            
    def print_local_partition_schemes(self, test, xml_save_path):
        for i in self.partitions:
            scheme = self.get_local_partition_scheme(test, xml_save_path, i)
            virtual_partitions = get_local_virtual_partitions(self.partitions)
            self.print_partition_schemes(virtual_partitions, scheme, test, i)
        
    def print_cluster_partition_schemes(self, test, xml_save_path):
        scheme = self.get_cluster_partition_scheme(test, xml_save_path)
        virtual_partitions = get_cluster_virtual_partitions(self.nodes, self.partitions)
        self.print_partition_schemes(virtual_partitions, scheme, test, 0)
        
    def print_partition_schemes(self, virtual_partitions, scheme, test, partitions):
        print
        print "---------------- Partition Scheme --------------------"
        print "    Test: " + test
        print "    Virtual Partitions: " + str(virtual_partitions)
        print "    Disks: " + str(len(self.base_paths))
        print "    Partitions: " + str(partitions)
        
        if len(scheme) > 0:
            folder_length = len(scheme[0][3]) + 5
            row_format = "{:>5} {:>5} {:<" + str(folder_length) + "} {:<" + str(folder_length) + "}"
            HEADER = ("Index", "Link", "Data Path", "Link Path")
            print row_format.format(*HEADER)
            for row in scheme:
                print row_format.format(*row)
            print
        else:
            print "    Scheme is EMPTY."

    def get_local_partition_scheme(self, test, xml_save_path, partition):
        scheme = []
        virtual_partitions = get_local_virtual_partitions(self.partitions)
        data_schems = get_partition_scheme(virtual_partitions, self.base_paths)
        
        link_base_schemes = get_partition_scheme(partition, self.base_paths, "data_links/" + test)
        # Match link paths to real data paths.
        offset = 0
        group_size = len(data_schemes) / len(link_base_schemes)
        for link_disk, link_virtual, link_index, link_path in enumerate(link_base_schemes):
            for data_disk, data_virtual, data_index, data_path in enumerate(data_schemes):
                if test == "local_speed_up" and offset <= data_index and data_index < offset + group_size:
                    scheme.append([data_index, link_index, data_path, link_path])
                elif test == "local_batch_scale_out" and data_index == link_index:
                    scheme.append([data_index, link_index, data_path, link_path])
            offset += group_size
        return scheme
    
    def get_cluster_partition_scheme(self, test, xml_save_path):
        node_index = self.get_current_node_index()
        if node_index == -1:
            print "Unknown host."
            return 
        
        scheme = []
        virtual_partitions = get_cluster_virtual_partitions(self.nodes, self.partitions)
        data_paths = get_partition_paths(virtual_partitions, self.base_paths)
        link_base_paths = get_cluster_link_paths(len(self.nodes), self.base_paths, "data_links/" + test)

        # Match link paths to real data paths.
        link_base_paths.sort()
        for link_index, link_path in enumerate(link_base_paths):
            # Prep
            link_offset = link_index % len(self.nodes)
            disk_offset = link_index // len(self.nodes)
            if test == "speed_up":
                group_size = len(data_paths) / (link_offset + 1) / (len(self.base_paths))
            elif test == "batch_scale_out":
                group_size = len(data_paths) / len(self.nodes) / (len(self.base_paths))
            else:
                print "Unknown test."
                return
            node_offset = group_size * node_index
            for j in range(disk_offset):
                node_offset += len(data_paths) / (len(self.base_paths))
            has_data = True
            if link_offset < node_index:
                has_data = False
                    
            # Make links
            data_paths.sort()
            for data_index, data_path in enumerate(data_paths):
                if has_data and node_offset <= data_index and data_index < node_offset + group_size:
                    scheme.append([data_index, link_index, data_path, link_path])
            scheme.append([-1, link_index, "", link_path])
        return scheme
    
    def build_data_links(self, xml_save_path):
        if (len(self.base_paths) == 0):
            return
        for test in self.dataset.get_tests():
            if test in self.BENCHMARK_LOCAL_TESTS:
                for i in self.partitions:
                    scheme = self.get_local_partition_scheme(test, xml_save_path, i)
                    self.build_data_links_scheme(scheme)
            elif test in self.BENCHMARK_CLUSTER_TESTS:
                scheme = self.get_cluster_partition_scheme(test, xml_save_path)
                self.build_data_links_scheme(scheme)
            else:
                print "Unknown test."
                exit()
    
    def build_data_links_scheme(self, scheme):
        """Build all the data links based on the scheme information."""
        link_path_cleared = []
        for (data_index, partition, data_path, link_path) in scheme:
            if link_path not in link_path_cleared and os.path.isdir(link_path):
                shutil.rmtree(link_path)
                link_path_cleared.append(link_path)
            self.add_collection_links_for(data_path, link_path, data_index)
    
    def get_current_node_index(self):
        found = False
        node_index = 0
        for machine in self.nodes:
            if socket.gethostname() == machine.get_node_name():
                found = True
                break
            node_index += 1
    
        if found:
            return node_index
        else:
            return -1
    
    def add_collection_links_for(self, real_path, link_path, index):
        for collection in self.QUERY_COLLECTIONS:
            collection_path = link_path + collection + "/"
            if not os.path.isdir(collection_path):
                os.makedirs(collection_path)
            if index >= 0:
                os.symlink(real_path + collection + "/", collection_path + "index" + str(index))
            
    def copy_query_files(self, reset):
        for test in self.dataset.get_tests():
            if test in self.BENCHMARK_LOCAL_TESTS:
                self.copy_local_query_files(test, reset)
            elif test in self.BENCHMARK_CLUSTER_TESTS:
                self.copy_cluster_query_files(test, reset)
            else:
                print "Unknown test."
                exit()
            
    def copy_cluster_query_files(self, test, reset):
        '''Determine the data_link path for cluster query files and copy with
        new location for collection.'''
        partitions = self.dataset.get_partitions()[0]
        for i in range(len(self.nodes)):
            query_path = get_cluster_query_path(self.base_paths, test, i)
            prepare_path(query_path, reset)
        
            # Copy query files.
            partition_paths = get_cluster_link_paths_for_node(i, self.base_paths, "data_links/" + test)
            self.copy_and_replace_query(query_path, partition_paths)

    def copy_local_query_files(self, test, reset):
        '''Determine the data_link path for local query files and copy with
        new location for collection.'''
        for i in self.partitions:
            query_path = get_local_query_path(self.base_paths, test, i)
            prepare_path(query_path, reset)
    
            # Copy query files.
            partition_paths = get_partition_paths(i, self.base_paths, "data_links/" + test)
            self.copy_and_replace_query(query_path, partition_paths)

    def copy_and_replace_query(self, query_path, replacement_list):
        '''Copy the query files over to the query_path and replace the path
        for the where the collection data is located.'''
        for query_file in self.QUERY_FILE_LIST + self.QUERY_UTILITY_LIST:
            shutil.copyfile(self.QUERY_MASTER_FOLDER + query_file, query_path + query_file)
        
            # Make a search replace for each collection.
            for collection in self.QUERY_COLLECTIONS:
                replacement_list_with_type = []
                for replace in replacement_list:
                    replacement_list_with_type.append(replace + collection)

                replace_string = self.SEPERATOR.join(replacement_list_with_type)
                for line in fileinput.input(query_path + query_file, True):
                    sys.stdout.write(line.replace(self.QUERY_REPLACEMENT_KEY + collection, replace_string))
                    
    def get_number_of_slices(self):
        if len(self.dataset.get_tests()) == 0:
            print "No test has been defined in config file."
        else:
            for test in self.dataset.get_tests():
                if test in self.BENCHMARK_LOCAL_TESTS:
                    return get_local_virtual_partitions(self.partitions)
                elif test in self.BENCHMARK_CLUSTER_TESTS:
                    return get_cluster_virtual_partitions(self.nodes, self.partitions)
                else:
                    print "Unknown test."
                    exit()

def get_cluster_link_paths(nodes, base_paths, key="partitions"):        
    link_paths = []
    for i in range(0, nodes):
        new_link_path = get_cluster_link_paths_for_node(i, base_paths, key)
        link_paths.extend(new_link_path)
    return link_paths

def get_cluster_link_paths_for_node(node_id, base_paths, key="partitions"):        
    link_paths = []
    for j in range(0, len(base_paths)):
        new_link_path = base_paths[j] + key + "/" + str(node_id) + "nodes/"
        link_paths.append(new_link_path)
    return link_paths

def get_local_query_path(base_paths, test, partition):        
    return base_paths[0] + "queries/" + test + "/" + get_local_query_folder(len(base_paths), partition) + "/"

def get_local_query_folder(disks, partitions):        
    return "d" + str(disks) + "_p" + str(partitions)

def get_cluster_query_path(base_paths, test, nodes):        
    return base_paths[0] + "queries/" + test + "/" + str(nodes) + "nodes/"

def get_cluster_virtual_partitions(nodes, partitions):
    if len(partitions) != 1:
        print "Cluster configurations must only have one partition."
        exit()
    return calculate_partitions(range(len(nodes), 0, -1))

def get_local_virtual_partitions(partitions):
    return calculate_partitions(partitions)

def calculate_partitions(list):
    x = 1
    for i in list:
        if x % i != 0:
            if i % x == 0:
                x = i
            else:
                x *= i
    return x
