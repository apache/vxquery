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

    DATA_LINKS_FOLDER = "data_links/"
    LARGE_FILE_ROOT_TAG = WeatherDataFiles.LARGE_FILE_ROOT_TAG
    QUERY_REPLACEMENT_KEY = "/tmp/1.0_partition_ghcnd_all_xml/"
    QUERY_MASTER_FOLDER = "../queries/"
    QUERY_FILE_LIST = [
                       "q00.xq",
                       "q01.xq",
                       "q02.xq",
                       "q03.xq",
                       "q04.xq",
                       "q05.xq",
                       "q06.xq",
                       "q07.xq"
                       ] 
    QUERY_UTILITY_LIST = [
                          "no_result.xq",
                          "sensor_count.xq",
                          "station_count.xq",
                          "q04_sensor.xq",
                          "q04_station.xq",
                          "q05_sensor.xq",
                          "q05_station.xq",
                          "q06_sensor.xq",
                          "q06_station.xq",
                          "q07_tmin.xq",
                          "q07_tmax.xq",
                          ] 
    BENCHMARK_LOCAL_TESTS = ["local_speed_up", "local_batch_scale_out"] 
    BENCHMARK_CLUSTER_TESTS = ["speed_up", "batch_scale_out"] 
    QUERY_COLLECTIONS = ["sensors", "stations"]

    SEPERATOR = "|"
    
    def __init__(self, base_paths, partitions, dataset, nodes):
        self.base_paths = base_paths
        self.partitions = partitions
        self.dataset = dataset
        self.nodes = nodes
        
    def print_partition_scheme(self):
        if (len(self.base_paths) == 0):
            return
        for test in self.dataset.get_tests():
            if test in self.BENCHMARK_LOCAL_TESTS:
                self.print_local_partition_schemes(test)
            elif test in self.BENCHMARK_CLUSTER_TESTS:
                self.print_cluster_partition_schemes(test)
            else:
                print "Unknown test."
                exit()
            
    def print_local_partition_schemes(self, test):
        node_index = 0
        virtual_disk_partitions = get_local_virtual_disk_partitions(self.partitions)
        for p in self.partitions:
            scheme = self.get_local_partition_scheme(test, p)
            self.print_partition_schemes(virtual_disk_partitions, scheme, test, p, node_index)
        
    def print_cluster_partition_schemes(self, test):
        node_index = self.get_current_node_index()
        virtual_disk_partitions = get_cluster_virtual_disk_partitions(self.nodes, self.partitions)
        for p in self.partitions:
            scheme = self.get_cluster_partition_scheme(test, p)
            self.print_partition_schemes(virtual_disk_partitions, scheme, test, p, node_index)
        
    def print_partition_schemes(self, virtual_partitions, scheme, test, partitions, node_id):
        print
        print "---------------- Partition Scheme --------------------"
        print "    Test: " + test
        print "    Virtual Partitions: " + str(virtual_partitions)
        print "    Disks: " + str(len(self.base_paths))
        print "    Partitions: " + str(partitions)
        print "    Node Id: " + str(node_id)
        
        if isinstance(scheme, (tuple, list, dict, set)) and len(scheme) > 0:
            folder_length = len(scheme[0][3]) + 5
            row_format = "{:>5} {:>5} {:>5} {:<" + str(folder_length) + "} {:<" + str(folder_length) + "}"
            HEADER = ("Disk", "Index", "Link", "Data Path", "Link Path")
            print row_format.format(*HEADER)
            for row in scheme:
                print row_format.format(*row)
            print
        else:
            print "    Scheme is EMPTY."

    def get_local_partition_scheme(self, test, partition):
        scheme = []
        virtual_disk_partitions = get_local_virtual_disk_partitions(self.partitions)
        data_schemes = get_disk_partition_scheme(0, virtual_disk_partitions, self.base_paths)
        link_base_schemes = get_disk_partition_scheme(0, partition, self.base_paths, self.DATA_LINKS_FOLDER + test)

        # Match link paths to real data paths.
        group_size = len(data_schemes) / len(link_base_schemes)
        for d in range(len(self.base_paths)):
            offset = 0
            for link_node, link_disk, link_virtual, link_index, link_path in link_base_schemes:
                if d == link_disk:
                    # Only consider a single disk at a time.
                    for data_node, data_disk, data_virtual, data_index, data_path in data_schemes:
                        if test == "local_speed_up" and data_disk == link_disk \
                                and offset <= data_index and data_index < offset + group_size:
                            scheme.append([data_disk, data_index, link_index, data_path, link_path])
                        elif test == "local_batch_scale_out" and data_disk == link_disk \
                                and data_index == link_index:
                            scheme.append([data_disk, data_index, link_index, data_path, link_path])
                    offset += group_size
        return scheme
    
    def get_cluster_partition_scheme(self, test, partition):
        node_index = self.get_current_node_index()
        if node_index == -1:
            print "Unknown host."
            return 
        
        scheme = []
        virtual_disk_partitions = get_cluster_virtual_disk_partitions(self.nodes, self.partitions)
        data_schemes = get_disk_partition_scheme(node_index, virtual_disk_partitions, self.base_paths)
        link_base_schemes = get_cluster_link_scheme(len(self.nodes), partition, self.base_paths, self.DATA_LINKS_FOLDER + test)

        # Match link paths to real data paths.
        for link_node, link_disk, link_virtual, link_index, link_path in link_base_schemes:
            # Prep
            if test == "speed_up":
                group_size = virtual_disk_partitions / (link_node + 1) / partition
            elif test == "batch_scale_out":
                group_size = virtual_disk_partitions / len(self.nodes) / partition
            else:
                print "Unknown test."
                return
            
            node_offset = group_size * node_index * partition
            node_offset += group_size * link_index
            has_data = True
            if link_node < node_index:
                has_data = False
    
            # Make links
            for date_node, data_disk, data_virtual, data_index, data_path in data_schemes:
                if has_data and data_disk == link_disk \
                        and node_offset <= data_index and data_index < node_offset + group_size:
                    scheme.append([link_disk, data_index, link_index, data_path, link_path])
            scheme.append([link_disk, -1, link_index, "", link_path])
        return scheme
    
    def build_data_links(self, reset):
        if (len(self.base_paths) == 0):
            return
        if reset:
            shutil.rmtree(self.base_paths[0] + self.DATA_LINKS_FOLDER)
        for test in self.dataset.get_tests():
            if test in self.BENCHMARK_LOCAL_TESTS:
                for i in self.partitions:
                    scheme = self.get_local_partition_scheme(test, i)
                    self.build_data_links_scheme(scheme)
                if 1 in self.partitions and len(self.base_paths) > 1:
                    scheme = self.build_data_links_local_zero_partition(test)
                    self.build_data_links_scheme(scheme)
            elif test in self.BENCHMARK_CLUSTER_TESTS:
                for i in self.partitions:
                    scheme = self.get_cluster_partition_scheme(test, i)
                    self.build_data_links_scheme(scheme)
                if 1 in self.partitions and len(self.base_paths) > 1:
                    scheme = self.build_data_links_cluster_zero_partition(test)
                    self.build_data_links_scheme(scheme)
            else:
                print "Unknown test."
                exit()
    
    def build_data_links_scheme(self, scheme):
        '''Build all the data links based on the scheme information.'''
        for (data_disk, data_index, partition, data_path, link_path) in scheme:
            self.add_collection_links_for(data_path, link_path, data_index)
    
    def build_data_links_cluster_zero_partition(self, test):
        '''Build a scheme for all data in one symbolically linked folder. (0 partition)'''
        scheme = []
        link_base_schemes = get_cluster_link_scheme(len(self.nodes), 1, self.base_paths, self.DATA_LINKS_FOLDER + test)
        for link_node, link_disk, link_virtual, link_index, link_path in link_base_schemes:
            new_link_path = self.get_zero_partition_path(link_node, self.DATA_LINKS_FOLDER + test + "/" + str(link_node) + "nodes")
            scheme.append([0, link_disk, 0, link_path, new_link_path])
        return scheme

    def build_data_links_local_zero_partition(self, test):
        '''Build a scheme for all data in one symbolically linked folder. (0 partition)'''
        scheme = []
        index = 0
        link_base_schemes = get_disk_partition_scheme(0, 1, self.base_paths, self.DATA_LINKS_FOLDER + test)
        for link_node, link_disk, link_virtual, link_index, link_path in link_base_schemes:
            if test == "local_batch_scale_out" and index > 0:
                continue
            new_link_path = self.get_zero_partition_path(link_node, self.DATA_LINKS_FOLDER + test)
            scheme.append([0, index, 0, link_path, new_link_path])
            index += 1
        return scheme

    def get_zero_partition_path(self, node, key):
        '''Return a partition path for the zero partition.'''
        base_path = self.base_paths[0]
        new_link_path = get_disk_partition_scheme(node, 1, [base_path], key)[0][PARTITION_INDEX_PATH]
        return new_link_path.replace("p1", "p0")
        
    def get_current_node_index(self):
        found = False
        node_index = 0
        for machine in self.nodes:
            if socket.gethostname().startswith(machine.get_node_name()):
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
            collection_index = collection_path + "index" + str(index)
            if not os.path.isdir(collection_path):
                os.makedirs(collection_path)
            if index >= 0:
                if os.path.islink(collection_index):
                    os.unlink(collection_index)
                os.symlink(real_path + collection + "/", collection_index)
            
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
        if 1 in self.partitions and len(self.base_paths) > 1:
            for n in range(len(self.nodes)):
                query_path = get_cluster_query_path(self.base_paths, test, 0, n)
                prepare_path(query_path, reset)
            
                # Copy query files.
                new_link_path = self.get_zero_partition_path(n, self.DATA_LINKS_FOLDER + test + "/" + str(n) + "nodes")
                self.copy_and_replace_query(query_path, [new_link_path])
        for n in range(len(self.nodes)):
            for p in self.partitions:
                query_path = get_cluster_query_path(self.base_paths, test, p, n)
                prepare_path(query_path, reset)
            
                # Copy query files.
                partition_paths = get_disk_partition_paths(n, p, self.base_paths, self.DATA_LINKS_FOLDER + test + "/" + str(n) + "nodes")
                self.copy_and_replace_query(query_path, partition_paths)

    def copy_local_query_files(self, test, reset):
        '''Determine the data_link path for local query files and copy with
        new location for collection.'''
        if 1 in self.partitions and len(self.base_paths) > 1:
            query_path = get_local_query_path(self.base_paths, test, 0)
            prepare_path(query_path, reset)
    
            # Copy query files.
            new_link_path = self.get_zero_partition_path(0, self.DATA_LINKS_FOLDER + test)
            self.copy_and_replace_query(query_path, [new_link_path])
        for p in self.partitions:
            query_path = get_local_query_path(self.base_paths, test, p)
            prepare_path(query_path, reset)
    
            # Copy query files.
            partition_paths = get_disk_partition_paths(0, p, self.base_paths, self.DATA_LINKS_FOLDER + test)
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
                    
            # Make a search replace for partition type.
            if self.dataset.get_partition_type() == "large_files":
                for line in fileinput.input(query_path + query_file, True):
                    sys.stdout.write(line.replace("/stationCollection", "/" + self.LARGE_FILE_ROOT_TAG + "/stationCollection"))
                for line in fileinput.input(query_path + query_file, True):
                    sys.stdout.write(line.replace("/dataCollection", "/" + self.LARGE_FILE_ROOT_TAG + "/dataCollection"))
                    
    def get_number_of_slices_per_disk(self):
        if len(self.dataset.get_tests()) == 0:
            print "No test has been defined in config file."
        else:
            for test in self.dataset.get_tests():
                if test in self.BENCHMARK_LOCAL_TESTS:
                    return get_local_virtual_disk_partitions(self.partitions)
                elif test in self.BENCHMARK_CLUSTER_TESTS:
                    return get_cluster_virtual_disk_partitions(self.nodes, self.partitions)
                else:
                    print "Unknown test."
                    exit()

def get_cluster_link_scheme(nodes, partition, base_paths, key="partitions"):        
    link_paths = []
    for n in range(0, nodes):
        new_link_path = get_disk_partition_scheme(n, partition, base_paths, key + "/" + str(n) + "nodes")
        link_paths.extend(new_link_path)
    return link_paths

def get_local_query_path(base_paths, test, partition):        
    return base_paths[0] + "queries/" + test + "/" + get_local_query_folder(len(base_paths), partition) + "/"

def get_local_query_folder(disks, partitions):        
    return "d" + str(disks) + "_p" + str(partitions)

def get_cluster_query_path(base_paths, test, partition, nodes):        
    return base_paths[0] + "queries/" + test + "/" + str(nodes) + "nodes/" + get_local_query_folder(len(base_paths), partition) + "/"

def get_cluster_virtual_disk_partitions(nodes, partitions):
    vp = get_local_virtual_disk_partitions(partitions)
    vn = calculate_partitions(range(1, len(nodes)+1, 1))
    return vp * vn

def get_local_virtual_disk_partitions(partitions):
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
