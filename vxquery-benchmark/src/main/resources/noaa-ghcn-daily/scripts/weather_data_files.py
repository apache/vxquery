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
import glob
import os.path
import linecache
import distutils.core

from weather_convert_to_xml import *
from collections import OrderedDict

# Weather data files created to manage the conversion process.
# Allows partition and picking up where you left off.
class WeatherDataFiles:

    LARGE_FILE_ROOT_TAG = "root"

    INDEX_DATA_FILE_NAME = 0
    INDEX_DATA_SENSORS_STATUS = 1
    INDEX_DATA_STATION_STATUS = 2
    INDEX_DATA_FILE_COUNT = 3
    INDEX_DATA_FOLDER_DATA = 4

    DATA_FILE_START_INDEX = 0
    DATA_FILE_EXTENSION = ".dly"
    DATA_FILE_MISSING = "missing"
    DATA_FILE_INITIAL = "initialized"
    DATA_FILE_DOWNLOADED = "downloaded"
    DATA_FILE_GENERATED = "generated"
    SEPERATOR = ","
    
    type = "sensor"
    data_reset = False
    
    def __init__(self, base_path, progress_file_name="/tmp/_weather_data.csv"):
        self.base_path = base_path

        self.progress_file_name = progress_file_name
        
        self.current = self.DATA_FILE_START_INDEX
        self.progress_data = []

    def get_file_list_iterator(self):
        """Return the list of files one at a time."""
        return glob.iglob(self.base_path + "/*" + self.DATA_FILE_EXTENSION)

    # Save Functions
    def build_progress_file(self, options, convert):
        if not os.path.isfile(self.progress_file_name) or 'reset' in options:
            # Build a new file.
            file = open(self.progress_file_name, 'w')
            contents = self.get_default_progress_file_csv()
            file.write(contents)
            file.close()
        elif 'append' in options or 'recalculate' in options:
            self.open_progress_data()
            row_count = len(self.progress_data)
            for row in range(0, row_count):
                row_contents = self.progress_data[row].rsplit(self.SEPERATOR)
                file_name = row_contents[self.INDEX_DATA_FILE_NAME]
                if self.get_file_row(file_name) < 0 and 'append' in options: 
                    self.progress_data.append(self.get_progress_csv_row(file_name, self.DATA_FILE_INITIAL, self.DATA_FILE_INITIAL))
                elif 'recalculate' in options:
                    # The folder is hard coded
                    station_id = os.path.basename(file_name).split('.')[0]
                    folder_name = convert.get_base_folder(station_id)
                    if os.path.exists(folder_name):
                        row_contents = self.progress_data[row].rsplit(self.SEPERATOR)
                        sensor_status = row_contents[self.INDEX_DATA_SENSORS_STATUS]
                        station_status = row_contents[self.INDEX_DATA_STATION_STATUS]
                        file_count = self.get_file_count(folder_name)
                        data_size = self.get_folder_size(folder_name)
                        self.progress_data[row] = self.get_progress_csv_row(file_name, sensor_status, station_status, file_count, data_size)
                    else:
                        self.progress_data[row] = self.get_progress_csv_row(file_name, self.DATA_FILE_INITIAL, self.DATA_FILE_INITIAL)
            # Save file
            self.close_progress_data(True)
        self.reset()
        
    def copy_to_n_partitions(self, save_path, partitions, base_paths, reset):
        """Once the initial data has been generated, the data can be copied into a set number of partitions. """
        if (len(base_paths) == 0):
            return
        
        # Initialize the partition paths.
        partition_paths = get_disk_partition_paths(0, partitions, base_paths)
        for path in partition_paths:
            # Make sure the xml folder is available.
            prepare_path(path, reset)

        import fnmatch
        import os
        
        # copy stations and sensors into each partition
        current_sensor_partition = 0
        current_station_partition = 0
        self.open_progress_data()
        row_count = len(self.progress_data)
        for row in range(0, row_count):
            row_contents = self.progress_data[row].rsplit(self.SEPERATOR)
            file_name = row_contents[self.INDEX_DATA_FILE_NAME]
            station_id = os.path.basename(file_name).split('.')[0]
               
            # Copy sensor files
            type = "sensors"
            file_path = build_base_save_folder(save_path, station_id, type) + station_id
            for root, dirnames, filenames in os.walk(file_path):
                for filename in fnmatch.filter(filenames, '*.xml'):
                    xml_path = os.path.join(root, filename)
                    new_file_base = build_base_save_folder(partition_paths[current_sensor_partition], station_id, type) + station_id
                    if not os.path.isdir(new_file_base):
                        os.makedirs(new_file_base)
                    shutil.copyfile(xml_path, new_file_base + "/" + filename)
                    current_sensor_partition += 1
                    if current_sensor_partition >= len(partition_paths):
                        current_sensor_partition = 0
            
            # Copy station files
            type = "stations"
            file_path = build_base_save_folder(save_path, station_id, type) + station_id + ".xml"
            new_file_base = build_base_save_folder(partition_paths[current_station_partition], station_id, type)
            new_file_path = new_file_base + station_id + ".xml"
            if os.path.isfile(file_path):
                if not os.path.isdir(new_file_base):
                    os.makedirs(new_file_base)
                shutil.copyfile(file_path, new_file_path)
            current_station_partition += 1
            if current_station_partition >= len(partition_paths):
                current_station_partition = 0

    def build_to_n_partition_files(self, save_path, partitions, base_paths, reset):
        """Once the initial data has been generated, the data can be divided into partitions 
        and stored in single files.
        """
        if (len(base_paths) == 0):
            return
        
        XML_START = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        
        partition_paths = get_disk_partition_paths(0, partitions, base_paths)

        import fnmatch
        import os
        
        for path in partition_paths:
            prepare_path(path, reset)

        # Initialize the partition paths.
        types = ["sensors", "stations"]
        for type in types:
            partition_files = []
            for path in partition_paths:
                # Make sure the xml folder is available.
                prepare_path(path + type + "/", False)
                partition_files.append(open(path + type + "/partition.xml", 'w'))
                partition_files[-1].write(XML_START + "<" + self.LARGE_FILE_ROOT_TAG + ">\n")

            # copy into each partition
            current_partition = 0
            self.open_progress_data()
            row_count = len(self.progress_data)
            for row in range(0, row_count):
                row_contents = self.progress_data[row].rsplit(self.SEPERATOR)
                file_name = row_contents[self.INDEX_DATA_FILE_NAME]
                station_id = os.path.basename(file_name).split('.')[0]
                
                # Copy files
                if type == "sensors":
                    file_path = build_base_save_folder(save_path, station_id, type) + station_id
                    for root, dirnames, filenames in os.walk(file_path):
                        for filename in fnmatch.filter(filenames, '*.xml'):
                            xml_path = os.path.join(root, filename)
                            xml_data = file_get_contents(xml_path).replace(XML_START, "") + "\n"
                            partition_files[current_partition].write(xml_data)
                            current_partition += 1
                            if current_partition >= len(partition_files):
                                current_partition = 0
                elif type == "stations":
                    file_path = build_base_save_folder(save_path, station_id, type) + station_id + ".xml"
                    xml_path = os.path.join(root, file_path)
                    xml_data = file_get_contents(xml_path).replace(XML_START, "") + "\n"
                    partition_files[current_partition].write(xml_data)
                    current_partition += 1
                    if current_partition >= len(partition_paths):
                        current_partition = 0

            for row in range(0, len(partition_paths)):
                partition_files[row].write("</" + self.LARGE_FILE_ROOT_TAG + ">\n")
                partition_files[row].close()

    def get_file_row(self, file_name):
        for i in range(0, len(self.progress_data)):
            if self.progress_data[i].startswith(file_name):
                return i
        return -1
        
    def get_default_progress_file_csv(self):
        contents = ""
        for path in self.get_file_list_iterator():
            file_name = os.path.basename(path)
            contents += self.get_progress_csv_row(file_name, self.DATA_FILE_INITIAL, self.DATA_FILE_INITIAL)
        return contents
    
    def print_progress_file_stats(self, convert):
        sensor_count_missing = 0
        sensor_count = 0
        file_count = 0
        data_size = 0
        
        sensor_count_actual = 0
        file_count_actual = 0
        data_size_actual = 0
        
        station_count_missing = 0
        station_count_generated = 0
        station_count_downloaded = 0
        
        self.open_progress_data()
        row_count = len(self.progress_data)
        for row in range(0, row_count):
            row_contents = self.progress_data[row].rsplit(self.SEPERATOR)
            if int(row_contents[self.INDEX_DATA_FILE_COUNT]) != -1 and  int(row_contents[self.INDEX_DATA_FOLDER_DATA]) != -1:
                sensor_count += 1
                file_count += int(row_contents[self.INDEX_DATA_FILE_COUNT])
                data_size += int(row_contents[self.INDEX_DATA_FOLDER_DATA])
            else:
                sensor_count_missing += 1
                
            if row_contents[self.INDEX_DATA_STATION_STATUS] == "generated":
                station_count_generated += 1
            if row_contents[self.INDEX_DATA_STATION_STATUS] == "downloaded":
                station_count_downloaded += 1
            else:
                station_count_missing += 1

            file_name = row_contents[self.INDEX_DATA_FILE_NAME]
            station_id = os.path.basename(file_name).split('.')[0]
            folder_name = convert.get_base_folder(station_id)
            if os.path.exists(folder_name):
                sensor_count_actual += 1
                file_count_actual += self.get_file_count(folder_name)
                data_size_actual += self.get_folder_size(folder_name)


        print "Progress File:\t" + self.progress_file_name + "\n"
        
        print "CSV DETAILS OF PROCESSED SENSORS"
        print "Number of stations:\t" + "{:,}".format(sensor_count)
        print "Number of files:\t" + "{:,}".format(file_count)
        print "Data size:\t\t" + "{:,}".format(data_size) + " Bytes\n"

        print "CSV DETAILS OF unPROCESSED SENSORS"
        print "Number of stations:\t" + "{:,}".format(sensor_count_missing) + "\n"

        print "CSV DETAILS OF PROCESSED STATIONS"
        print "Generated:\t\t" + "{:,}".format(station_count_generated)
        print "Downloaded:\t\t" + "{:,}".format(station_count_downloaded)
        print "Missing:\t\t" + "{:,}".format(station_count_missing) + "\n"

        print "FOLDER DETAILS"
        print "Number of stations:\t" + "{:,}".format(sensor_count_actual)
        print "Number of files:\t" + "{:,}".format(file_count_actual)
        print "Data size:\t\t" + "{:,}".format(data_size_actual) + " Bytes\n"

    
    def get_progress_csv_row(self, file_name, sensors_status, station_status, file_count=-1, data_size=-1):
        return file_name + self.SEPERATOR + sensors_status + self.SEPERATOR + station_status + self.SEPERATOR + str(file_count) + self.SEPERATOR + str(data_size) + "\n"
    
    def update_file_sensor_status(self, file_name, sensors_status, file_count=-1, data_size=-1):
        for row in range(0, len(self.progress_data)):
            if self.progress_data[row].startswith(file_name):
                station_status = self.progress_data[row].rsplit(self.SEPERATOR)[self.INDEX_DATA_STATION_STATUS]
                self.progress_data[row] = self.get_progress_csv_row(file_name, sensors_status, station_status, file_count, data_size)
                break

        # Save the file            
        self.close_progress_data(True)

    def update_file_station_status(self, file_name, station_status):
        for row in range(0, len(self.progress_data)):
            if self.progress_data[row].startswith(file_name):
                row_contents = self.progress_data[row].rsplit(self.SEPERATOR)
                sensors_status = row_contents[self.INDEX_DATA_SENSORS_STATUS]
                file_count = int(row_contents[self.INDEX_DATA_FILE_COUNT])
                data_size = int(row_contents[self.INDEX_DATA_FOLDER_DATA])
                self.progress_data[row] = self.get_progress_csv_row(file_name, sensors_status, station_status, file_count, data_size)
                break

        # Save the file            
        self.close_progress_data(True)

    def get_file_count(self, folder_name):
        count = 0
        for dirpath, dirnames, filenames in os.walk(folder_name):
            for f in filenames:
                count += 1
        return count

    def get_folder_size(self, folder_name):
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(folder_name):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                total_size += os.path.getsize(fp)
        return total_size

    def get_station_status(self, return_value):
        if return_value == 2:
            return self.DATA_FILE_DOWNLOADED
        elif return_value == 1:
            return self.DATA_FILE_GENERATED
        return self.DATA_FILE_MISSING
        
    
    def open_progress_data(self):
        with open(self.progress_file_name, 'r') as file:
            self.progress_data = file.readlines()

    def close_progress_data(self, force=False):
        if len(self.progress_data) > 0 or force:
            with open(self.progress_file_name, 'w') as file:
                file.writelines(self.progress_data)

    
    def reset(self):
        self.close_progress_data()

        self.current = self.DATA_FILE_START_INDEX
        self.open_progress_data()

    def set_type(self, type):
        self.type = type

    def set_data_reset(self, data_reset):
        self.data_reset = data_reset


    # Iterator Functions
    def __iter__(self):
        return self

    def next(self):
        columns = []
        while True:
            # find a row that has not been created.
            if self.current >= len(self.progress_data):
                raise StopIteration
            row = self.progress_data[self.current]
            self.current += 1
            columns = row.rsplit(self.SEPERATOR)
            if self.type == "sensor" and (columns[self.INDEX_DATA_SENSORS_STATUS].strip() != self.DATA_FILE_GENERATED or self.data_reset):
                break
            elif self.type == "station" and (columns[self.INDEX_DATA_STATION_STATUS].strip() != self.DATA_FILE_DOWNLOADED or self.data_reset):
                break
        return columns[self.INDEX_DATA_FILE_NAME]
    
    
# Index values of each field details.
PARTITION_INDEX_NODE = 0
PARTITION_INDEX_DISK = 1
PARTITION_INDEX_VIRTUAL = 2
PARTITION_INDEX = 3
PARTITION_INDEX_PATH = 4
PARTITION_HEADER = ("Node", "Disk", "Virtual", "Index", "Path")

def get_disk_partition_paths(node_id, partitions, base_paths, key="partitions"):
    partition_paths = []
    for scheme in get_disk_partition_scheme(node_id, partitions, base_paths, key):
        partition_paths.append(scheme[PARTITION_INDEX_PATH])
    return partition_paths

def get_disk_partition_scheme(node_id, virtual_disk_partitions, base_paths, key="partitions"):
    partition_scheme = []
    for i in range(0, virtual_disk_partitions):
        for j in range(0, len(base_paths)):
            new_partition_path = base_paths[j] + key + "/" + get_partition_folder(j, virtual_disk_partitions, i) + "/"
            partition_scheme.append((node_id, j, virtual_disk_partitions, i, new_partition_path))
    return partition_scheme

def get_partition_folder(disks, partitions, index):        
    return "d" + str(disks) + "_p" + str(partitions) + "_i" + str(index)

def prepare_path(path, reset):
    """Ensures the directory is available. If reset, then its a brand new directory."""
    if os.path.isdir(path) and reset:
        shutil.rmtree(path)
                
    if not os.path.isdir(path):
        os.makedirs(path)

def file_get_contents(filename):
    with open(filename) as f:
        return f.read()
