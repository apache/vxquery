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

# Weather data files created to manage the conversion process.
# Allows partition and picking up where you left off.
class WeatherDataFiles:

    INDEX_DATA_FILE_NAME = 0
    INDEX_DATA_STATUS = 1
    INDEX_DATA_FILE_COUNT = 2
    INDEX_DATA_FOLDER_DATA = 3

    DATA_FILE_START_INDEX = 0
    DATA_FILE_EXTENSION = ".dly"
    DATA_FILE_MISSING = "missing"
    DATA_FILE_INITIAL = "initialized"
    DATA_FILE_CREATED = "created"
    SEPERATOR = ","
    
    def __init__(self, base_path, progress_file_name="/tmp/_weather_data.csv"):
        self.base_path = base_path

        self.progress_file_name = progress_file_name
        
        self.current = self.DATA_FILE_START_INDEX
        self.progress_data = []

        
    def get_file_list(self):
        return glob.glob(self.base_path + "/*" + self.DATA_FILE_EXTENSION)

    def get_file_list_iterator(self):
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
                file_name = self.progress_data[row].rsplit(self.SEPERATOR)[self.INDEX_DATA_FILE_NAME]
                if self.get_file_row(file_name) < 0 and 'append' in options: 
                    self.progress_data.append(self.get_progress_csv_row(file_name, self.DATA_FILE_INITIAL))
                elif 'recalculate' in options:
                    # The folder is hard coded
                    station_id = os.path.basename(file_name).split('.')[0]
                    folder_name = convert.get_base_folder(station_id)
                    if os.path.exists(folder_name):
                        file_count = self.get_file_count(folder_name)
                        data_size = self.get_folder_size(folder_name)
                        self.progress_data[row] = self.get_progress_csv_row(file_name, self.DATA_FILE_CREATED, file_count, data_size)
                    else:
                        self.progress_data[row] = self.get_progress_csv_row(file_name, self.DATA_FILE_INITIAL)
            # Save file
            self.close_progress_data(True)
        self.reset()
        
    # Save Functions
    def build_partition_structure(self, nodes, partitions):
        self.open_progress_data()
        row_count = len(self.progress_data)
        
        # Get the dictionary of all the files and data sizes.
        csv_dict = dict()
        for row in range(0, row_count):
            file_name = self.progress_data[row].rsplit(self.SEPERATOR)[self.INDEX_DATA_FILE_NAME]
            folder_data = int(self.progress_data[row].rsplit(self.SEPERATOR)[self.INDEX_DATA_FOLDER_DATA])
            
            csv_dict[file_name] = folder_data
        
        # New sorted list.
        csv_sorted = sorted(csv_dict, key=csv_dict.get, reverse=True)
        
        # Initialize the partition variables.
        total_partitions = nodes * partitions
        current_partition = 0
        list_of_partitions = []
        for i in range(0, total_partitions):
            list_of_partitions.append(set())
        
        # Add the files in a round robin order.
        for item in csv_sorted:
            list_of_partitions[current_partition].add(item)
            current_partition += 1
            if current_partition >= total_partitions:
                current_partition = 0
                
        # Save list of files for each node's partitions.
        for i in range(0, nodes):
            for j in range(0, partitions):
                current_partition = (i * partitions) + j
                self.write_partition_file(i + 1, j + 1, list_of_partitions[current_partition])
        
        
    # Write out the partition file list to a CSV file.
    def write_partition_file(self, node, partition, items):
        save_partition_file = "node_" + str(node) + "_level_" + str(partition) + ".csv"
        file = open(save_partition_file, 'w')
        contents = ""
        for file_name in items:
            contents += self.get_progress_csv_row(file_name, self.DATA_FILE_INITIAL)
        file.write(contents)
        file.close()
        
        
    def get_file_row(self, file_name):
        for i in range(0, len(self.progress_data)):
            if self.progress_data[i].startswith(file_name):
                return i
        return -1
        
    def get_default_progress_file_csv(self):
        contents = ""
        for path in self.get_file_list_iterator():
            file_name = os.path.basename(path)
            contents += self.get_progress_csv_row(file_name, self.DATA_FILE_INITIAL)
        return contents
    
    def print_progress_file_stats(self, convert):
        station_count_missing = 0
        station_count = 0
        file_count = 0
        data_size = 0
        
        station_count_actual = 0
        file_count_actual = 0
        data_size_actual = 0
        
        self.open_progress_data()
        row_count = len(self.progress_data)
        for row in range(0, row_count):
            row_contents = self.progress_data[row].rsplit(self.SEPERATOR)
            if int(row_contents[self.INDEX_DATA_FILE_COUNT]) != -1 and  int(row_contents[self.INDEX_DATA_FOLDER_DATA]) != -1:
                station_count += 1
                file_count += int(row_contents[self.INDEX_DATA_FILE_COUNT])
                data_size += int(row_contents[self.INDEX_DATA_FOLDER_DATA])
            else:
                station_count_missing += 1
                
            file_name = row_contents[self.INDEX_DATA_FILE_NAME]
            station_id = os.path.basename(file_name).split('.')[0]
            folder_name = convert.get_base_folder(station_id)
            if os.path.exists(folder_name):
                station_count_actual += 1
                file_count_actual += self.get_file_count(folder_name)
                data_size_actual += self.get_folder_size(folder_name)


        print "Progress File:\t" + self.progress_file_name + "\n"
        
        print "CSV DETAILS OF PROCESSED STATIONS"
        print "Number of stations:\t" + "{:,}".format(station_count)
        print "Number of files:\t" + "{:,}".format(file_count)
        print "Data size:\t\t" + sizeof_fmt(data_size) + "\n"

        print "CSV DETAILS OF unPROCESSED STATIONS"
        print "Number of stations:\t" + "{:,}".format(station_count_missing) + "\n"

        print "FOLDER DETAILS"
        print "Number of stations:\t" + "{:,}".format(station_count_actual)
        print "Number of files:\t" + "{:,}".format(file_count_actual)
        print "Data size:\t\t" + sizeof_fmt(data_size_actual) + "\n"

    
    def get_progress_csv_row(self, file_name, status, file_count=-1, data_size=-1):
        return file_name + self.SEPERATOR + status + self.SEPERATOR + str(file_count) + self.SEPERATOR + str(data_size) + "\n"
    
    def update_file_status(self, file_name, status, file_count=-1, data_size=-1):
        for i in range(0, len(self.progress_data)):
            if self.progress_data[i].startswith(file_name):
                self.progress_data[i] = self.get_progress_csv_row(file_name, status, file_count, data_size)
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
            if columns[self.INDEX_DATA_STATUS].strip() != self.DATA_FILE_CREATED:
                break
        return columns[self.INDEX_DATA_FILE_NAME]

# sizeof_fmt function is taken from an answer posted to stackoverflow.com.
#
# Question: 
#   http://stackoverflow.com/questions/1094841
# Answer Author: 
#   http://stackoverflow.com/users/55246/sridhar-ratnakumar
def sizeof_fmt(num):
    for x in ['bytes', 'KB', 'MB', 'GB']:
        if num < 1024.0 and num > -1024.0:
            return "%3.1f%s" % (num, x)
        num /= 1024.0
    return "%3.1f%s" % (num, 'TB')
    
