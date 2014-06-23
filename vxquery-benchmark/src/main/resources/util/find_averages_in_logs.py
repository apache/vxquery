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
import fnmatch
import getopt
import glob
import os
import sys
import csv

SEARCH_STRING = 'Average execution time:'

def find_files(directory, pattern):
    for root, dirs, files in os.walk(directory):
        for basename in files:
            if fnmatch.fnmatch(basename, pattern):
                yield (root, basename)
    
    
def main(argv):
    ''' Same as bash: find $FOLDER -type f -name "*.xml" -exec basename {} \; > list_xml.csv
    '''
    log_folder = ""
    save_file = ""
    data_type = ""
    
    # Get the base folder
    try:
        opts, args = getopt.getopt(argv, "f:hs:t:", ["folder=", "save_file=", "data_type="])
    except getopt.GetoptError:
        print 'The file options for list_xml_files.py were not correctly specified.'
        print 'To see a full list of options try:'
        print '  $ python list_xml_files.py -h'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'Options:'
            print '    -f        The base folder to build XML file list.'
            print '    -s        The save file.'
            sys.exit()
        elif opt in ('-f', "--folder"):
            # check if file exists.
            if os.path.exists(arg):
                log_folder = arg
            else:
                print 'Error: Argument must be a folder name for --folder (-f).'
                sys.exit()
        elif opt in ('-s', "--save_file"):
            save_file = arg
        elif opt in ('-t', "--data_type"):
            data_type = arg
  
    # Required fields to run the script.
    if log_folder == "" or not os.path.exists(log_folder):
        print 'Error: The folder path option must be supplied:  --folder (-f).'
        sys.exit()
    if save_file == "":
        print 'Error: The folder path option must be supplied:  --save_file (-s).'
        sys.exit()
      
    list_xml_csv = ''
    with open(save_file, 'w') as outfile:
        csvfile = csv.writer(outfile)
        for path, filename in find_files(log_folder, '*.log'):
            # Only write out a specific type of data xml documents found in a specific path.
            with open(path + "/" + filename) as infile:
                folders = path.replace(log_folder, "")
                for line in infile:
                    # Skip the root tags.
                    if line.startswith(SEARCH_STRING):
                        time_split = line.split(" ")
                        name_split = filename.split(".")
                        folder_split = folders.split("/")

                        # Build data row
                        row = folder_split
                        row.append(name_split[0])
                        row.append(time_split[3])
                        row.append(name_split[2])
                        csvfile.writerow(row)
        
          
if __name__ == "__main__":
    main(sys.argv[1:])
