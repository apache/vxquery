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
    
    fields = ("usr", "wai", "read")
    fields_indexes = (0, 3, 6)
    fields_conversion = (1, 1, 1024*1024)
    fields_min = [999, 999, 999]
    fields_max = [0, 0, 0]
    fields_sum = [0, 0, 0]
    poll_count = 0
    
    START_LINE = 7

    # Get the base folder
    try:
        opts, args = getopt.getopt(argv, "f:hs:t:", ["folder=", "save_file="])
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

    # Required fields to run the script.
    if log_folder == "" or not os.path.exists(log_folder):
        print 'Error: The folder path option must be supplied:  --folder (-f).'
        sys.exit()
    if save_file == "":
        print 'Error: The folder path option must be supplied:  --save_file (-s).'
        sys.exit()

    with open(save_file, 'w') as outfile:
        csvfile = csv.writer(outfile)
        for path, filename in find_files(log_folder, '*thread_*.csv'):
            keys = filename.split(".")[0].split("_")
            with open(path + "/" + filename, 'rU') as infile:
                csv_input_file = csv.reader(infile, quotechar='"', delimiter = ',')
                count = 0
                print 'Working on file: ' + filename
                for line in csv_input_file:
                    if (count > START_LINE):
                        for i, v in enumerate(fields_indexes):
                            if (len(line) <= v):
                                print 'Bad file: ' + filename
                                break
                            else:
                                if (float(line[v]) < fields_min[i]):
                                    fields_min[i] = float(line[v])
                                if (fields_max[i] < float(line[v])):
                                    fields_max[i] = float(line[v])
                                fields_sum[i] += float(line[v])
                                poll_count += 1
                    count += 1
                    
            # Build data row
            row = []
            if (poll_count > 0):
                row.extend(keys)
                for i, v in enumerate(fields_indexes):
                    row.append("%.2f" % (fields_min[i] / fields_conversion[i]))
                    row.append("%.2f" % (fields_max[i] / fields_conversion[i]))
                    row.append("%.2f" % (fields_sum[i] / fields_conversion[i] / poll_count))
                csvfile.writerow(row)
            
            # Reset
            fields_min = [999, 999, 999]
            fields_max = [0, 0, 0]
            fields_sum = [0, 0, 0]
            poll_count = 0

if __name__ == "__main__":
    main(sys.argv[1:])
