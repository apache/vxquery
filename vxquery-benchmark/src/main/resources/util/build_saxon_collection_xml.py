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
import getopt, glob, os, sys
 
def main(argv):
    xml_folder = ""
     
    # Get the base folder
    try:
        opts, args = getopt.getopt(argv, "f:h", ["folder="])
    except getopt.GetoptError:
        print 'The file options for build_saxon_collection_xml.py were not correctly specified.'
        print 'To see a full list of options try:'
        print '  $ python build_saxon_collection_xml.py -h'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'Options:'
            print '    -f        The base folder to create collection XML file.'
            sys.exit()
        elif opt in ('-f', "--folder"):
            # check if file exists.
            if os.path.exists(arg):
                xml_folder = arg
            else:
                print 'Error: Argument must be a folder name for --folder (-f).'
                sys.exit()
  
    # Required fields to run the script.
    if xml_folder == "" or not os.path.exists(xml_folder):
        print 'Error: The folder path option must be supplied:  --folder (-f).'
        sys.exit()
      
    # find all XML files in folder
    collection_xml = "<collection>"
    for i in range(1, 5):
        # Search the ith directory level.
        search_pattern = xml_folder + ('/*' * i) + '.xml'
        for file_path in glob.iglob(search_pattern):
            collection_xml += '<doc href="' + str.replace(file_path, xml_folder, '') + '"/>'
    collection_xml += "</collection>"
          
    # create collection XML
    file = open('collection.xml', 'w')
    file.write(collection_xml)
    file.close()

if __name__ == "__main__":
    main(sys.argv[1:])
