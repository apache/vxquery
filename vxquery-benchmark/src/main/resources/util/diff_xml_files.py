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
    f1 = ""
    f2 = ""
     
    # Get the base folder
    try:
        opts, args = getopt.getopt(argv, "h", ["f1=", "f2="])
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
        elif opt in ('--f1'):
            # check if file exists.
            if os.path.exists(arg):
                f1 = arg
            else:
                print 'Error: Argument must be a file name for --f1.'
                sys.exit()
        elif opt in ('--f2'):
            # check if file exists.
            if os.path.exists(arg):
                f2 = arg
            else:
                print 'Error: Argument must be a file name for --f2.'
                sys.exit()

    # Required fields to run the script.
    if f1 == "" or not os.path.exists(f1):
        print 'Error: The file path option must be supplied:  --f1.'
        sys.exit()
    if f2 == "" or not os.path.exists(f2):
        print 'Error: The file path option must be supplied:  --f2.'
        sys.exit()
      
    missing_in_f1 = []
    missing_in_f2 = []
    found_in_both = []
    
    with open(f1) as f:
        content_f1 = f.readlines()
    set_f1 = set(content_f1)
    
    
    with open(f2) as f:
        content_f2 = f.readlines()
    set_f2 = set(content_f2)
    
    missing_in_f1 = set_f2.difference(set_f1)
    missing_in_f2 = set_f1.difference(set_f2)
    found_in_both = set_f1.intersection(set_f2)
    
    print ""
    print "Missing files in " + f1
    for f1_name in missing_in_f1:
        print " + " + f1_name.strip()

    print ""
    print "Missing files in " + f2
    for f2_name in missing_in_f2:
        print " + " + f2_name.strip()
    
    offset = 40
    print ""
    print "XML Summary"
    print (" - Found in both:").ljust(offset) + str(len(found_in_both))
    print (" - " + f1 + " diff set vs list:").ljust(offset) + str(len(content_f1) - len(set_f1))
    print (" - " + f2 + " diff set vs list:").ljust(offset) + str(len(content_f2) - len(set_f2))
    print (" - " + f1 + " missing:").ljust(offset) + str(len(missing_in_f1))
    print (" - " + f2 + " missing:").ljust(offset) + str(len(missing_in_f2))
    

if __name__ == "__main__":
    main(sys.argv[1:])
