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
import sys, getopt

# Custom modules.
from weather_data_files import *
from weather_download_files import *
from weather_convert_to_xml import *

DEBUG_OUTPUT = False
COMPRESSED = False

#
# Weather conversion for GHCN-DAILY files to xml.
#
# http://www1.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt
#
def main(argv):
    max_records = 0
    package = "ghcnd_gsn"
    partitions = 0
    process_file_name = ""
    reset = False
    save_path = "/tmp"
    section = "all"
    token = ""
    update = False
    
    try:
        opts, args = getopt.getopt(argv, "a:cf:hl:m:p:rs:uvw:", ["max_station_files=", "file=", "locality=", "save_directory=", "package=", "partitions=", "nodes=", "web_service="])
    except getopt.GetoptError:
        print 'The file options for weather_cli.py were not correctly specified.'
        print 'To see a full list of options try:'
        print '  $ python weather_cli.py -h'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'Converting weather daily files to xml options:'
            print '    -a (int)  The number of partitions for creating split up generated data.'
            print '    -c        Compress the produced XML file with .gz.'
            print '    -f (str)  The file name of a specific station to process.'
            print '              * Helpful when testing a single stations XML file output.'
            print '    -l (str)  Select the locality of the scripts execution (download, progress_file, sensor_build, station_build, partition, statistics).'
            print '    -m (int)  Limits the number of files created for each station.'
            print '              * Helpful when testing to make sure all elements are supported for each station.'
            print '              Alternate form: --max_station_files=(int)'
            print '    -p (str)  The package used to generate files. (all, gsn, hcn)'
            print '    -r        Reset the build process. (For one section or all sections depending on other parameters.)'
            print '    -s (str)  The directory for saving the downloaded files and generated XML files.'
            print '    -u        Recalculate the file count and data size for each data source file.'
            print '    -v        Extra debug information.'
            print '    -w (str)  Downloads the station XML file form the web service.'
            sys.exit()
        elif opt in ('-a', "--partitions"):
            if arg.isdigit():
                partitions = int(arg)
            else:
                print 'Error: Argument must be an integer for --partitions (-a).'
                sys.exit()
        elif opt == '-c':
            global COMPRESSED
            COMPRESSED = True
        elif opt in ('-f', "--file"):
            # check if file exists.
            if os.path.exists(arg):
                process_file_name = arg
            else:
                print 'Error: Argument must be a file name for --file (-f).'
                sys.exit()
        elif opt in ('-l', "--locality"):
            if arg in ("download", "progress_file", "sensor_build", "station_build", "partition", "statistics"):
                section = arg
            else:
                print 'Error: Argument must be a string for --locality (-l) and a valid locality.'
                sys.exit()
        elif opt in ('-m', "--max_station_files"):
            if arg.isdigit():
                max_records = int(arg)
            else:
                print 'Error: Argument must be an integer for --max_station_files (-m).'
                sys.exit()
        elif opt in ('-p', "--package"):
            if arg in ("all", "gsn", "hcn"):
                package = "ghcnd_" + arg
            else:
                print 'Error: Argument must be an string for one of the known weather packages: "all", "gsn", "hcn"'
                sys.exit()
        elif opt == '-r':
            reset = True
        elif opt in ('-s', "--save_directory"):
            # check if file exists.
            if os.path.exists(arg):
                save_path = arg
            else:
                print 'Error: Argument must be a directory for --save_directory (-s).'
                sys.exit()
        elif opt == '-u':
            update = True
        elif opt == '-v':
            global DEBUG_OUTPUT
            DEBUG_OUTPUT = True
        elif opt == '-w':
            # check if file exists.
            if arg is not "":
                token = arg
            else:
                print 'Error: Argument must be a string --web_service (-w).'
                sys.exit()

    # Required fields to run the script.
    if save_path == "" or not os.path.exists(save_path):
        print 'Error: The save directory option must be supplied: --save_directory (-s).'
        sys.exit()

    # Set up downloads folder.
    download_path = save_path + "/downloads"
    if section in ("all", "download"):
        print 'Processing the download section.'
        download = WeatherDownloadFiles(download_path)
        download.download_all_files(reset)

        # Unzip the required file.
        download.unzip_package(package, reset)


    # Create some basic paths for save files and references.
    ghcnd_data_dly_path = download_path + '/' + package + '/' + package
    ghcnd_xml_path = save_path + "/1.0_partition_" + package + '_xml/'
    ghcnd_xml_gz_path = save_path + "/1.0_partition_" + package + '_xml_gz/'
    if COMPRESSED:
        xml_data_save_path = ghcnd_xml_gz_path
    else:
        xml_data_save_path = ghcnd_xml_path

    # Make sure the xml folder is available.
    if not os.path.isdir(xml_data_save_path):
        os.makedirs(xml_data_save_path)


    # Set up the XML build objects.
    convert = WeatherWebServiceMonthlyXMLFile(download_path, xml_data_save_path, COMPRESSED, DEBUG_OUTPUT)
    progress_file = xml_data_save_path + "_data_progress.csv"
    data = WeatherDataFiles(ghcnd_data_dly_path, progress_file)
    if section in ("all", "progress_file"):
        print 'Processing the progress_file section.'
        options = list()
        options.append('append')
        if update:
            options.append('recalculate')
        if reset:
            options.append('reset')
        data.build_progress_file(options, convert)
    
    if section in ("all", "sensor_build"):
        print 'Processing the sensor_build section.'
        if process_file_name is not "":
            # process a single file
            if os.path.exists(process_file_name):
                (file_count, data_size) = convert.process_sensor_file(process_file_name, max_records)
                data.update_file_sensor_status(process_file_name, WeatherDataFiles.DATA_FILE_GENERATED, file_count, data_size)
            else:
                data.update_file_sensor_status(process_file_name, WeatherDataFiles.DATA_FILE_MISSING)
        else:
            # process directory
            data.reset()
            data.set_type("sensor")
            for file_name in data:
                file_path = ghcnd_data_dly_path + '/' + file_name
                if os.path.exists(file_path):
                    (file_count, data_size) = convert.process_sensor_file(file_path, max_records)
                    data.update_file_sensor_status(file_name, WeatherDataFiles.DATA_FILE_GENERATED, file_count, data_size)
                else:
                    data.update_file_sensor_status(file_name, WeatherDataFiles.DATA_FILE_MISSING)
                
    if section in ("all", "station_build"):
        data.reset()
        data.set_type("station")
        if token is not "":
            convert.set_token(token)
        for file_name in data: 
            file_path = ghcnd_data_dly_path + '/' + file_name
            if os.path.exists(file_path):
                return_status = convert.process_station_file(file_path)
                status = data.get_station_status(return_status)
                data.update_file_station_status(file_name, status)
            else:
                data.update_file_station_status(file_name, WeatherDataFiles.DATA_FILE_MISSING)
                
    if section in ("all", "partition") and partitions > 1:
        print 'Processing the partition section.'
        data.reset()
        data.copy_to_n_partitions(xml_data_save_path, partitions)

    if section in ("all", "statistics"):
        print 'Processing the statistics section.'
        data.print_progress_file_stats(convert)
                
if __name__ == "__main__":
    main(sys.argv[1:])
