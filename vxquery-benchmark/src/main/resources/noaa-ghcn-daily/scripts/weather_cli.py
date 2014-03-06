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
from weather_config import *
from weather_benchmark import *

DEBUG_OUTPUT = False
COMPRESSED = False

#
# Weather conversion for GHCN-DAILY files to xml.
#
# http://www1.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt
#
def main(argv):
    append = False
    max_records = 0
    process_file_name = ""
    reset = False
    section = "all"
    token = ""
    update = False
    xml_config_path = ""
    
    try:
        opts, args = getopt.getopt(argv, "acf:hl:m:ruvw:x:", ["file=", "locality=", "max_station_files=", "web_service=", "xml_config="])
    except getopt.GetoptError:
        print 'The file options for weather_cli.py were not correctly specified.'
        print 'To see a full list of options try:'
        print '  $ python weather_cli.py -h'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'Converting weather daily files to xml options:'
            print '    -a        Append the results to the progress file.'
            print '    -c        Compress the produced XML file with .gz.'
            print '    -f (str)  The file name of a specific station to process.'
            print '              * Helpful when testing a single stations XML file output.'
            print '    -l (str)  Select the locality of the scripts execution (download, progress_file, sensor_build, station_build, partition, statistics).'
            print '    -m (int)  Limits the number of files created for each station.'
            print '              * Helpful when testing to make sure all elements are supported for each station.'
            print '              Alternate form: --max_station_files=(int)'
            print '    -r        Reset the build process. (For one section or all sections depending on other parameters.)'
            print '    -u        Recalculate the file count and data size for each data source file.'
            print '    -v        Extra debug information.'
            print '    -w (str)  Downloads the station XML file form the web service.'
            print '    -x (str)  XML config file for weather data.'
            sys.exit()
        elif opt in ('-a', "--append"):
            append = True
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
            if arg in ("download", "progress_file", "sensor_build", "station_build", "partition", "test_links", "queries", "statistics"):
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
        elif opt == '-r':
            reset = True
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
        elif opt in ('-x', "--xml_config"):
            # check if file exists.
            if os.path.exists(arg):
                xml_config_path = arg
            else:
                print 'Error: Argument must be a xml file for --xml_config (-x).'
                sys.exit()

    # Required fields to run the script.
    if xml_config_path == "" or not os.path.exists(xml_config_path):
        print 'Error: The xml config option must be supplied: --xml_config (-x).'
        sys.exit()
    config = WeatherConfig(xml_config_path)
    
    # Required fields to run the script.
    if config.get_save_path() == "" or not os.path.exists(config.get_save_path()):
        print 'Error: The save directory option must be supplied in the config file.'
        sys.exit()

    # Set up downloads folder.
    download_path = config.get_save_path() + "/downloads"
    if section in ("all", "download"):
        print 'Processing the download section.'
        download = WeatherDownloadFiles(download_path)
        download.download_ghcnd_files(reset)
        download.download_mshr_files(reset)

        # Unzip the required file.
        download.unzip_ghcnd_package(config.get_package(), reset)
        download.unzip_mshr_files(reset)


    # Create some basic paths for save files and references.
    ghcnd_data_dly_path = download_path + '/' + config.get_package() + '/' + config.get_package()
    xml_data_save_path = config.get_save_path() + '/all_xml_files/'

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
        if append:
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
                (file_count, data_size) = convert.process_sensor_file(process_file_name, max_records, 4)
                data.update_file_sensor_status(process_file_name, WeatherDataFiles.DATA_FILE_GENERATED, file_count, data_size)
            else:
                data.update_file_sensor_status(process_file_name, WeatherDataFiles.DATA_FILE_MISSING)
        else:
            # process directory
            data.reset()
            data.set_type("sensor")
            data.set_data_reset(reset)
            for file_name in data:
                file_path = ghcnd_data_dly_path + '/' + file_name
                if os.path.exists(file_path):
                    (file_count, data_size) = convert.process_sensor_file(file_path, max_records, 4)
                    data.update_file_sensor_status(file_name, WeatherDataFiles.DATA_FILE_GENERATED, file_count, data_size)
                else:
                    data.update_file_sensor_status(file_name, WeatherDataFiles.DATA_FILE_MISSING)
                
    if section in ("all", "station_build"):
        print 'Processing the station_build section.'
        data.reset()
        data.set_type("station")
        data.set_data_reset(reset)
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
                    
    for dataset in config.get_dataset_list():
        # Set up the setting for each dataset.
        dataset_folder = "/dataset-" + dataset.get_name()
        progress_file = config.get_save_path() + dataset_folder + "/_data_progress.csv"
        data = WeatherDataFiles(ghcnd_data_dly_path, progress_file)

        base_paths = []
        for paths in dataset.get_save_paths():
            base_paths.append(paths + dataset_folder + "/")
        benchmark = WeatherBenchmark(base_paths, dataset.get_partitions(), dataset, config.get_node_machine_list())
        
        if section in ("all", "partition"):
            slices = benchmark.get_number_of_slices()
            print 'Processing the partition section (' + dataset.get_name() + ':d' + str(len(base_paths)) + ':s' + str(slices) + ').'
            data.reset()
            data.copy_to_n_partitions(xml_data_save_path, slices, base_paths)
    
        if section in ("all", "test_links"):
            # TODO determine current node 
            print 'Processing the test links section (' + dataset.get_name() + ').'
            benchmark.build_data_links(xml_data_save_path)

        if section in ("all", "queries"):
            print 'Processing the queries section (' + dataset.get_name() + ').'
            benchmark.copy_query_files()
    
#     if section in ("statistics"):
#         print 'Processing the statistics section.'
#         data.print_progress_file_stats(convert)
                  
if __name__ == "__main__":
    main(sys.argv[1:])
