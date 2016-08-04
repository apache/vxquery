<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

Weather Data Conversion To XML
=====================
Before procceeding to the actual process, be sure that you are familiar with
deploying a server and then getting it started. You can find information about
these steps in the official VXQuery website. It would, also, be good to verify
that you can ssh in different nodes without verifying the password since the
scripts require to ssh from the current node to different ones.

# Introduction

The NOAA has hosted DAILY GLOBAL HISTORICAL CLIMATOLOGY NETWORK (GHCN-DAILY) 
.dat files. Weather.gov has an RSS/XML feed that gives current weather sensor 
readings. Using the RSS feed as a template, the GHCN-DAILY historical 
information is used to generate past RSS feed XML documents. The process allows 
testing on a large set of information with out having to continually monitor 
the weather.gov site for all the weather details for years.

# Detailed Description

Detailed GHDN-DAILY information: 
<http://www1.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt>

The process takes a save folder for the data. The folder contains a several 
folders:

 - all_xml_files (The generated xml files for a given package)
 - downloads (All files taken from the NOAA HTTP site)
 - dataset-[name] (all files related to a single dataset)
     
To convert the weather data to XML, 4 stages have to be completed. The stages
are described below:

 - download (Dowload the weather data from the website)
 - progress_file (Verify that all the data have been downloaded)
 - sensor_build (Convert the sensor readings to XML files)
 - station_build (Convert the station data to XML files)

After the convertion is completed, the system has to be setup to execute some
queries to evaluate its performance. The steps for this procedure are described
below:

 - partition (The partition schemes are configured in an XML file. An example of
this file is the weather_example_cluster.xml. This stage configures in how many
partitions the raw data will be partitioned in each node)
 - test_links (establish the correspondence between the partitioned data and the raw data)
 - queries (creates a folder with all the XML queries)

     
# Examples commands
Downloading 
python weather_cli.py -l download -x weather_example.xml

Building
python weather_cli.py -l sensor_build -x weather_example.xml (-l station_build for the station data)

Partitioning
python weather_cli.py -l partition -x weather_example.xml

Linking
python weather_cli.py -l test_links -x weather_example.xml

Building queries
python weather_cli.py -l queries -x weather_example.xml

Executing queries
run_group_test.sh cluster_ip path/to/weather_folder

