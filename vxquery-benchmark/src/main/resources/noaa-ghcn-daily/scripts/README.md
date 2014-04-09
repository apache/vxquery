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
     
     
# Examples commands

Building


Partitioning
python weather_cli.py -x weather_example.xml

Linking