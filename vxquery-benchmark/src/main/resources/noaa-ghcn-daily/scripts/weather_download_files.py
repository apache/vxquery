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
import sys
import os.path
import shutil
import tarfile
import urllib
import zipfile

# Custom modules.
from weather_config_ghcnd import *
from weather_config_mshr import *

class WeatherDownloadFiles:

    def __init__(self, save_path):
        self.save_path = save_path
        
        if not os.path.isdir(save_path):
            os.makedirs(save_path)


    def download_ghcnd_files(self, reset=False):
        """Download the complete list."""
        for file_name in FILE_NAMES:
            url = BASE_DOWNLOAD_URL + file_name
            self.download_file(url, reset)

    def download_mshr_files(self, reset=False):
        for url in MSHR_URLS:
            self.download_file(url, reset)

    def download_file(self, url, reset=False):
        """Download the file, unless it exists."""
        file_name = self.save_path + "/" + url.split('/')[-1]

        if not os.path.isfile(file_name) or reset:
            print "Downloading: " + url
            urllib.urlretrieve(url, file_name, report_download_status)
            print

    def unzip_ghcnd_package(self, package, reset=False):
        """Unzip the package file, unless it exists."""
        file_name = self.save_path + "/" + package + ".tar.gz"
        unzipped_path = self.save_path + "/" + package
        
        if os.path.isdir(unzipped_path) and reset:
            shutil.rmtree(unzipped_path)
            
        if not os.path.isdir(unzipped_path):
            print "Unzipping: " + file_name
            tar_file = tarfile.open(file_name, 'r:gz')
            tar_file.extractall(unzipped_path)
 
    def unzip_mshr_files(self, reset=False):
        """Unzip the package file, unless it exists."""
        for url in MSHR_URLS:
            if url.endswith('.zip'):
                file_name = self.save_path + "/" + url.split('/')[-1]
                print "Unzipping: " + file_name
                with zipfile.ZipFile(file_name, 'r') as myzip:
                    myzip.extractall(self.save_path)
 
def report_download_status(count, block, size):
    """Report download status."""
    line_size = 50
    erase = "\b" * line_size
    sys.stdout.write(erase)
    report = get_report_line((float(count) * block / size), line_size)
    sys.stdout.write(report)

def get_report_line(percentage, line_size):
    """Creates a string to be used in reporting the percentage done."""
    report = ""
    for i in range(0, line_size):
        if (float(i) / line_size < percentage):
            report += "="
        else:
            report += "-"
    return report
            
def download_file_save_as(url, new_file_name, reset=False):
    """Download the file, unless it exists."""
    if not os.path.isfile(new_file_name) or reset:
        print "Downloading: " + url
        urllib.urlretrieve(url, new_file_name, report_download_status)
        print

