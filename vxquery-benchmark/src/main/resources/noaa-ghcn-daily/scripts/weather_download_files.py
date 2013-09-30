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
import os.path
import shutil
import tarfile
import urllib2

# Custom modules.
from weather_dly_config import *

class WeatherDownloadFiles:

    def __init__(self, save_path):
        self.save_path = save_path
        
        if not os.path.isdir(save_path):
            os.makedirs(save_path)


    # Download the complete list
    def download_all_files(self, reset=False):
        for file_name in FILE_NAMES:
            url = BASE_DOWNLOAD_URL + file_name
            self.download_file(url, reset)

    # Download the file, unless it exists.
    def download_file(self, url, reset=False):
        file_name = self.save_path + "/" + url.split('/')[-1]

        if not os.path.isfile(file_name) or reset:
            download_file_with_status(url, file_name)

    # download_file_with_status function is based on a question posted to
    # stackoverflow.com.
    #
    # Question: 
    #   http://stackoverflow.com/questions/22676
    # Answer Authors: 
    #   http://stackoverflow.com/users/394/pablog
    #   http://stackoverflow.com/users/160206/bjorn-pollex
    def download_file_with_status(self, url, file_name):
        u = urllib2.urlopen(url)
        f = open(file_name, 'wb')
        meta = u.info()
        file_size = int(meta.getheaders("Content-Length")[0])
        print "Downloading: %s Bytes: %s" % (file_name, file_size)

        file_size_dl = 0
        block_sz = 8192
        while True:
            buffer = u.read(block_sz)
            if not buffer:
                break

            file_size_dl += len(buffer)
            f.write(buffer)
            status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
            status = status + chr(8) * (len(status) + 1)
            print status,

        f.close()

    # Unzip the package file, unless it exists.
    def unzip_package(self, package, reset=False):
        file_name = self.save_path + "/" + package + ".tar.gz"
        unzipped_path = self.save_path + "/" + package
        
        if os.path.isdir(unzipped_path) and reset:
            shutil.rmtree(unzipped_path)
            
        if not os.path.isdir(unzipped_path):
            print "Unzipping: " + file_name
            tar_file = tarfile.open(file_name, 'r:gz')
            tar_file.extractall(unzipped_path)
 