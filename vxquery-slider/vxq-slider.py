#!/usr/bin/env python
"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""
import argparse
import json
import os
import tarfile
import zipfile


def archive(filename='vxquery.tar', dirname='../vxquery-cli/target/appassembler/lib/', path='vxquery/package/files/', filetype='tar'):
    """
    :param filename: the name of the archive file.
    :param dirname: the directory path containing the files/folder that will be archived.
    :param path: path to directory that will contain the archive file.
    :param filetype: what type of archive it will be, tar, zip.
    Creates an archive file containing the files that exist in the dirname directory and saves it in the path directory.
    By default it creates the tar file that contains the vxquery libraries.
    """
    path = path + filename
    # Delete old file.
    try:
        os.remove(path)
    except OSError:
        pass

    files = os.listdir(dirname)

    if filetype == 'tar':
        #Create new tar file.
        tFile = tarfile.open(path, 'a')
        for f in files:
            path = dirname + f
            tFile.add(path, arcname=f)

        tFile.close()
    else:
        zf = zipfile.ZipFile(filename, 'a')
        for directory, subdirs, files in os.walk(dirname):
            zf.write(directory)
            for filename in files:
                zf.write(os.path.join(directory, filename))
        zf.close()

def message(message):
    """
    :param message: message we want to print.
    Prints the given message
    """
    print '----------------------------------------------------------------'
    print message

if __name__ == "__main__":
    # Read user aguments for query file and output file.
    parser = argparse.ArgumentParser(description="VXQuery-Slider python script.")
    parser.add_argument('--query', type=str, dest='query', default="")
    parser.add_argument('--output', type=str, dest='output', default="/tmp/result")
    args = parser.parse_args()

    # Write user arguments to appConfig json file.
    with open('vxquery/appConfig.json', 'rw+') as data_file:
        data = json.load(data_file)
        query = data['global']['site.global.query']
        data['global']['site.global.query'] = args.query
        data['global']['site.global.result_dir'] = args.output
        data_file.seek(0)
        data_file.write(json.dumps(data, indent=4, sort_keys=True))
        data_file.truncate()
        data_file.close()

    # Create tar containing all the necessary libraries.
    archive()

    # Create zip with the vxquery slider package.
    os.chdir('vxquery')
    archive(filename='vxquery.zip', dirname='./', filetype='zip', path='./')

    # Destroy old cluster
    message('Destroying old package.')
    os.system('../bin/slider destroy vxquery')

    # Call slider to create new cluster with the new package
    message('Installing vxquery new package.')
    os.system('../bin/slider package --install --name vxquery --package vxquery.zip --replacepkg')

    # Create new cluster
    message('Creating and running the query.')
    os.system('../bin/slider create vxquery --template appConfig.json --resources resources.json')
