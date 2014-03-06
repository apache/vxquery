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

# Base URL used to get all the required files.
BASE_DOWNLOAD_URL = 'http://www1.ncdc.noaa.gov/pub/data/ghcn/daily/'

# List of required files for a build.
FILE_NAMES = []
FILE_NAMES.append('ghcnd-countries.txt')
FILE_NAMES.append('ghcnd-inventory.txt')
FILE_NAMES.append('ghcnd-states.txt')
FILE_NAMES.append('ghcnd-stations.txt')
FILE_NAMES.append('ghcnd-version.txt')
FILE_NAMES.append('ghcnd_all.tar.gz')
FILE_NAMES.append('ghcnd_gsn.tar.gz')
FILE_NAMES.append('ghcnd_hcn.tar.gz')
FILE_NAMES.append('readme.txt')
FILE_NAMES.append('status.txt')

# Store the row details here.

# Index values of each field details.
FIELD_INDEX_NAME = 0
FIELD_INDEX_START = 1
FIELD_INDEX_END = 2
FIELD_INDEX_TYPE = 3

DLY_FIELD_ID = 0
DLY_FIELD_YEAR = 1
DLY_FIELD_MONTH = 2
DLY_FIELD_ELEMENT = 3

DLY_FIELD_DAY_OFFSET = 4
DLY_FIELD_DAY_FIELDS = 4

DLY_FIELDS = []

# Details about the row.
DLY_FIELDS.append(['ID', 1, 11, 'Character'])
DLY_FIELDS.append(['YEAR', 12, 15, 'Integer'])
DLY_FIELDS.append(['MONTH', 16, 17, 'Integer'])
DLY_FIELDS.append(['ELEMENT', 18, 21, 'Character'])

# Days in each row.
for i in range(1, 32):
    start = 22 + ((i - 1) * 8)
    DLY_FIELDS.append(['VALUE' + str(i), (start + 0), (start + 4), 'Integer'])
    DLY_FIELDS.append(['MFLAG' + str(i), (start + 5), (start + 5), 'Character'])
    DLY_FIELDS.append(['QFLAG' + str(i), (start + 6), (start + 6), 'Character'])
    DLY_FIELDS.append(['SFLAG' + str(i), (start + 7), (start + 7), 'Character'])

# Details about the row.
STATIONS_FIELDS = {}
STATIONS_FIELDS['ID'] = ['ID', 1, 11, 'Character']
STATIONS_FIELDS['LATITUDE'] = ['LATITUDE', 13, 20, 'Real']
STATIONS_FIELDS['LONGITUDE'] = ['LONGITUDE', 22, 30, 'Real']
STATIONS_FIELDS['ELEVATION'] = ['ELEVATION', 32, 37, 'Real']
STATIONS_FIELDS['STATE'] = ['STATE', 39, 40, 'Character']
STATIONS_FIELDS['NAME'] = ['NAME', 42, 71, 'Character']
STATIONS_FIELDS['GSNFLAG'] = ['GSNFLAG', 73, 75, 'Character']
STATIONS_FIELDS['HCNFLAG'] = ['HCNFLAG', 77, 79, 'Character']
STATIONS_FIELDS['WMOID'] = ['WMOID', 81, 85, 'Character']

# Details about the row.
COUNTRIES_FIELDS = {}
COUNTRIES_FIELDS['CODE'] = ['CODE', 1, 2, 'Character']
COUNTRIES_FIELDS['NAME'] = ['NAME', 4, 50, 'Character']

# Details about the row.
STATES_FIELDS = {}
STATES_FIELDS['CODE'] = ['CODE', 1, 2, 'Character']
STATES_FIELDS['NAME'] = ['NAME', 4, 50, 'Character']

# Details about the row.
INVENTORY_FIELDS = []
INVENTORY_FIELDS.append(['ID', 1, 11, 'Character'])
INVENTORY_FIELDS.append(['LATITUDE', 13, 20, 'Real'])
INVENTORY_FIELDS.append(['LONGITUDE', 22, 30, 'Real'])
INVENTORY_FIELDS.append(['ELEMENT', 32, 35, 'Character'])
INVENTORY_FIELDS.append(['FIRSTYEAR', 37, 40, 'Integer'])
INVENTORY_FIELDS.append(['LASTYEAR', 42, 45, 'Integer'])

