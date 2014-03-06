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

# List of required files for a build.
MSHR_URLS = []
MSHR_URLS.append('ftp://ftp.ncdc.noaa.gov/pub/data/homr/docs/MSHR_Enhanced_Table.txt')
MSHR_URLS.append('http://www.ncdc.noaa.gov/homr/file/mshr_enhanced.txt.zip')

# Index values of each field details.
MSHR_FIELD_INDEX_NAME = 0
MSHR_FIELD_INDEX_START = 1
MSHR_FIELD_INDEX_END = 2
MSHR_FIELD_INDEX_TYPE = 3

# Store the row details here.
MSHR_FIELDS = {}

# Details about the row.
MSHR_FIELDS['SOURCE_ID'] = ['SOURCE_ID', 1, 20, 'X(20)']
MSHR_FIELDS['SOURCE'] = ['SOURCE', 22, 31, 'X(10)']
MSHR_FIELDS['BEGIN_DATE'] = ['BEGIN_DATE', 33, 40, 'YYYYMMDD']
MSHR_FIELDS['END_DATE'] = ['END_DATE', 42, 49, 'YYYYMMDD']
MSHR_FIELDS['STATION_STATUS'] = ['STATION_STATUS', 51, 70, 'X(20)']
MSHR_FIELDS['NCDCSTN_ID'] = ['NCDCSTN_ID', 72, 91, 'X(20)']
MSHR_FIELDS['ICAO_ID'] = ['ICAO_ID', 93, 112, 'X(20)']
MSHR_FIELDS['WBAN_ID'] = ['WBAN_ID', 114, 133, 'X(20)']
MSHR_FIELDS['FAA_ID'] = ['FAA_ID', 135, 154, 'X(20)']
MSHR_FIELDS['NWSLI_ID'] = ['NWSLI_ID', 156, 175, 'X(20)']
MSHR_FIELDS['WMO_ID'] = ['WMO_ID', 177, 196, 'X(20)']
MSHR_FIELDS['COOP_ID'] = ['COOP_ID', 198, 217, 'X(20)']
MSHR_FIELDS['TRANSMITTAL_ID'] = ['TRANSMITTAL_ID', 219, 238, 'X(20)']
MSHR_FIELDS['GHCND_ID'] = ['GHCND_ID', 240, 259, 'X(20)']
MSHR_FIELDS['NAME_PRINCIPAL'] = ['NAME_PRINCIPAL', 261, 360, 'X(100)']
MSHR_FIELDS['NAME_PRINCIPAL_SHORT'] = ['NAME_PRINCIPAL_SHORT', 362, 391, 'X(30)']
MSHR_FIELDS['NAME_COOP'] = ['NAME_COOP', 393, 492, 'X(100)']
MSHR_FIELDS['NAME_COOP_SHORT'] = ['NAME_COOP_SHORT', 494, 523, 'X(30)']
MSHR_FIELDS['NAME_PUBLICATION'] = ['NAME_PUBLICATION', 525, 624, 'X(100)']
MSHR_FIELDS['NAME_ALIAS'] = ['NAME_ALIAS', 626, 725, 'X(100)']
MSHR_FIELDS['NWS_CLIM_DIV'] = ['NWS_CLIM_DIV', 727, 736, 'X(10)']
MSHR_FIELDS['NWS_CLIM_DIV_NAME'] = ['NWS_CLIM_DIV_NAME', 738, 777, 'X(40)']
MSHR_FIELDS['STATE_PROV'] = ['STATE_PROV', 779, 788, 'X(10)']
MSHR_FIELDS['COUNTY'] = ['COUNTY', 790, 839, 'X(50)']
MSHR_FIELDS['NWS_ST_CODE'] = ['NWS_ST_CODE', 841, 842, 'X(2)']
MSHR_FIELDS['FIPS_COUNTRY_CODE'] = ['FIPS_COUNTRY_CODE', 844, 845, 'X(2)']
MSHR_FIELDS['FIPS_COUNTRY_NAME'] = ['FIPS_COUNTRY_NAME', 847, 946, 'X(100)']
MSHR_FIELDS['NWS_REGION'] = ['NWS_REGION', 948, 977, 'X(30)']
MSHR_FIELDS['NWS_WFO'] = ['NWS_WFO', 979, 988, 'X(10)']
MSHR_FIELDS['ELEV_GROUND'] = ['ELEV_GROUND', 990, 1029, 'X(40)']
MSHR_FIELDS['ELEV_GROUND_UNIT'] = ['ELEV_GROUND_UNIT', 1031, 1050, 'X(20)']
MSHR_FIELDS['ELEV_BAROM'] = ['ELEV_BAROM', 1052, 1091, 'X(40)']
MSHR_FIELDS['ELEV_BAROM_UNIT'] = ['ELEV_BAROM_UNIT', 1093, 1112, 'X(20)']
MSHR_FIELDS['ELEV_AIR'] = ['ELEV_AIR', 1114, 1153, 'X(40)']
MSHR_FIELDS['ELEV_AIR_UNIT'] = ['ELEV_AIR_UNIT', 1155, 1174, 'X(20)']
MSHR_FIELDS['ELEV_ZERODAT'] = ['ELEV_ZERODAT', 1176, 1215, 'X(40)']
MSHR_FIELDS['ELEV_ZERODAT_UNIT'] = ['ELEV_ZERODAT_UNIT', 1217, 1236, 'X(20)']
MSHR_FIELDS['ELEV_UNK'] = ['ELEV_UNK', 1238, 1277, 'X(40)']
MSHR_FIELDS['ELEV_UNK_UNIT'] = ['ELEV_UNK_UNIT', 1279, 1298, 'X(20)']
MSHR_FIELDS['LAT_DEC'] = ['LAT_DEC', 1300, 1319, 'X(20)']
MSHR_FIELDS['LON_DEC'] = ['LON_DEC', 1321, 1340, 'X(20)']
MSHR_FIELDS['LAT_LON_PRECISION'] = ['LAT_LON_PRECISION', 1342, 1351, 'X(10)']
MSHR_FIELDS['RELOCATION'] = ['RELOCATION', 1353, 1414, 'X(62)']
MSHR_FIELDS['UTC_OFFSET'] = ['UTC_OFFSET', 1416, 1431, '9(16)']
MSHR_FIELDS['OBS_ENV'] = ['OBS_ENV', 1433, 1472, 'X(40) ']
MSHR_FIELDS['PLATFORM'] = ['PLATFORM', 1474, 1573, 'X(100)']
