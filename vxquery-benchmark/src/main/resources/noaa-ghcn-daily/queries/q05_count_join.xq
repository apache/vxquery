(: Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at
   
     http://www.apache.org/licenses/LICENSE-2.0
   
   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License. :)

(:
XQuery Join Aggregate Query
-------------------
Find the lowest recorded temperature (TMIN) in the United States for 2001.
:)
fn:count(
    let $station_collection := "/tmp/1.0_partition_ghcnd_all_xml/stations"
    for $s in collection($station_collection)/stationCollection/station
    
    let $sensor_collection := "/tmp/1.0_partition_ghcnd_all_xml/sensors"
    for $r in collection($sensor_collection)/dataCollection/data
    
    where $s/id eq $r/station
        and (some $x in $s/locationLabels satisfies ($x/type eq "CNTRY" and $x/id eq "FIPS:US"))
        and $r/dataType eq "TMIN" 
        and fn:year-from-dateTime(xs:dateTime(fn:data($r/date))) eq 2001
    return $r
)