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

(: XQuery Self Join Query :)
(: Self join with all stations finding the difference in min and max       :)
(: temperature and get the average.                                        :)
fn:count(
    let $sensor_collection_min := "/tmp/1.0_partition_ghcnd_all_xml/sensors"
    for $r_min in collection($sensor_collection_min)
    for $data_min in $r_min("dataCollection")("data")
    let $sensor_collection_max := "/tmp/1.0_partition_ghcnd_all_xml/sensors"
    for $r_max in collection($sensor_collection_max)
    for $data_max in $r_max("dataCollection")("data")
    where $data_min("station") eq $data_max("station")
        and $data_min("date") eq $data_max("date")
        and $data_min("dataType") eq "TMIN"
        and fn:year-from-dateTime(xs:dateTime(fn:data($data_min("date")))) gt 2000
        and $data_max("dataType") eq "TMAX"
        and fn:year-from-dateTime(xs:dateTime(fn:data($data_max("date")))) gt 2000
    return $data_max("value") - $data_min("value")
)