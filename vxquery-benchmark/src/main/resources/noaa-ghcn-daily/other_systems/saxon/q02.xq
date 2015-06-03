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
XQuery Aggregate Query
----------------------
Find the annual precipitation (PRCP) for a Syracuse, NY using the airport
weather station (USW00014771) report for 1999.                                     
:)
fn:sum(
    let $collection := "../../../../../../../weather_data/dataset-tiny-local/data_links/local_speed_up/d0_p1_i0/sensors/?select=*.xml;recurse=yes"
    for $r in collection($collection)/root/dataCollection/data
    where $r/station eq "GHCND:USW00014771" 
        and $r/dataType eq "PRCP" 
        and fn:year-from-dateTime(xs:dateTime(fn:data($r/date))) eq 1999
    return $r/value
) div 10
