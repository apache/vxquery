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
XQuery Filter Query
-------------------
See historical data for Key West International Airport, FL (USW00012836)
station by selecting  the weather readings for December 25 over the last 
10 years. 
:)
let $collection := "/tmp/1.0_partition_ghcnd_all_xml/sensors"
for $r in collection($collection)
for $data in $r("results")()
let $datetime := xs:dateTime(fn:data($data("date")))
where $data("station") eq "GHCND:USW00012836" 
    and fn:year-from-dateTime($datetime) ge 2003
    and fn:month-from-dateTime($datetime) eq 12 
    and fn:day-from-dateTime($datetime) eq 25
return $data