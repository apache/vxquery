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

(: Search Lucene Index :)
(: Find all the weather readings for Washington state for a specific day    :)
(: 2002-2-2.                                                                  :)
for $s in collection("src/test/resources/TestSources/ghcnd/half_1/quarter_1|src/test/resources/TestSources/ghcnd/half_1/quarter_2|src/test/resources/TestSources/ghcnd/half_2/quarter_3|src/test/resources/TestSources/ghcnd/half_2/quarter_4")/stationCollection/station
for $r in collection("src/test/resources/TestSources/ghcnd/half_1/quarter_1|src/test/resources/TestSources/ghcnd/half_1/quarter_2|src/test/resources/TestSources/ghcnd/half_2/quarter_3|src/test/resources/TestSources/ghcnd/half_2/quarter_4")/dataCollection/data
    
where $s/id eq $r/station 
    and (some $x in $s/locationLabels satisfies ($x/type eq "ST" and fn:upper-case(fn:data($x/displayName)) eq "STATE 1"))
    and xs:dateTime(fn:data($r/date)) eq xs:dateTime("2002-02-02T00:00:00.000")
return $r
