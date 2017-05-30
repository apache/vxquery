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
(: Find the annual precipitation (PRCP) for a Seattle using the airport       :)
(: station (US000000002) for 2002.                                            :)
fn:sum(
    for $r in collection("src/test/resources/TestSources/ghcnd/half_1/quarter_1|src/test/resources/TestSources/ghcnd/half_1/quarter_2|src/test/resources/TestSources/ghcnd/half_2/quarter_3|src/test/resources/TestSources/ghcnd/half_2/quarter_4")/dataCollection/data
    where $r/station eq "GHCND:US000000002" 
        and $r/dataType eq "PRCP" 
        and fn:year-from-dateTime(xs:dateTime(fn:data($r/date))) eq 2002
    return $r/value
) div 10
