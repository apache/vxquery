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

(: XQuery Filter Query :)
(: Find all reading for hurricane force wind warning or extreme wind warning. :)
(: The warnings occur when the wind speed (AWND) exceeds 110 mph (49.1744     :)
(: meters per second). (Wind value is in tenth of a meter per second)         :)
let $collection := "ghcnd_quarter_1|ghcnd_quarter_2|ghcnd_quarter_3|ghcnd_quarter_4"
for $r in collection($collection)/dataCollection/data
where $r/dataType eq "AWND" and xs:decimal(fn:data($r/value)) gt 491.744
return $r
