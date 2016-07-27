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

(: JSONiq Changes to value comparison semantics :)
1 eq null,
null eq 1,
null eq null,
1 ne null,
null ne 1,
null ne null,
null lt 1,
1 lt null,
null lt null,
null gt 1,
1 gt null,
null gt null,
2 ge null,
null ge 1,
null ge null,
null le 2,
1 le null,
null le null
