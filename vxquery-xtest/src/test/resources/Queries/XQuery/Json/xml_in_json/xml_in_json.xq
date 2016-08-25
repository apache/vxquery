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

(: XML in JSON :)
{
    "element" : <mercury>Hg</mercury>,
    "atomic value" : xs:date("1896-01-24")
    "several dates" : [ xs:date("1066-10-14"), xs:date("1935-01-11"), xs:date("1989-11-09") ]
},
[ xs:date("1066-10-14"), <mercury>Hg</mercury>, "ice cream" ],
let $object := { "x" : 10, "y" : 12 }
let $x := $object("x")
return <x>{ $x }</x>
