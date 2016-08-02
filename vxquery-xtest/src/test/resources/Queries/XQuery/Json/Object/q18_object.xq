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

(: Json Object Query :)
(: Issue VXQUERY-210 :)
{
    "xml" : <tra>123</tra> ,
    "array" : [1 ,2, 3],
    "object": {"name": "riyafa"},
    "boolean": fn:true(),
    "date" : xs:date("2002-10-10Z"),
    "dateTime" : xs:dateTime("2002-03-06T00:00:00Z"),
    "dayTimeDuration" : xs:dayTimeDuration("PT10H"),
    "byte" : xs:byte(0),
    "decimal" : xs:decimal(-3.14),
    "double" : xs:double(6.022E23),
    "duration" : xs:duration("P0Y1347M0D"),
    "float" : xs:float('1.2345e-2'),
    "gDay" : xs:gDay("---10"),
    "gMonth" : xs:gMonth("--05"),
    "gMonthDay" : xs:gMonthDay("--05-10"),
    "gYear" : xs:gYear("1999"),
    "gYearMonth" : xs:gYearMonth("2002-03"),
    "hexBinary" : xs:hexBinary("FF"),
    "int" : xs:int(42),
    "unsignedShort" : xs:unsignedShort(6553),
    "integer" : xs:integer(65537),
    "long" : xs:long("9223372036854775"),
    "negativeInteger" : xs:negativeInteger("-99999999999999999"),
    "nonPositiveInteger" : xs:nonPositiveInteger(-9999999999),
    "nonNegativeInteger" : xs:nonNegativeInteger(9999999999),
    "positiveInteger" : xs:positiveInteger(9999999999999999),
    "unsignedInt" : xs:unsignedInt(429496729),
    "unsignedLong" : xs:unsignedLong(9223372036854775),
    "NOTATION" : xs:NOTATION("prefix:local"),
    "short" : xs:short(-6553),
    "normalizedString" : xs:normalizedString("NCName"),
    "token" : xs:token("foo"),
    "language" : xs:language("en-US"),
    "NMTOKEN" :  xs:NMTOKEN("ab"),
    "NCname" : xs:NCName("foobar"),
    "ID" : xs:ID("foo"),
    "IDREF" : xs:IDREF("foo"),
    "ENTITY" :  xs:ENTITY("entity1"),
    "time" : xs:time("23:02:12Z")
    "sequence" : (1 ,2, 3),
    "attribute" : <a attr="content"/>,
    "null" : null,
    "comment" : <!-- a comment -->
}
