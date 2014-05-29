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

(: XMark Query see README.md for full details. :)

<result>
  <preferred>
    {
      count(
        let $collection1 := "vxquery-benchmark/src/main/resources/xmark/data/people/"
        for $b in collection($collection1)/site/people/person/profile[@income >= 100000]
        return $b
      )
    }
  </preferred>
  <standard>
    {
      count(
        let $collection2 := "vxquery-benchmark/src/main/resources/xmark/data/people/"
         for $c in collection($collection2)/site/people/person/profile[@income < 100000 and @income >= 30000]
         return $c
      )
    }
  </standard>
  <challenge>
    {
      count(
        let $collection3 := "vxquery-benchmark/src/main/resources/xmark/data/people/"
        for $d in collection($collection3)/site/people/person/profile[@income < 30000]
        return $d
      )
    }
  </challenge>
  <na>
    {
      count(
        let $collection4 := "vxquery-benchmark/src/main/resources/xmark/data/people/"
        for $p in collection($collection4)/site/people/person
        where empty($p/profile/@income)
        return $p
      )
    }
  </na>
</result>
