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

let $collection3 := "vxquery-benchmark/src/main/resources/xmark/data/people/"
for $p in collection($collection3)/site/people/person 
let $a :=
  let $collection1 := "vxquery-benchmark/src/main/resources/xmark/data/closed_auctions/"
  for $t in collection($collection1)/site/closed_auctions/closed_auction 
  where $p/@id = $t/buyer/@person
  return
    let $n := 
      let $collection2 := "vxquery-benchmark/src/main/resources/xmark/data/regions/"
      for $t2 in collection($collection2)/site/regions/europe/item 
      where $t/itemref/@item = $t2/@id 
      return $t2
    return <item>{$n/name/text()}</item>
return <person name="{$p/name/text()}">{$a}</person>
