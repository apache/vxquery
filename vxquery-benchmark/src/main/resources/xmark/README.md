<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

XMark
=====

# Description

The original XMark queries and data generation are available at http://www.xml-benchmark.org/downloads.html, 
with the queries being viewable online at http://www.ins.cwi.nl/projects/xmark/Assets/xmlquery.txt. In the
query folder, we have separate query files singling out the exact syntax for the versions of these queries 
used in our VXQuery work with XMark; the differences are due to the way we have physically organized the 
XMark data for storage/parallelism reasons.
  

# Query Results

VXQuery is still being developed and the following outlines the results for each of the XMark queries.
All queries have been modified to use fn:collection instead of fn:doc. In addition the data file has been split up 
by the first child of the site tag. Example XML files and folders can be found under the data folder.

q01: Modified version works. (Full support with VXQUERY-125)
q02: VXQUERY-125
q03: VXQUERY-125 and VXQUERY-73
q04: VXQUERY-72 missing node functions
q05: Works.
q06: VXQUERY-126 needs unnesting implementation of //