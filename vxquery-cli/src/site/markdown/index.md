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
# VXQuery CLI

VXQuery CLI is the command line utility which can be used to execute XQueries 
with ease. No pre-configuration needs to be done in order to execute an XQuery.

---

## Quick Start

***

### Requirements

- Apache VXQuery™ source archive (apache-vxquery-X.Y-source-release.zip)
- JDK >= 1.8
- Apache Maven >= 3.2

***

### Installing

VXQuery CLI comes bundled with the VXQuery source distribution 
(apache-vxquery-X.Y-source-release.zip).

First, run `mvn package`.

```
$ unzip apache-vxquery-X.Y-source-release.zip
$ cd apache-vxquery-X.Y
$ mvn package -DskipTests
```

**vxquery-cli** binaries are located at `vxquery-cli/target/appassembler/bin`. 
There are 2 files in this directory, **vxq** which is the bash executable for unix
based systems and **vxq.bat** for windows systems. Depending on the platform,
suitable executable needs to be selected.

***

### Executing a Query

#### Put the query into a file

VXQuery CLI takes a file location as the argument where this file includes the 
query(statement) to be executed. Suppose the following query needs to be executed.

```
for $x in doc("books.xml")/bookstore/book
where $x/price>30
order by $x/title
return $x/title
```
This statement is querying for the book titles in **books.xml** where price of
the book is greater than 30. Also this query asks for the results to be ordered by
*title* as well. Now, create a file (say **test.xq**) and put the above query as
the content.

**NOTE:** You can replace **books.xml** with any XML file that you have and want 
to run a query on.

#### Execute the query

We need to invoke the matching executable according to your platform (unix/windows) 
inside `vxquery-cli/target/appassembler/bin` directory. To execute the query, run:

```
sh ./apache-vxquery-X.Y/vxquery-cli/target/appassembler/bin/vxq path/to/test.xq
```

***

## Command Line Options

```
 -O N                      : Optimization Level. (default: Full Optimization)
 -available-processors N   : Number of available processors. (default: java's available processors)
 -buffer-size N            : Disk read buffer size in bytes.
 -compileonly              : Compile the query and stop.
 -frame-size N             : Frame size in bytes. (default: 65,536)
 -hdfs-conf VAL            : Directory path to Hadoop configuration files
 -join-hash-size N         : Join hash size in bytes. (default: 67,108,864)
 -local-node-controllers N : Number of local node controllers. (default: 1)
 -maximum-data-size N      : Maximum possible data size in bytes. (default: 150,323,855,000)
 -repeatexec N             : Number of times to repeat execution.
 -rest-ip-address VAL      : IP Address of the REST Server.
 -rest-port N              : Port of REST Server.
 -result-file VAL          : File path to save the query result.
 -showast                  : Show abstract syntax tree.
 -showoet                  : Show optimized expression tree.
 -showquery                : Show query string.
 -showrp                   : Show Runtime plan.
 -showtet                  : Show translated expression tree.
 -timing                   : Produce timing information.
 -timing-ignore-queries N  : Ignore the first X number of quereies.
```

**NOTE:** Normally, CLI starts a local VXQuery Server to execute the query. But,
if you already have a VXQuery Server running, you can send the query to the 
inbuilt *REST Server* running in that server by specifying the **port** and **ip address** 
of the REST Server through options `-rest-ip-address` and `-rest-port`.
