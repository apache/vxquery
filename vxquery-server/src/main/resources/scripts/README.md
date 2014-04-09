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

Utilities for Cluster Operations
=====================

# Introduction

Helpful scripts to work with the VXQuery cluster.

## Cluster Cli

The CLI script includes options to deploy, start, and stop a cluster.

Example commands:
python cluster_cli.py -c ../conf/cluster.xml -a deploy -d /apache-vxquery/vxquery-server
python cluster_cli.py -c ../conf/cluster.xml -a deploy -d /apache-vxquery/vxquery-cli
python cluster_cli.py -c ../conf/cluster.xml -a start
python cluster_cli.py -c ../conf/cluster.xml -a stop