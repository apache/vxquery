#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

mvn assembly:assembly

export ARCHIVE=apache-vxquery-0.2-incubating-src.zip
gpg2 --armor --output ${ARCHIVE}.asc --detach-sig target/${ARCHIVE}
cat target/${ARCHIVE} | openssl dgst -sha1 | sed -e's/(stdin)= //' > ${ARCHIVE}.sha1
cat target/${ARCHIVE} | openssl dgst -md5 | sed -e's/(stdin)= //' > ${ARCHIVE}.md5

#mvn site site:deploy
