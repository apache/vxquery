/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.vxquery.xmlparser;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;

public class TreeNodeIdProvider implements ITreeNodeIdProvider {

    private final short partitionDataSource;
    private final short dataSouceScanId;
    private final byte dataSourceBits;
    private short currentId;
    private byte fileId;
    private static Map<String, Byte> Files = new HashMap<String, Byte>();
    // File ID 0 is reserved for element constructor
    private static byte nextFileId = 1;

    public TreeNodeIdProvider(short partitionDataSource, short dataSouceScanId, short totalDataSources) {
        this.partitionDataSource = partitionDataSource;
        this.dataSouceScanId = dataSouceScanId;
        this.dataSourceBits = getBitsNeeded(totalDataSources);
        currentId = 0;
        fileId = 0;
    }

    public TreeNodeIdProvider(short partitionDataSource, short dataSouceScanId, short totalDataSources, String uri) {
        this.partitionDataSource = partitionDataSource;
        this.dataSouceScanId = dataSouceScanId;
        this.dataSourceBits = getBitsNeeded(totalDataSources);
        currentId = 0;
        fileId = getFileId(uri);
    }

    public TreeNodeIdProvider(short partition) {
        this.partitionDataSource = partition;
        dataSouceScanId = 0;
        dataSourceBits = 0;
        currentId = 0;
        fileId = 0;
    }

    public TreeNodeIdProvider(short partition, String uri) {
        this.partitionDataSource = partition;
        dataSouceScanId = 0;
        dataSourceBits = 0;
        currentId = 0;
        fileId = getFileId(uri);
    }

    public byte getFileId(String uri) {
        // Try to get canonical path
        String path = null;
        try {
            path = new File(uri).getCanonicalPath();
        } catch (Exception e) {
            path = uri;
        }
        if (Files.containsKey(path)) {
            return Files.get(path);
        } else {
            Files.put(path, nextFileId);
            return nextFileId++;
        }
    }

    public int getId() {
        int p = partitionDataSource;
        int dssi = dataSouceScanId;
        int f = fileId;
        return (f << 24) | (p << 16) | (dssi << (16 - dataSourceBits)) | currentId++;
    }

    private byte getBitsNeeded(int number) {
        byte count = 0;
        while (number > 0) {
            count++;
            number = number >> 1;
        }
        return count;
    }
}
