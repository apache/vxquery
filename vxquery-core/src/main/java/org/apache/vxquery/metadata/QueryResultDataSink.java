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
package org.apache.vxquery.metadata;

import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSink;
import org.apache.hyracks.algebricks.core.algebra.properties.FileSplitDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.RandomPartitioningProperty;
import org.apache.hyracks.api.io.FileSplit;

public class QueryResultDataSink implements IDataSink {
    private final FileSplit[] fileSplits;
    private final IPartitioningProperty pProperty;

    public QueryResultDataSink(FileSplit[] fileSplits) {
        this.fileSplits = fileSplits;
        pProperty = new RandomPartitioningProperty(new FileSplitDomain(fileSplits));
    }

    @Override
    public Object getId() {
        return null;
    }

    @Override
    public Object[] getSchemaTypes() {
        return null;
    }

    @Override
    public IPartitioningProperty getPartitioningProperty() {
        return pProperty;
    }

    public FileSplit[] getFileSplits() {
        return fileSplits;
    }
}
