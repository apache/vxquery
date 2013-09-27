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

import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSink;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.RandomPartitioningProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.ResultSetDomain;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;

public class QueryResultSetDataSink implements IDataSink {

    private ResultSetId id;
    private Object[] schemaTypes;

    public QueryResultSetDataSink(ResultSetId id, Object[] schemaTypes) {
        this.id = id;
        this.schemaTypes = schemaTypes;
    }

    @Override
    public ResultSetId getId() {
        return id;
    }

    @Override
    public Object[] getSchemaTypes() {
        return schemaTypes;
    }

    @Override
    public IPartitioningProperty getPartitioningProperty() {
        return new RandomPartitioningProperty(new ResultSetDomain());
    }
}