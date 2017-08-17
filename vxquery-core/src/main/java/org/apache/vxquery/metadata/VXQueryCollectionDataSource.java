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

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourcePropertiesProvider;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.RandomPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.vxquery.compiler.rewriter.rules.CollectionFileDomain;

public class VXQueryCollectionDataSource extends AbstractVXQueryDataSource {
    private VXQueryCollectionDataSource(int id, String collection, Object[] types) {
        this.dataSourceId = id;
        this.collectionName = collection;
        this.collectionPartitions = collectionName.split(DELIMITER);
        this.types = types;

        final IPhysicalPropertiesVector vec = new StructuralPropertiesVector(
                new RandomPartitioningProperty(new CollectionFileDomain(collectionName)),
                new ArrayList<ILocalStructuralProperty>());
        this.propProvider = new IDataSourcePropertiesProvider() {
            @Override
            public IPhysicalPropertiesVector computePropertiesVector(List<LogicalVariable> scanVariables) {
                return vec;
            }
        };
        this.tag = null;
    }

    public static VXQueryCollectionDataSource create(int id, String collection, Object type) {
        return new VXQueryCollectionDataSource(id, collection, new Object[] { type });
    }

    @Override
    public String toString() {
        return "VXQueryCollectionDataSource [collectionName=" + collectionName + ", childSeq=" + childSeq
                + ", valuePointableCount=" + valueOffsets.size() + "]";
    }

    public boolean usingIndex() {
        return false;
    }

}
