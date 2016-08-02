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

import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourcePropertiesProvider;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.RandomPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.vxquery.compiler.rewriter.rules.CollectionFileDomain;

import java.util.ArrayList;
import java.util.List;

/**
 * Datasource object for indexing.
 */
public class VXQueryIndexingDataSource implements IDataSource<String> {
    private static final String DELIMITER = "\\|";
    private final int dataSourceId;
    private final String collectionName;
    private final String indexName;
    private final ArrayList<Integer> childSeq;
    private String[] collectionPartitions;
    private String[] indexPartitions;
    private int totalDataSources;
    private String tag;
    private String function;

    private final Object[] types;

    private IDataSourcePropertiesProvider propProvider;

    private VXQueryIndexingDataSource(int id, String collection, String index, Object[] types, String functionCall) {
        this.dataSourceId = id;
        this.collectionName = collection;
        this.indexName = index;
        this.function = functionCall;
        if (collectionName != null){
            collectionPartitions = collectionName.split(DELIMITER);
        }
        indexPartitions = indexName.split(DELIMITER);
        this.types = types;
        final IPhysicalPropertiesVector vec = new StructuralPropertiesVector(
                new RandomPartitioningProperty(new CollectionFileDomain(indexName)),
                new ArrayList<ILocalStructuralProperty>());
        propProvider = new IDataSourcePropertiesProvider() {
            @Override
            public IPhysicalPropertiesVector computePropertiesVector(List<LogicalVariable> scanVariables) {
                return vec;
            }
        };
        this.tag = null;
        this.childSeq = new ArrayList<>();
    }

    public static VXQueryIndexingDataSource create(int id, String collection, String index, Object type, String
            function) {
        return new VXQueryIndexingDataSource(id, collection, index, new Object[] { type }, function);
    }

    public int getTotalDataSources() {
        return totalDataSources;
    }

    public void setTotalDataSources(int totalDataSources) {
        this.totalDataSources = totalDataSources;
    }

    public int getDataSourceId() {
        return dataSourceId;
    }

    public String[] getCollectionPartitions() {
        return collectionPartitions;
    }

    public void setCollectionPartitions(String[] collectionPartitions) {
        this.collectionPartitions = collectionPartitions;
    }

    public int getPartitionCount() {
        return indexPartitions.length;
    }

    public String getTag() {
        return this.tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String[] getIndexPartitions() {
        return indexPartitions;
    }

    public void setIndexPartitions(String[] indexPartitions) {
        this.indexPartitions = indexPartitions;
    }

    public void addChildSeq(int integer) {
        childSeq.add(integer);
    }

    public List<Integer> getChildSeq() {
        return childSeq;
    }

    @Override
    public String getId() {
        return collectionName;
    }

    @Override
    public Object[] getSchemaTypes() {
        return types;
    }

    @Override
    public IDataSourcePropertiesProvider getPropertiesProvider() {
        return propProvider;
    }

    @Override
    public void computeFDs(List scanVariables, List fdList) {

    }

    @Override
    public String toString() {
        return "VXQueryIndexingDataSource [collectionName=" + collectionName + ", indexName=" + indexName + ", "
                + "function=" + function + "]";
    }
}
