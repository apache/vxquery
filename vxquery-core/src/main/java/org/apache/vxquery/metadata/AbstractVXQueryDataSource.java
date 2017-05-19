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

import java.util.List;

import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourcePropertiesProvider;
import org.apache.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;

public abstract class AbstractVXQueryDataSource implements IVXQueryDataSource {
    protected static final String DELIMITER = "\\|";
    protected int dataSourceId;
    protected String collectionName;
    protected String[] collectionPartitions;

    protected List<Integer> childSeq;
    protected List<Byte[]> valueSeq;
    protected int totalDataSources;
    protected String tag;

    protected Object[] types;

    protected IDataSourcePropertiesProvider propProvider;
    
    @Override
    public INodeDomain getDomain() {
        return null;
    }

    @Override
    public boolean isScanAccessPathALeaf() {
        return false;
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

    public int getPartitionCount() {
        return collectionPartitions.length;
    }

    public String getTag() {
        return this.tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
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
    public void computeFDs(List<LogicalVariable> scanVariables, List<FunctionalDependency> fdList) {
    }

    public void addChildSeq(int integer) {
        childSeq.add(integer);
    }

    public List<Integer> getChildSeq() {
        return childSeq;
    }

    public void addValueSeq(Byte[] value) {
        valueSeq.add(value);
    }

    public List<Byte[]> getValueSeq() {
        return valueSeq;
    }

    public String[] getPartitions() {
        return collectionPartitions;
    }

    public void setPartitions(String[] collectionPartitions) {
        this.collectionPartitions = collectionPartitions;
    }

    abstract public boolean usingIndex();
}
