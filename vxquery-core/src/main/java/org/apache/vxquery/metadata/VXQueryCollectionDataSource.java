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

import org.apache.vxquery.compiler.rewriter.rules.CollectionFileDomain;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSource;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourcePropertiesProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.RandomPartitioningProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;

public class VXQueryCollectionDataSource implements IDataSource<String> {
    private final int dataSourceId;
    private final String collectionName;
    private final List<Integer> childSeq;
    private int totalDataSources;

    private final Object[] types;

    private IDataSourcePropertiesProvider propProvider;

    public VXQueryCollectionDataSource(int id, String file, Object[] types) {
        this.dataSourceId = id;
        this.collectionName = file;
        this.types = types;
        final IPhysicalPropertiesVector vec = new StructuralPropertiesVector(new RandomPartitioningProperty(
                new CollectionFileDomain(collectionName)), new ArrayList<ILocalStructuralProperty>());
        propProvider = new IDataSourcePropertiesProvider() {
            @Override
            public IPhysicalPropertiesVector computePropertiesVector(List<LogicalVariable> scanVariables) {
                return vec;
            }
        };
        this.childSeq = new ArrayList<Integer>();
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

}
