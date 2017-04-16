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

import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourcePropertiesProvider;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;

public abstract class AbstractVXQueryDataSource implements IDataSource<String> {
    protected static final String DELIMITER = "\\|";
    protected int dataSourceId;
    protected String collectionName;
    protected String[] collectionPartitions;
    protected String elementPath;
    protected List<Integer> childSeq;
    protected int totalDataSources;
    protected String tag;
    protected String function;

    protected Object[] types;

    protected IDataSourcePropertiesProvider propProvider;

    public abstract String getFunctionCall();

    @Override
    public boolean isScanAccessPathALeaf() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public INodeDomain getDomain() {
        // TODO Auto-generated method stub
        return null;
    }

}
