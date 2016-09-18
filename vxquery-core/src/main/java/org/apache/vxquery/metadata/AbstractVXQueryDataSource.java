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

import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourcePropertiesProvider;

import java.util.List;

public abstract class AbstractVXQueryDataSource {
    protected static final String DELIMITER = "\\|";
    protected int dataSourceId;
    protected String collectionName;
    protected String[] collectionPartitions;
    protected String elementPath;
    protected List<Integer> childSeq;
<<<<<<< 9f1b465c615e96008beb2f6ef02e530302b6bfe9
    protected List<Integer> valueSeq;
=======
    protected List<Byte[]> valueSeq;
>>>>>>> Implementation of PushValueIntoDatascanRule
    protected int totalDataSources;
    protected String tag;
    protected String function;

    protected Object[] types;

    protected IDataSourcePropertiesProvider propProvider;

    public abstract String getFunctionCall();
}
