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
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.RandomPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.vxquery.compiler.rewriter.rules.CollectionFileDomain;

import java.util.ArrayList;
import java.util.List;

/**
 * Datasource object for indexing.
 */
public class VXQueryIndexingDataSource extends AbstractVXQueryDataSource implements IDataSource<String> {

	protected Object[] types;

	protected IDataSourcePropertiesProvider propProvider;

	private VXQueryIndexingDataSource(int id, String collection, String elementPath, Object[] types,
			String functionCall) {
		this.dataSourceId = id;
		this.collectionName = collection;
		this.elementPath = elementPath;
		this.function = functionCall;
		this.collectionPartitions = collectionName.split(DELIMITER);

		this.types = types;
		final IPhysicalPropertiesVector vec = new StructuralPropertiesVector(
				new RandomPartitioningProperty(new CollectionFileDomain(collectionName)), new ArrayList<>());
		propProvider = new IDataSourcePropertiesProvider() {
			@Override
			public IPhysicalPropertiesVector computePropertiesVector(List<LogicalVariable> scanVariables) {
				return vec;
			}
		};
		this.tag = null;
		this.childSeq = new ArrayList<>();
		this.valueSeq = new ArrayList<>();
	}

	public static VXQueryIndexingDataSource create(int id, String collection, String index, Object type,
			String function) {
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

	public String getElementPath() {
		return elementPath;
	}

	public String[] getCollectionPartitions() {
		return collectionPartitions;
	}

	public void setCollectionPartitions(String[] collectionPartitions) {
		this.collectionPartitions = collectionPartitions;
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
	public void computeFDs(List scanVariables, List fdList) {
	}

	@Override
	public String toString() {
		return "VXQueryIndexingDataSource [collectionName=" + collectionName + ", elementPath=" + elementPath + " "
				+ "function=" + function + "]";
	}

	@Override
	public String getFunctionCall() {
		return function;
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

}
