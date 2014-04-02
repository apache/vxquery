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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSink;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSource;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.data.IAWriterFactory;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;
import edu.uci.ics.hyracks.algebricks.data.IResultSerializerFactoryProvider;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.runtime.serializer.ResultSerializerFactoryProvider;
import edu.uci.ics.hyracks.algebricks.runtime.writers.PrinterBasedWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IResultSerializerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.result.ResultWriterOperatorDescriptor;

public class VXQueryMetadataProvider implements IMetadataProvider<String, String> {
    String[] nodeList;

    public VXQueryMetadataProvider(String[] nodeList) {
        this.nodeList = nodeList;
    }

    @Override
    public IDataSource<String> findDataSource(String id) throws AlgebricksException {
        return null;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getScannerRuntime(IDataSource<String> dataSource,
            List<LogicalVariable> scanVariables, List<LogicalVariable> projectVariables, boolean projectPushed,
            IOperatorSchema opSchema, IVariableTypeEnvironment typeEnv, JobGenContext context,
            JobSpecification jobSpec, Object implConfig) throws AlgebricksException {
        VXQueryCollectionDataSource ds = (VXQueryCollectionDataSource) dataSource;
        RecordDescriptor rDesc = new RecordDescriptor(new ISerializerDeserializer[opSchema.getSize()]);
        IOperatorDescriptor scanner = new VXQueryCollectionOperatorDescriptor(jobSpec, ds, rDesc);

        AlgebricksPartitionConstraint constraint = getClusterLocations(nodeList, ds.getPartitionCount());
        return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(scanner, constraint);
    }

    public AlgebricksPartitionConstraint getClusterLocations() {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        if (availableProcessors < 1) {
            availableProcessors = 1;
        }
        return getClusterLocations(nodeList, availableProcessors);
    }

    private static AlgebricksPartitionConstraint getClusterLocations(String[] nodeList, int partitions) {
        ArrayList<String> locs = new ArrayList<String>();
        for (String node : nodeList) {
            for (int j = 0; j < partitions; j++) {
                locs.add(node);
            }
        }
        String[] cluster = new String[locs.size()];
        cluster = locs.toArray(cluster);
        return new AlgebricksAbsolutePartitionConstraint(cluster);
    }

    @Override
    public boolean scannerOperatorIsLeaf(IDataSource<String> dataSource) {
        return false;
    }

    @Override
    public Pair<IPushRuntimeFactory, AlgebricksPartitionConstraint> getWriteFileRuntime(IDataSink sink,
            int[] printColumns, IPrinterFactory[] printerFactories, RecordDescriptor inputDesc)
            throws AlgebricksException {
        return null;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getWriteResultRuntime(
            IDataSource<String> dataSource, IOperatorSchema propagatedSchema, List<LogicalVariable> keys,
            LogicalVariable payLoadVar, JobGenContext context, JobSpecification jobSpec) throws AlgebricksException {
        return null;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getInsertRuntime(IDataSource<String> dataSource,
            IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv, List<LogicalVariable> keys,
            LogicalVariable payLoadVar, RecordDescriptor recordDesc, JobGenContext context, JobSpecification jobSpec)
            throws AlgebricksException {
        return null;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getDeleteRuntime(IDataSource<String> dataSource,
            IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv, List<LogicalVariable> keys,
            LogicalVariable payLoadVar, RecordDescriptor recordDesc, JobGenContext context, JobSpecification jobSpec)
            throws AlgebricksException {
        return null;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexInsertRuntime(
            IDataSourceIndex<String, String> dataSource, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, ILogicalExpression filterExpr, RecordDescriptor recordDesc,
            JobGenContext context, JobSpecification spec) throws AlgebricksException {
        return null;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexDeleteRuntime(
            IDataSourceIndex<String, String> dataSource, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, ILogicalExpression filterExpr, RecordDescriptor recordDesc,
            JobGenContext context, JobSpecification spec) throws AlgebricksException {
        return null;
    }

    @Override
    public IDataSourceIndex<String, String> findDataSourceIndex(String indexId, String dataSourceId)
            throws AlgebricksException {
        return null;
    }

    @Override
    public IFunctionInfo lookupFunction(final FunctionIdentifier fid) {
        return new IFunctionInfo() {
            @Override
            public FunctionIdentifier getFunctionIdentifier() {
                return fid;
            }

            public boolean isFunctional() {
                return true;
            }
        };
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getResultHandleRuntime(IDataSink sink,
            int[] printColumns, IPrinterFactory[] printerFactories, RecordDescriptor inputDesc, boolean ordered,
            JobSpecification spec) throws AlgebricksException {
        QueryResultSetDataSink rsds = (QueryResultSetDataSink) sink;
        ResultSetId rssId = (ResultSetId) rsds.getId();

        IResultSerializerFactoryProvider resultSerializerFactoryProvider = ResultSerializerFactoryProvider.INSTANCE;
        IAWriterFactory writerFactory = PrinterBasedWriterFactory.INSTANCE;

        ResultWriterOperatorDescriptor resultWriter = null;
        try {
            IResultSerializerFactory resultSerializedAppenderFactory = resultSerializerFactoryProvider
                    .getAqlResultSerializerFactoryProvider(printColumns, printerFactories, writerFactory);
            resultWriter = new ResultWriterOperatorDescriptor(spec, rssId, ordered, false,
                    resultSerializedAppenderFactory);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }

        return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(resultWriter, null);
    }

}