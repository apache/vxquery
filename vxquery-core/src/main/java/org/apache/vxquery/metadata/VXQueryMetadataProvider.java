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

import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSink;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.data.IAWriterFactory;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.algebricks.data.IResultSerializerFactoryProvider;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.serializer.ResultSerializerFactoryProvider;
import org.apache.hyracks.algebricks.runtime.writers.PrinterBasedWriterFactory;
import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IResultSerializerFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.result.ResultWriterOperatorDescriptor;
import org.apache.vxquery.common.VXQueryCommons;
import org.apache.vxquery.compiler.rewriter.rules.AbstractCollectionRule;
import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class VXQueryMetadataProvider implements IMetadataProvider<String, String> {
    private final String[] nodeList;
    private final Map<String, File> sourceFileMap;
    private final StaticContext staticCtx;
    private final String hdfsConf;
    private final Map<String, NodeControllerInfo> nodeControllerInfos;

    public VXQueryMetadataProvider(String[] nodeList, Map<String, File> sourceFileMap, StaticContext staticCtx,
            String hdfsConf, Map<String, NodeControllerInfo> nodeControllerInfos) {
        this.nodeList = nodeList;
        this.sourceFileMap = sourceFileMap;
        this.staticCtx = staticCtx;
        this.hdfsConf = hdfsConf;
        this.nodeControllerInfos = nodeControllerInfos;
    }

    @Override
    public IDataSource<String> findDataSource(String id) throws AlgebricksException {
        return null;
    }

    public Map<String, File> getSourceFileMap() {
        return sourceFileMap;
    }

    public StaticContext getStaticContext() {
        return staticCtx;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getScannerRuntime(IDataSource<String> dataSource,
            List<LogicalVariable> scanVariables, List<LogicalVariable> projectVariables, boolean projectPushed,
            List<LogicalVariable> minFilterVars, List<LogicalVariable> maxFilterVars, IOperatorSchema opSchema,
            IVariableTypeEnvironment typeEnv, JobGenContext context, JobSpecification jobSpec, Object implConfig)
                    throws AlgebricksException {


        //Remove following redundant code
        IOperatorDescriptor scanner = null;
        AlgebricksPartitionConstraint constraint = null;
        if (VXQueryCommons.indexingFunctions.contains(
                AbstractCollectionRule.functionCall.getFunctionIdentifier())) {
            //Indexing
            VXQueryIndexingDataSource ds = (VXQueryIndexingDataSource) dataSource;
            if (sourceFileMap != null) {
                final int len = ds.getCollectionPartitions().length;
                String[] collectionPartitions = new String[len];
                for (int i = 0; i < len; ++i) {
                    String partition = ds.getCollectionPartitions()[i];
                    File mapped = sourceFileMap.get(partition);
                    collectionPartitions[i] = mapped != null ? mapped.toString() : partition;
                }
                ds.setCollectionPartitions(collectionPartitions);
            }
            RecordDescriptor rDesc = new RecordDescriptor(new ISerializerDeserializer[opSchema.getSize()]);
            scanner = new VXQueryIndexingOperatorDescriptor(jobSpec, ds, rDesc, this.hdfsConf,
                    this.nodeControllerInfos);
            constraint = getClusterLocations(nodeList, ds.getPartitionCount());

        } else if (VXQueryCommons.collectionFunctions.contains(
                AbstractCollectionRule.functionCall.getFunctionIdentifier())){
            // collection
            VXQueryCollectionDataSource ds = (VXQueryCollectionDataSource) dataSource;
            if (sourceFileMap != null) {
                final int len = ds.getPartitions().length;
                String[] collectionPartitions = new String[len];
                for (int i = 0; i < len; ++i) {
                    String partition = ds.getPartitions()[i];
                    File mapped = sourceFileMap.get(partition);
                    collectionPartitions[i] = mapped != null ? mapped.toString() : partition;
                }
                ds.setPartitions(collectionPartitions);
            }
            RecordDescriptor rDesc = new RecordDescriptor(new ISerializerDeserializer[opSchema.getSize()]);
            scanner = new VXQueryCollectionOperatorDescriptor(jobSpec, ds, rDesc, this.hdfsConf,
                    this.nodeControllerInfos);
            constraint = getClusterLocations(nodeList, ds.getPartitionCount());
        }

        return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(scanner, constraint);
    }

    public static AlgebricksPartitionConstraint getClusterLocations(String[] nodeList) {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        if (availableProcessors < 1) {
            availableProcessors = 1;
        }
        return getClusterLocations(nodeList, availableProcessors);
    }

    public static AlgebricksPartitionConstraint getClusterLocations(String[] nodeList, int partitions) {
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

    private void setSourceFileMap(IDataSource dataSource) {

    }
    @Override
    public boolean scannerOperatorIsLeaf(IDataSource<String> dataSource) {
        return false;
    }

    @Override
    public Pair<IPushRuntimeFactory, AlgebricksPartitionConstraint> getWriteFileRuntime(IDataSink sink,
            int[] printColumns, IPrinterFactory[] printerFactories, RecordDescriptor inputDesc)
                    throws AlgebricksException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getWriteResultRuntime(
            IDataSource<String> dataSource, IOperatorSchema propagatedSchema, List<LogicalVariable> keys,
            LogicalVariable payLoadVar, List<LogicalVariable> additionalNonKeyFields, JobGenContext context,
            JobSpecification jobSpec) throws AlgebricksException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getDeleteRuntime(IDataSource<String> dataSource,
            IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv, List<LogicalVariable> keys,
            LogicalVariable payLoadVar, List<LogicalVariable> additionalNonKeyFields, RecordDescriptor recordDesc,
            JobGenContext context, JobSpecification jobSpec) throws AlgebricksException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexDeleteRuntime(
            IDataSourceIndex<String, String> dataSource, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, List<LogicalVariable> additionalNonKeyFields,
            ILogicalExpression filterExpr, RecordDescriptor recordDesc, JobGenContext context, JobSpecification spec)
                    throws AlgebricksException {
        throw new UnsupportedOperationException();
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

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getInsertRuntime(IDataSource<String> dataSource,
            IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv, List<LogicalVariable> keys,
            LogicalVariable payLoadVar, List<LogicalVariable> additionalNonKeyFields, RecordDescriptor recordDesc,
            JobGenContext context, JobSpecification jobSpec, boolean bulkload) throws AlgebricksException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexInsertRuntime(
            IDataSourceIndex<String, String> dataSource, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, List<LogicalVariable> additionalNonKeyFields,
            ILogicalExpression filterExpr, RecordDescriptor recordDesc, JobGenContext context, JobSpecification spec,
            boolean bulkload) throws AlgebricksException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getTokenizerRuntime(
            IDataSourceIndex<String, String> dataSource, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, ILogicalExpression filterExpr, RecordDescriptor recordDesc,
            JobGenContext context, JobSpecification spec, boolean bulkload) throws AlgebricksException {
        throw new UnsupportedOperationException();
    }

}
