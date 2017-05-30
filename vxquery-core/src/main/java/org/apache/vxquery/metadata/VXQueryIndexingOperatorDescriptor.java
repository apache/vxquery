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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.logging.Logger;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameFieldAppender;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.FrameFixedFieldTupleAppender;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.functions.BuiltinFunctions;
import org.apache.vxquery.runtime.functions.index.IndexConstructorUtil;
import org.apache.vxquery.runtime.functions.index.VXQueryIndexReader;
import org.apache.vxquery.runtime.functions.index.centralizer.IndexCentralizerUtil;
import org.apache.vxquery.runtime.functions.index.update.IndexUpdater;
import org.apache.vxquery.xmlparser.ITreeNodeIdProvider;
import org.apache.vxquery.xmlparser.TreeNodeIdProvider;

public class VXQueryIndexingOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    protected static final Logger LOGGER = Logger.getLogger(VXQueryCollectionOperatorDescriptor.class.getName());
    private static final long serialVersionUID = 1L;
    private short dataSourceId;
    private short totalDataSources;
    private String[] collectionPartitions;
    private final String functionCall;
    private List<Integer> childSeq;

    public VXQueryIndexingOperatorDescriptor(IOperatorDescriptorRegistry spec, VXQueryIndexingDataSource ds,
            RecordDescriptor rDesc) {
        super(spec, 1, 1);
        this.functionCall = ds.getFunctionCall();
        collectionPartitions = ds.getPartitions();
        dataSourceId = (short) ds.getDataSourceId();
        totalDataSources = (short) ds.getTotalDataSources();
        recordDescriptors[0] = rDesc;
        childSeq = ds.getChildSeq();
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        final FrameTupleAccessor fta = new FrameTupleAccessor(
                recordDescProvider.getInputRecordDescriptor(getActivityId(), 0));
        final int fieldOutputCount = recordDescProvider.getOutputRecordDescriptor(getActivityId(), 0).getFieldCount();
        final IFrame frame = new VSizeFrame(ctx);
        final IFrameFieldAppender appender = new FrameFixedFieldTupleAppender(fieldOutputCount);
        final short partitionId = (short) ctx.getTaskAttemptId().getTaskId().getPartition();
        final ITreeNodeIdProvider nodeIdProvider = new TreeNodeIdProvider(partitionId, dataSourceId, totalDataSources);
        final String nodeId = ctx.getJobletContext().getApplicationContext().getNodeId();
        final String collectionName = collectionPartitions[partition % collectionPartitions.length];
        final String collectionModifiedName = collectionName.replace("${nodeId}", nodeId);
        IndexCentralizerUtil indexCentralizerUtil = new IndexCentralizerUtil(
                ctx.getIOManager().getIODevices().get(0).getMount());
        indexCentralizerUtil.readIndexDirectory();
        final IPointable result = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();

        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
            @Override
            public void open() throws HyracksDataException {
                appender.reset(frame, true);
                writer.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                fta.reset(buffer);

                final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
                final ArrayBackedValueStorage abvsFileNode = new ArrayBackedValueStorage();

                abvs.reset();
                abvsFileNode.reset();

                if (collectionModifiedName.contains("hdfs://")) {
                    throw new HyracksDataException("Indexing support for HDFS not yet implemented.");
                } else {

                    if (functionCall.equals(
                            BuiltinFunctions.FN_BUILD_INDEX_ON_COLLECTION_1.getFunctionIdentifier().getName())) {
                        try {
                            createIndex(result, abvs, abvsFileNode);
                        } catch (IOException e) {
                            throw new HyracksDataException(e);
                        }
                    } else if (functionCall
                            .equals(BuiltinFunctions.FN_UPDATE_INDEX_1.getFunctionIdentifier().getName())) {
                        updateIndex(result, abvs, abvsFileNode);
                    } else if (functionCall
                            .equals(BuiltinFunctions.FN_DELETE_INDEX_1.getFunctionIdentifier().getName())) {
                        deleteIndex(result, abvs, abvsFileNode);
                    } else if (functionCall
                            .equals(BuiltinFunctions.FN_COLLECTION_FROM_INDEX_1.getFunctionIdentifier().getName())
                            || functionCall
                                    .equals(BuiltinFunctions.FN_COLLECTION_1.getFunctionIdentifier().getName())) {
                        usingIndex(result);
                    } else {
                        throw new HyracksDataException("Unsupported function call (" + functionCall + ")");
                    }
                }
            }

            public void createIndex(IPointable result, ArrayBackedValueStorage abvs,
                    ArrayBackedValueStorage abvsFileNode) throws IOException {
                String indexModifiedName = indexCentralizerUtil.putIndexForCollection(collectionModifiedName);
                File collectionDirectory = new File(collectionModifiedName);

                //check if directory is in the local file system
                if (collectionDirectory.exists() && collectionDirectory.isDirectory()) {
                    IndexConstructorUtil indexConstructorUtil = new IndexConstructorUtil();
                    try {
                        indexConstructorUtil.evaluate(collectionModifiedName, indexModifiedName, result, abvs,
                                nodeIdProvider, abvsFileNode, false, nodeId);
                        XDMConstants.setTrue(result);
                        FrameUtils.appendFieldToWriter(writer, appender, result.getByteArray(), result.getStartOffset(),
                                result.getLength());
                    } catch (SystemException e) {
                        throw new HyracksDataException("Could not create index for collection: " + collectionName
                                + " in dir: " + indexModifiedName + " " + e.getMessage(), e);
                    }
                } else {
                    throw new HyracksDataException("Cannot find Collection Directory (" + nodeId + ":"
                            + collectionDirectory.getAbsolutePath() + ")");
                }
            }

            public void updateIndex(IPointable result, ArrayBackedValueStorage abvs,
                    ArrayBackedValueStorage abvsFileNode) throws HyracksDataException {
                String indexModifiedName = indexCentralizerUtil.getIndexForCollection(collectionModifiedName);
                IndexUpdater updater = new IndexUpdater(indexModifiedName, result, abvs, nodeIdProvider, abvsFileNode,
                        nodeId);
                try {
                    updater.setup();
                    updater.updateIndex();
                    updater.updateMetadataFile();
                    updater.exit();
                    XDMConstants.setTrue(result);
                    FrameUtils.appendFieldToWriter(writer, appender, result.getByteArray(), result.getStartOffset(),
                            result.getLength());
                } catch (IOException e) {
                    throw new HyracksDataException(
                            "Could not update index in " + indexModifiedName + " " + e.getMessage(), e);
                }
            }

            public void deleteIndex(IPointable result, ArrayBackedValueStorage abvs,
                    ArrayBackedValueStorage abvsFileNode) throws HyracksDataException {
                String indexModifiedName = indexCentralizerUtil.getIndexForCollection(collectionModifiedName);
                IndexUpdater updater = new IndexUpdater(indexModifiedName, result, abvs, nodeIdProvider, abvsFileNode,
                        nodeId);
                indexCentralizerUtil.deleteEntryForCollection(collectionModifiedName);
                try {
                    updater.setup();
                    updater.deleteAllIndexes();
                    XDMConstants.setTrue(result);
                    FrameUtils.appendFieldToWriter(writer, appender, result.getByteArray(), result.getStartOffset(),
                            result.getLength());
                } catch (IOException e) {
                    throw new HyracksDataException(
                            "Could not delete index in " + indexModifiedName + " " + e.getMessage(), e);
                }
            }

            public void usingIndex(IPointable result) throws HyracksDataException {
                String indexModifiedName = indexCentralizerUtil.getIndexForCollection(collectionModifiedName);
                VXQueryIndexReader indexReader = new VXQueryIndexReader(ctx, indexModifiedName, childSeq, appender);
                try {
                    indexReader.init();
                    for (int tupleIndex = 0; tupleIndex < fta.getTupleCount(); ++tupleIndex) {
                        while (indexReader.step(result, writer, tupleIndex)) {
                        }
                    }
                } catch (AlgebricksException e) {
                    throw new HyracksDataException("Could not read index.", e);
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                writer.fail();
            }

            @Override
            public void close() throws HyracksDataException {
                // Check if needed?
                if (appender.getTupleCount() > 0) {
                    appender.flush(writer);
                }
                writer.close();
                indexCentralizerUtil.writeIndexDirectory();
            }
        };
    }
}
