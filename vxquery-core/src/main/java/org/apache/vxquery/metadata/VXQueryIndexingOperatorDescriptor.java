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

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.client.NodeControllerInfo;
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
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.FrameFixedFieldTupleAppender;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.vxquery.common.VXQueryCommons;
import org.apache.vxquery.compiler.rewriter.rules.AbstractCollectionRule;
import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.functions.BuiltinFunctions;
import org.apache.vxquery.runtime.functions.index.IndexConstructorUtil;
import org.apache.vxquery.runtime.functions.index.VXQueryIndexReader;
import org.apache.vxquery.runtime.functions.index.updateIndex.IndexUpdater;
import org.apache.vxquery.xmlparser.ITreeNodeIdProvider;
import org.apache.vxquery.xmlparser.TreeNodeIdProvider;
import org.apache.vxquery.xmlparser.XMLParser;

import javax.xml.bind.JAXBException;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class VXQueryIndexingOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    protected static final Logger LOGGER = Logger.getLogger(VXQueryCollectionOperatorDescriptor.class.getName());
    private static final long serialVersionUID = 1L;
    private final String START_TAG = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n";
    private final String hdfsConf;
    private final Map<String, NodeControllerInfo> nodeControllerInfos;
    private short dataSourceId;
    private short totalDataSources;
    private String[] collectionPartitions;
    private List<Integer> childSeq;

    public VXQueryIndexingOperatorDescriptor(IOperatorDescriptorRegistry spec, VXQueryIndexingDataSource ds,
            RecordDescriptor rDesc, String hdfsConf, Map<String, NodeControllerInfo> nodeControllerInfos) {
        super(spec, 1, 1);
        collectionPartitions = ds.getCollectionPartitions();
        dataSourceId = (short) ds.getDataSourceId();
        totalDataSources = (short) ds.getTotalDataSources();
        recordDescriptors[0] = rDesc;
        this.childSeq = ds.getChildSeq();
        this.hdfsConf = hdfsConf;
        this.nodeControllerInfos = nodeControllerInfos;
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
        final DynamicContext dCtx = (DynamicContext) ctx.getJobletContext().getGlobalJobData();
        final String collectionName = collectionPartitions != null?collectionPartitions[partition %
                collectionPartitions.length]:"";
        String collectionModifiedName = collectionName.replace("${nodeId}", nodeId);

        final XMLParser parser = new XMLParser(false, nodeIdProvider, nodeId, appender, childSeq,
                dCtx.getStaticContext());

        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
            @Override
            public void open() throws HyracksDataException {
                appender.reset(frame, true);
                writer.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                fta.reset(buffer);

                IPointable result = new TaggedValuePointable();

                final UTF8StringPointable stringp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
                final TaggedValuePointable nodep = (TaggedValuePointable) TaggedValuePointable.FACTORY
                        .createPointable();

                final ByteBufferInputStream bbis = new ByteBufferInputStream();
                final DataInputStream di = new DataInputStream(bbis);
                final SequenceBuilder sb = new SequenceBuilder();
                final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
                final ArrayBackedValueStorage abvsFileNode = new ArrayBackedValueStorage();

                String indexModifiedName;

                if (collectionModifiedName.contains("hdfs://")) {
                    throw new HyracksDataException("Indexing support for HDFS not yet implemented.");
                } else {

                    if (AbstractCollectionRule.functionCall.getFunctionIdentifier().equals(BuiltinFunctions
                            .FN_BUILD_INDEX_ON_COLLECTION_1.getFunctionIdentifier())) {
                        indexModifiedName = VXQueryCommons.INDEX_CENTRALIZER_UTIL.putIndexForCollection
                                (collectionModifiedName);
                        File collectionDirectory = new File(collectionModifiedName);

                        //check if directory is in the local file system
                        if (collectionDirectory.exists() && collectionDirectory.isDirectory()) {
                            IndexConstructorUtil indexConstructorUtil = new IndexConstructorUtil();
                            try {
                                indexConstructorUtil
                                        .evaluate(collectionModifiedName, indexModifiedName, result, stringp, bbis, di,
                                                sb, abvs, nodeIdProvider, abvsFileNode, nodep, false, nodeId);
                                XDMConstants.setTrue(result);
                                FrameUtils.appendFieldToWriter(writer, appender, result.getByteArray(), result.getStartOffset(),
                                        result.getLength());
                            } catch (SystemException | JAXBException e) {
                                throw new HyracksDataException("Could not create index for collection: " +
                                        collectionName + " in dir: " + null + " " + e.getMessage());
                            }
                        } else {
                            throw new HyracksDataException("Cannot find Collection Directory (" + nodeId + ":" +
                                    collectionDirectory.getAbsolutePath() + ")");
                        }
                    } else if (AbstractCollectionRule.functionCall.getFunctionIdentifier().equals(BuiltinFunctions.FN_UPDATE_INDEX_1.getFunctionIdentifier())) {
                        indexModifiedName = VXQueryCommons.INDEX_CENTRALIZER_UTIL.getIndexForCollection
                                (collectionModifiedName);
                        IndexUpdater updater = new IndexUpdater(indexModifiedName, result, stringp, bbis, di, sb, abvs,
                                nodeIdProvider, abvsFileNode, nodep, nodeId);
                        try {
                            updater.setup();
                            updater.updateIndex();
                            updater.updateMetadataFile();
                            updater.exit();
//                            XDMConstants.setTrue(result);
                        } catch (NoSuchAlgorithmException | IOException | JAXBException | SystemException e) {
                            throw new HyracksDataException("Could not update index in " + indexModifiedName + " " + e.getMessage());
                        }
                    } else if (AbstractCollectionRule.functionCall.getFunctionIdentifier().equals(BuiltinFunctions.FN_DELETE_INDEX_1.getFunctionIdentifier())) {
                        indexModifiedName = VXQueryCommons.INDEX_CENTRALIZER_UTIL.getIndexForCollection
                                (collectionModifiedName);
                        IndexUpdater updater = new IndexUpdater(indexModifiedName, result, stringp, bbis, di, sb, abvs,
                                nodeIdProvider, abvsFileNode, nodep, nodeId);
                        VXQueryCommons.INDEX_CENTRALIZER_UTIL.deleteEntryForCollection(collectionModifiedName);
                        try {
                            updater.setup();
                            updater.deleteAllIndexes();
                        } catch (SystemException | JAXBException | IOException | NoSuchAlgorithmException e) {
                            throw new HyracksDataException("Could not delete index in " + indexModifiedName + " " + e.getMessage());
                        }

                    } else if (AbstractCollectionRule.functionCall.getFunctionIdentifier().equals(BuiltinFunctions
                            .FN_COLLECTION_FROM_INDEX_2.getFunctionIdentifier())) {
                        indexModifiedName = VXQueryCommons.INDEX_CENTRALIZER_UTIL.getIndexForCollection
                                (collectionModifiedName);
                        // In this scenario, collectionModifiedName represents the index directory, and
                        // indexModifiedName represents the path.
                        VXQueryIndexReader indexReader = new VXQueryIndexReader(ctx, indexModifiedName,
                                collectionModifiedName);
                        try {
                            indexReader.init();
                            indexReader.step(result);
                        } catch (AlgebricksException e) {
                            e.printStackTrace();
                        }

                    } else {
                        throw new HyracksDataException(
                                "Unsupported function call (" + AbstractCollectionRule.functionCall + ")");
                    }
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
                    appender.flush(writer, true);
                }
                writer.close();
                try {
                    VXQueryCommons.INDEX_CENTRALIZER_UTIL.writeIndexDirectory();
                } catch (JAXBException | FileNotFoundException e) {
                    e.printStackTrace();
                }
            }
        };
    }
}
