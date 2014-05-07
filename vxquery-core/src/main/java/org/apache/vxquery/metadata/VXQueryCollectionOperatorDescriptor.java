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
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.vxquery.datamodel.accessors.PointablePool;
import org.apache.vxquery.datamodel.accessors.PointablePoolFactory;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.step.ChildPathStepOperatorDescriptor;
import org.apache.vxquery.xmlparser.ITreeNodeIdProvider;
import org.apache.vxquery.xmlparser.TreeNodeIdProvider;
import org.apache.vxquery.xmlparser.XMLParser;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class VXQueryCollectionOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private short dataSourceId;
    private short totalDataSources;
    private String[] collectionPartitions;
    private List<Integer> childSeq;

    public VXQueryCollectionOperatorDescriptor(IOperatorDescriptorRegistry spec, VXQueryCollectionDataSource ds,
            RecordDescriptor rDesc) {
        super(spec, 1, 1);
        collectionPartitions = ds.getPartitions();
        dataSourceId = (short) ds.getDataSourceId();
        totalDataSources = (short) ds.getTotalDataSources();
        childSeq = ds.getChildSeq();
        recordDescriptors[0] = rDesc;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        final FrameTupleAccessor fta = new FrameTupleAccessor(ctx.getFrameSize(),
                recordDescProvider.getInputRecordDescriptor(getActivityId(), 0));
        final int fieldOutputCount = recordDescProvider.getOutputRecordDescriptor(getActivityId(), 0).getFieldCount();
        final ByteBuffer frame = ctx.allocateFrame();
        final FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize(), fieldOutputCount);
        final ArrayBackedValueStorage abvsFileNode = new ArrayBackedValueStorage();
        final short partitionId = (short) ctx.getTaskAttemptId().getTaskId().getPartition();
        final ITreeNodeIdProvider nodeIdProvider = new TreeNodeIdProvider(partitionId, dataSourceId, totalDataSources);
        final String nodeId = ctx.getJobletContext().getApplicationContext().getNodeId();
        final int frameSize = ctx.getFrameSize();
        final PointablePool ppool = PointablePoolFactory.INSTANCE.createPointablePool();
        final ChildPathStepOperatorDescriptor childPathStep = new ChildPathStepOperatorDescriptor(ctx, ppool);

        final String collectionName = collectionPartitions[partition % collectionPartitions.length];
        final XMLParser parser = new XMLParser(false, nodeIdProvider);;

        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
            @Override
            public void open() throws HyracksDataException {
                appender.reset(frame, true);
                writer.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                fta.reset(buffer);
                String collectionModifiedName = collectionName.replace("${nodeId}", nodeId);
                File collectionDirectory = new File(collectionModifiedName);

                // Go through each tuple.
                if (collectionDirectory.isDirectory()) {
                    for (int t = 0; t < fta.getTupleCount(); ++t) {
                        @SuppressWarnings("unchecked")
                        Iterator<File> it = FileUtils.iterateFiles(collectionDirectory, new VXQueryIOFileFilter(),
                                TrueFileFilter.INSTANCE);
                        while (it.hasNext()) {
                            addNextXmlNode(it.next(), t);
                        }
                    }
                } else {
                    throw new HyracksDataException(
                            "Invalid directory parameter passed to collection (VXQueryCollectionOperatorDescriptor.nextFrame).");
                }
            }

            /**
             * Add the document node to the frame output.
             */
            private void addNextXmlNode(File file, int t) throws HyracksDataException {
                // Now add new field.
                abvsFileNode.reset();
                try {
                    parser.parseFile(file, abvsFileNode);
                } catch (HyracksDataException e) {
                    e.setNodeId(nodeId);
                    throw e;
                }

                TaggedValuePointable tvp = ppool.takeOne(TaggedValuePointable.class);
                if (childSeq.isEmpty()) {
                    // Can not fit XML file into frame.
                    if (frameSize <= (abvsFileNode.getLength() - abvsFileNode.getStartOffset())) {
                        throw new HyracksDataException(
                                "XML node is to large for the current frame size (VXQueryCollectionOperatorDescriptor.addXmlFile).");
                    }
                    tvp.set(abvsFileNode.getByteArray(), abvsFileNode.getStartOffset(), abvsFileNode.getLength());
                    addNodeToTuple(tvp, t);
                } else {
                    // Process child nodes.
                    tvp.set(abvsFileNode.getByteArray(), abvsFileNode.getStartOffset(), abvsFileNode.getLength());
                    processChildStep(tvp, t);
                }
                ppool.giveBack(tvp);
            }

            private void processChildStep(TaggedValuePointable tvp, int t) throws HyracksDataException {
                try {
                    childPathStep.init(tvp, childSeq);
                } catch (SystemException e) {
                    throw new HyracksDataException("Child path step failed to load node tree.");
                }
                try {
                    TaggedValuePointable result = ppool.takeOne(TaggedValuePointable.class);
                    while (childPathStep.step(result)) {
                        addNodeToTuple(result, t);
                    }
                    ppool.giveBack(result);
                } catch (AlgebricksException e) {
                    throw new HyracksDataException(e);
                }
            }

            private void addNodeToTuple(TaggedValuePointable result, int t) throws HyracksDataException {
                // Send to the writer.
                if (!addNodeToTupleAppender(result, t)) {
                    FrameUtils.flushFrame(frame, writer);
                    appender.reset(frame, true);
                    if (!addNodeToTupleAppender(result, t)) {
                        throw new HyracksDataException(
                                "Could not write frame (VXQueryCollectionOperatorDescriptor.createPushRuntime).");
                    }
                }
            }

            private boolean addNodeToTupleAppender(TaggedValuePointable result, int t) throws HyracksDataException {
                // First copy all new fields over.
                if (fta.getFieldCount() > 0) {
                    for (int f = 0; f < fta.getFieldCount(); ++f) {
                        if (!appender.appendField(fta, t, f)) {
                            return false;
                        }
                    }
                }
                return appender.appendField(result.getByteArray(), result.getStartOffset(), result.getLength());
            }

            @Override
            public void fail() throws HyracksDataException {
                writer.fail();
            }

            @Override
            public void close() throws HyracksDataException {
                // Check if needed?
                fta.reset(frame);
                if (fta.getTupleCount() > 0) {
                    FrameUtils.flushFrame(frame, writer);
                }
                writer.close();
            }
        };
    }
}