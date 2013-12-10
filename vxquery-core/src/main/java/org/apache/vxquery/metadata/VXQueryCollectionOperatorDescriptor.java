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
import java.util.List;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.step.ChildPathStep;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;
import org.apache.vxquery.xmlparser.ITreeNodeIdProvider;
import org.apache.vxquery.xmlparser.TreeNodeIdProvider;
import org.xml.sax.InputSource;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class VXQueryCollectionOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private short dataSourceId;
    private short totalDataSources;
    private String collectionName;
    private List<Integer> childSeq;

    public VXQueryCollectionOperatorDescriptor(IOperatorDescriptorRegistry spec, VXQueryCollectionDataSource ds,
            RecordDescriptor rDesc) {
        super(spec, 1, 1);
        collectionName = ds.getId();
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
        final ArrayTupleBuilder tb = new ArrayTupleBuilder(recordDescProvider.getOutputRecordDescriptor(
                getActivityId(), 0).getFieldCount());
        final ByteBuffer frame = ctx.allocateFrame();
        final FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
        final InputSource in = new InputSource();
        final ArrayBackedValueStorage abvsFileNode = new ArrayBackedValueStorage();
        final short partitionId = (short) ctx.getTaskAttemptId().getTaskId().getPartition();
        final ITreeNodeIdProvider nodeIdProvider = new TreeNodeIdProvider(partitionId, dataSourceId, totalDataSources);
        final String nodeId = ctx.getJobletContext().getApplicationContext().getNodeId();
        final int frameSize = ctx.getFrameSize();
        final IHyracksTaskContext ctx1 = ctx;

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
                        addXmlFile(collectionDirectory, t);
                    }
                } else {
                    throw new HyracksDataException(
                            "Invalid directory parameter passed to collection (VXQueryCollectionOperatorDescriptor.nextFrame).");
                }
            }

            private void addXmlFile(File collectionDirectory, int t) throws HyracksDataException {
                // Add a field for the document node to each tuple.
                for (File file : collectionDirectory.listFiles()) {
                    // Add the document node to the frame output.
                    if (FunctionHelper.readableXmlFile(file.getPath())) {
                        // TODO Make field addition based on output tuples instead of files.

                        // Now add new field.
                        abvsFileNode.reset();
                        try {
                            FunctionHelper.readInDocFromString(file.getPath(), in, abvsFileNode, nodeIdProvider);
                            // Can not fit XML file into frame.
                            if (frameSize <= (abvsFileNode.getLength() - abvsFileNode.getStartOffset())) {
                                throw new HyracksDataException(
                                        "XML node is to large for the current frame size (VXQueryCollectionOperatorDescriptor.addXmlFile).");
                            }
                        } catch (Exception e) {
                            throw new HyracksDataException(e);
                        }

                        if (childSeq.isEmpty()) {
                            // Return the whole XML file.
                            tb.addField(abvsFileNode.getByteArray(), abvsFileNode.getStartOffset(),
                                    abvsFileNode.getLength());
                        } else {
                            // Process child nodes.
                            TaggedValuePointable tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY
                                    .createPointable();
                            tvp.set(abvsFileNode.getByteArray(), abvsFileNode.getStartOffset(),
                                    abvsFileNode.getLength());
                            processChildStep(tb, tvp, t);
                        }
                    } else if (file.isDirectory()) {
                        // Consider all XML file in sub directories.
                        addXmlFile(file, t);
                    }
                }
            }

            private void processChildStep(final ArrayTupleBuilder tb, TaggedValuePointable tvp, int t)
                    throws HyracksDataException {
                TaggedValuePointable result = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
                ChildPathStep childPathStep = new ChildPathStep(ctx1);
                try {
                    childPathStep.init(tvp, childSeq);
                } catch (SystemException e) {
                    throw new HyracksDataException("Child path step failed to load node tree.");
                }
                try {
                    while (childPathStep.step(result)) {
                        // First copy all new fields over.
                        tb.reset();
                        if (fta.getFieldCount() > 0) {
                            for (int f = 0; f < fta.getFieldCount(); ++f) {
                                tb.addField(fta, t, f);
                            }
                        }
                        tb.addField(result.getByteArray(), result.getStartOffset(), result.getLength());
                        // Send to the writer.
                        if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                            FrameUtils.flushFrame(frame, writer);
                            appender.reset(frame, true);
                            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                                throw new HyracksDataException(
                                        "Could not write frame (VXQueryCollectionOperatorDescriptor.createPushRuntime).");
                            }
                        }
                    }
                } catch (AlgebricksException e) {
                    throw new HyracksDataException(e);
                }
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