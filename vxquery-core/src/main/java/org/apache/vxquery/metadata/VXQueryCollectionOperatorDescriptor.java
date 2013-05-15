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

import org.apache.vxquery.runtime.functions.util.FunctionHelper;
import org.apache.vxquery.xmlparser.ITreeNodeIdProvider;
import org.apache.vxquery.xmlparser.TreeNodeIdProvider;
import org.xml.sax.InputSource;

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

    public VXQueryCollectionOperatorDescriptor(IOperatorDescriptorRegistry spec, String collectionName,
            int dataSourceId, int totalDataSources, RecordDescriptor rDesc) {
        super(spec, 1, 1);
        this.collectionName = collectionName;
        this.dataSourceId = (short) dataSourceId;
        this.totalDataSources = (short) totalDataSources;
        recordDescriptors[0] = rDesc;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
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

        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
            @Override
            public void open() throws HyracksDataException {
                appender.reset(frame, true);
                writer.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                fta.reset(buffer);
                File collectionDirectory = new File(collectionName);
                File[] list = collectionDirectory.listFiles();

                // Go through each tuple.
                for (int t = 0; t < fta.getTupleCount(); ++t) {
                    // Add a field for the document node to each tuple.
                    for (int i = 0; i < list.length; ++i) {
                        // Add the document node to the frame output.
                        if (list[i].getPath().endsWith(".xml")) {
                            // First copy all new fields over.
                            tb.reset();
                            if (fta.getFieldCount() > 0) {
                                for (int f = 0; f < fta.getFieldCount(); ++f) {
                                    tb.addField(fta, t, f);
                                }
                            }

                            // Now add new field.
                            abvsFileNode.reset();
                            try {
                                FunctionHelper.readInDocFromString(list[i].getPath(), in, abvsFileNode, nodeIdProvider);
                            } catch (Exception e) {
                                throw new HyracksDataException(e);
                            }
                            tb.addField(abvsFileNode.getByteArray(), abvsFileNode.getStartOffset(),
                                    abvsFileNode.getLength());

                            // Send to the writer.
                            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                                FrameUtils.flushFrame(frame, writer);
                                appender.reset(frame, true);
                                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                                    throw new IllegalStateException(
                                            "Could not write frame (VXQueryCollectionOperatorDescriptor.createPushRuntime).");
                                }
                            }
                        }
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
                fta.reset(frame);
                if (fta.getTupleCount() > 0) {
                    FrameUtils.flushFrame(frame, writer);
                }
                writer.close();
            }
        };
    }
}