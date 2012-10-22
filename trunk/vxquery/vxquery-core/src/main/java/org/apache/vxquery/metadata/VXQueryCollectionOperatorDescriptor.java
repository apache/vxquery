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
import org.xml.sax.InputSource;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

public class VXQueryCollectionOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private String collectionName;

    public VXQueryCollectionOperatorDescriptor(IOperatorDescriptorRegistry spec, String collectionName, RecordDescriptor rDesc) {
        super(spec, 0, 1);
        this.collectionName = collectionName;
        recordDescriptors[0] = rDesc;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        final ArrayTupleBuilder tb = new ArrayTupleBuilder(1);
        final ByteBuffer frame = ctx.allocateFrame();
        final FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
        final InputSource in = new InputSource();
        final ArrayBackedValueStorage abvsFileNode = new ArrayBackedValueStorage();

        return new AbstractUnaryOutputSourceOperatorNodePushable() {
            @Override
            public void initialize() throws HyracksDataException {
                appender.reset(frame, true);
                writer.open();
                try {
                    File collectionDirectory = new File(collectionName);
                    File[] list = collectionDirectory.listFiles();

                    for (int i = 0; i < list.length; ++i) {
                        // Add the document node to the frame output.
                        if (list[i].getPath().endsWith(".xml")) {
                            abvsFileNode.reset();
                            FunctionHelper.readInDocFromString(list[i].getPath(), in, abvsFileNode);

                            tb.reset();
                            tb.addField(abvsFileNode.getByteArray(), abvsFileNode.getStartOffset(), abvsFileNode.getLength());

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
                   
                } catch (Exception e) {
                    writer.fail();
                    throw new HyracksDataException(e);
                } finally {
                    writer.close();
                }
            }
        };
    }
}