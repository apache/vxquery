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
package org.apache.vxquery.runtime.functions.sequence;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentAggregateEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentAggregateEvaluatorFactory;
import org.apache.vxquery.util.GrowableIntArray;

public class SequenceAggregateEvaluatorFactory extends AbstractTaggedValueArgumentAggregateEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public SequenceAggregateEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IAggregateEvaluator createEvaluator(IScalarEvaluator[] args) throws HyracksDataException {
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final GrowableIntArray slots = new GrowableIntArray();
        final ArrayBackedValueStorage dataArea = new ArrayBackedValueStorage();
        final SequencePointable seq = (SequencePointable) SequencePointable.FACTORY.createPointable();
        final VoidPointable p = (VoidPointable) VoidPointable.FACTORY.createPointable();
        return new AbstractTaggedValueArgumentAggregateEvaluator(args) {
            @Override
            public void init() throws HyracksDataException {
                abvs.reset();
                slots.clear();
                dataArea.reset();
            }

            @Override
            public void finishPartial(IPointable result) throws HyracksDataException {
                finish(result);
            }

            @Override
            public void finish(IPointable result) throws HyracksDataException {
                if (slots.getSize() != 1) {
                    assembleResult(abvs, slots, dataArea);
                    result.set(abvs);
                } else {
                    result.set(dataArea);
                }
            }

            @Override
            protected void step(TaggedValuePointable[] args) throws SystemException {
                TaggedValuePointable tvp = args[0];
                if (tvp.getTag() == ValueTag.SEQUENCE_TAG) {
                    tvp.getValue(seq);
                    int seqLen = seq.getEntryCount();
                    for (int j = 0; j < seqLen; ++j) {
                        seq.getEntry(j, p);
                        addItem(p);
                    }
                } else {
                    addItem(tvp);
                }
            }

            private void assembleResult(ArrayBackedValueStorage abvs, GrowableIntArray slots,
                    ArrayBackedValueStorage dataArea) throws SystemException {
                try {
                    DataOutput out = abvs.getDataOutput();
                    out.write(ValueTag.SEQUENCE_TAG);
                    int size = slots.getSize();
                    out.writeInt(size);
                    if (size > 0) {
                        int[] slotArray = slots.getArray();
                        for (int i = 0; i < size; ++i) {
                            out.writeInt(slotArray[i]);
                        }
                        out.write(dataArea.getByteArray(), dataArea.getStartOffset(), dataArea.getLength());
                    }
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
            }

            private void addItem(final IPointable p) throws SystemException {
                try {
                    dataArea.getDataOutput().write(p.getByteArray(), p.getStartOffset(), p.getLength());
                    slots.append(dataArea.getLength());
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
            }
        };
    }
}
