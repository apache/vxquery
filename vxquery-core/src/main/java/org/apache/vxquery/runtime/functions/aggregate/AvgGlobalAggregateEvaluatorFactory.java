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
package org.apache.vxquery.runtime.functions.aggregate;

import java.io.DataOutput;

import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.arithmetic.AddOperation;
import org.apache.vxquery.runtime.functions.arithmetic.DivideOperation;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentAggregateEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentAggregateEvaluatorFactory;
import org.apache.vxquery.runtime.functions.util.ArithmeticHelper;

public class AvgGlobalAggregateEvaluatorFactory extends AbstractTaggedValueArgumentAggregateEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public AvgGlobalAggregateEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IAggregateEvaluator createEvaluator(IScalarEvaluator[] args) throws HyracksDataException {
        final ArrayBackedValueStorage abvsCount = new ArrayBackedValueStorage();
        final DataOutput dOutCount = abvsCount.getDataOutput();
        final ArrayBackedValueStorage abvsSum = new ArrayBackedValueStorage();
        final DataOutput dOutSum = abvsSum.getDataOutput();
        final AddOperation aOpAdd = new AddOperation();
        final ArithmeticHelper add1 = new ArithmeticHelper(aOpAdd, dCtx);
        final ArithmeticHelper add2 = new ArithmeticHelper(aOpAdd, dCtx);
        final DivideOperation aOpDivide = new DivideOperation();
        final ArithmeticHelper divide = new ArithmeticHelper(aOpDivide, dCtx);
        final LongPointable longp = (LongPointable) LongPointable.FACTORY.createPointable();
        final SequencePointable seq = (SequencePointable) SequencePointable.FACTORY.createPointable();
        final TaggedValuePointable tvpArg = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();

        return new AbstractTaggedValueArgumentAggregateEvaluator(args) {
            TaggedValuePointable tvpSum = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
            TaggedValuePointable tvpCount = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();

            @Override
            public void init() throws HyracksDataException {
                try {
                    abvsCount.reset();
                    dOutCount.write(ValueTag.XS_INTEGER_TAG);
                    dOutCount.writeLong(0);
                    tvpCount.set(abvsCount);
                    abvsSum.reset();
                    dOutSum.write(ValueTag.XS_INTEGER_TAG);
                    dOutSum.writeLong(0);
                    tvpSum.set(abvsSum);
                } catch (Exception e) {
                    throw new HyracksDataException(e);
                }
            }

            @Override
            public void finishPartial(IPointable result) throws HyracksDataException {
                finish(result);
            }

            @Override
            public void finish(IPointable result) throws HyracksDataException {
                tvpCount.getValue(longp);
                if (longp.getLong() == 0) {
                    XDMConstants.setEmptySequence(result);
                } else {
                    // Set count as a TaggedValuePointable.
                    try {
                        divide.compute(tvpSum, tvpCount, tvpSum);
                        result.set(tvpSum);
                    } catch (Exception e) {
                        throw new HyracksDataException(e);
                    }
                }
            }

            @Override
            protected void step(TaggedValuePointable[] args) throws HyracksDataException {
                TaggedValuePointable tvp = args[0];
                if (tvp.getTag() == ValueTag.SEQUENCE_TAG) {
                    tvp.getValue(seq);
                    int seqLen = seq.getEntryCount();
                    if (seqLen == 0) {
                        // No results from nodes.
                        return;
                    } else if (seqLen == 2) {
                        seq.getEntry(0, tvpArg);
                        add1.compute(tvpArg, tvpCount, tvpCount);
                        seq.getEntry(1, tvpArg);
                        add2.compute(tvpArg, tvpSum, tvpSum);
                    } else {
                        throw new SystemException(ErrorCode.SYSE0001);
                    }
                }
            }
        };
    }
}
