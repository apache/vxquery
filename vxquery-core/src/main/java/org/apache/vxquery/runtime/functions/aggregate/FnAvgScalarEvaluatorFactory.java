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

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.arithmetic.AddOperation;
import org.apache.vxquery.runtime.functions.arithmetic.DivideOperation;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.runtime.functions.util.ArithmeticHelper;

public class FnAvgScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnAvgScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws HyracksDataException {
        final DynamicContext dCtx = (DynamicContext) ctx.getJobletContext().getGlobalJobData();
        final SequencePointable seqp = (SequencePointable) SequencePointable.FACTORY.createPointable();
        final TaggedValuePointable tvpNext = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final TaggedValuePointable tvpSum = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final TaggedValuePointable tvpCount = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final DataOutput dOut = abvs.getDataOutput();
        final AddOperation aOpAdd = new AddOperation();
        final ArithmeticHelper add = new ArithmeticHelper(aOpAdd, dCtx);
        final DivideOperation aOpDivide = new DivideOperation();
        final ArithmeticHelper divide = new ArithmeticHelper(aOpDivide, dCtx);

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws HyracksDataException {
                TaggedValuePointable tvp = args[0];
                if (tvp.getTag() == ValueTag.SEQUENCE_TAG) {
                    tvp.getValue(seqp);
                    int seqLen = seqp.getEntryCount();
                    if (seqLen == 0) {
                        XDMConstants.setEmptySequence(result);
                    } else {
                        // Add up the sequence.
                        for (int j = 0; j < seqLen; ++j) {
                            seqp.getEntry(j, tvpNext);
                            if (j == 0) {
                                // Init.
                                tvpSum.set(tvpNext);
                            } else {
                                add.compute(tvpNext, tvpSum, tvpSum);
                            }
                        }

                        // Set count as a TaggedValuePointable.
                        try {
                            abvs.reset();
                            dOut.write(ValueTag.XS_INTEGER_TAG);
                            dOut.writeLong(seqLen);
                            tvpCount.set(abvs);
                        } catch (Exception e) {
                            throw new SystemException(ErrorCode.SYSE0001, e);
                        }

                        divide.compute(tvpSum, tvpCount, tvpSum);
                        result.set(tvpSum);
                    }
                } else {
                    // Only one result.
                    result.set(tvp);
                }
            }
        };
    }
}
