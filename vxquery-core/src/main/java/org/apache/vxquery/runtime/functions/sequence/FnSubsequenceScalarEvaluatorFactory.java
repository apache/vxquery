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

import java.io.IOException;

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.runtime.functions.numeric.FnRoundOperation;

public class FnSubsequenceScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnSubsequenceScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws HyracksDataException {
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final SequenceBuilder sb = new SequenceBuilder();
        final SequencePointable seq = (SequencePointable) SequencePointable.FACTORY.createPointable();
        final VoidPointable p = (VoidPointable) VoidPointable.FACTORY.createPointable();
        final DoublePointable doublep = (DoublePointable) DoublePointable.FACTORY.createPointable();
        final LongPointable longp = (LongPointable) LongPointable.FACTORY.createPointable();
        final XSDecimalPointable decp = (XSDecimalPointable) XSDecimalPointable.FACTORY.createPointable();
        final ArrayBackedValueStorage abvsRound = new ArrayBackedValueStorage();
        final FnRoundOperation round = new FnRoundOperation();
        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                try {
                    long startingLoc;
                    TaggedValuePointable tvp2 = args[1];
                    startingLoc = getLongFromArgument(tvp2);
                    if (startingLoc < 1) {
                        startingLoc = 1;
                    }

                    // Get length.
                    long endingLoc = Long.MAX_VALUE;
                    if (args.length > 2) {
                        TaggedValuePointable tvp3 = args[2];
                        endingLoc = getLongFromArgument(tvp3) + startingLoc;
                    }

                    abvs.reset();
                    sb.reset(abvs);
                    TaggedValuePointable tvp1 = args[0];
                    if (tvp1.getTag() == ValueTag.SEQUENCE_TAG) {
                        tvp1.getValue(seq);
                        int seqLen = seq.getEntryCount();
                        if (endingLoc < startingLoc) {
                            // Empty sequence.
                        } else if (startingLoc == 1 && endingLoc > seqLen) {
                            // Includes whole sequence.
                            result.set(tvp1);
                            return;
                        } else {
                            for (int j = 0; j < seqLen; ++j) {
                                if (startingLoc <= j + 1 && j + 1 < endingLoc) {
                                    seq.getEntry(j, p);
                                    sb.addItem(p);
                                }
                            }
                        }
                    } else if (startingLoc == 1 && endingLoc > 1) {
                        // Includes item.
                        result.set(tvp1);
                        return;
                    }
                    sb.finish();
                    result.set(abvs);
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001);
                }
            }

            /**
             * XQuery Specification calls for double value. Integer and Decimal are allowed to cut down on casting.
             *
             * @param tvp
             * @return long
             * @throws SystemException
             * @throws IOException
             */
            public long getLongFromArgument(TaggedValuePointable tvp) throws SystemException, IOException {
                if (tvp.getTag() == ValueTag.XS_DOUBLE_TAG) {
                    tvp.getValue(doublep);
                    abvsRound.reset();
                    round.operateDouble(doublep, abvsRound.getDataOutput());
                    doublep.set(abvsRound.getByteArray(), abvsRound.getStartOffset() + 1,
                            DoublePointable.TYPE_TRAITS.getFixedLength());
                    return doublep.longValue();
                } else if (tvp.getTag() == ValueTag.XS_INTEGER_TAG) {
                    tvp.getValue(longp);
                    return longp.longValue();
                } else if (tvp.getTag() == ValueTag.XS_DECIMAL_TAG) {
                    tvp.getValue(decp);
                    return decp.longValue();
                } else {
                    throw new SystemException(ErrorCode.FORG0006);
                }
            }
        };
    }
}
