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
package org.apache.vxquery.runtime.functions.datetime;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDurationPointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;

public abstract class AbstractValueFromDurationScalarEvaluatorFactory extends
        AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public AbstractValueFromDurationScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws HyracksDataException {
        final XSDurationPointable durationp = (XSDurationPointable) XSDurationPointable.FACTORY.createPointable();
        final LongPointable longp = (LongPointable) LongPointable.FACTORY.createPointable();
        final IntegerPointable intp = (IntegerPointable) IntegerPointable.FACTORY.createPointable();
        final SequencePointable seqp = (SequencePointable) SequencePointable.FACTORY.createPointable();
        final ArrayBackedValueStorage abvsInner = new ArrayBackedValueStorage();
        final DataOutput dOutInner = abvsInner.getDataOutput();

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                TaggedValuePointable tvp1 = args[0];
                long value;
                long YMDuration = 0, DTDuration = 0;

                switch (tvp1.getTag()) {
                    case ValueTag.SEQUENCE_TAG:
                        tvp1.getValue(seqp);
                        if (seqp.getEntryCount() == 0) {
                            XDMConstants.setEmptySequence(result);
                            return;
                        }
                        break;
                    case ValueTag.XS_DURATION_TAG:
                        tvp1.getValue(durationp);
                        YMDuration = durationp.getYearMonth();
                        DTDuration = durationp.getDayTime();
                        break;
                    case ValueTag.XS_DAY_TIME_DURATION_TAG:
                        tvp1.getValue(longp);
                        DTDuration = longp.getLong();
                        break;
                    case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                        tvp1.getValue(intp);
                        YMDuration = intp.getInteger();
                        break;
                    default:
                        throw new SystemException(ErrorCode.FORG0006);
                }

                value = convertDuration(YMDuration, DTDuration);

                try {
                    abvsInner.reset();
                    switch (getReturnTag()) {
                        case ValueTag.XS_INTEGER_TAG:
                            dOutInner.write(ValueTag.XS_INTEGER_TAG);
                            dOutInner.writeLong(value);
                            break;
                        case ValueTag.XS_DECIMAL_TAG:
                            long decimalPlace = 3;

                            // Normalize to decimal.
                            if (value % 1000 == 0) {
                                value = value / 1000;
                                decimalPlace = 0;
                            } else if (value % 100 == 0) {
                                value = value / 100;
                                decimalPlace = 1;
                            } else if (value % 10 == 0) {
                                value = value / 10;
                                decimalPlace = 2;
                            }

                            dOutInner.write(ValueTag.XS_DECIMAL_TAG);
                            dOutInner.write((byte) decimalPlace);
                            dOutInner.writeLong(value);
                            break;
                    }

                    result.set(abvsInner);
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
            }
        };
    }

    protected int getReturnTag() {
        return ValueTag.XS_INTEGER_TAG;
    }

    protected abstract long convertDuration(long YMDuration, long DTDuration);
}
