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
package org.apache.vxquery.runtime.functions.bool;

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.BytePointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;

public class FnBooleanScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnBooleanScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws HyracksDataException {
        return new FnBooleanScalarEvaluator(args);
    }

    private static class FnBooleanScalarEvaluator extends AbstractTaggedValueArgumentScalarEvaluator {
        final SequencePointable seqp = (SequencePointable) SequencePointable.FACTORY.createPointable();
        final LongPointable lp = (LongPointable) LongPointable.FACTORY.createPointable();
        final IntegerPointable ip = (IntegerPointable) IntegerPointable.FACTORY.createPointable();
        final ShortPointable sp = (ShortPointable) ShortPointable.FACTORY.createPointable();
        final BytePointable bp = (BytePointable) BytePointable.FACTORY.createPointable();
        final XSDecimalPointable decp = (XSDecimalPointable) XSDecimalPointable.FACTORY.createPointable();
        final DoublePointable dp = (DoublePointable) DoublePointable.FACTORY.createPointable();
        final FloatPointable fp = (FloatPointable) FloatPointable.FACTORY.createPointable();
        final UTF8StringPointable utf8p = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();

        public FnBooleanScalarEvaluator(IScalarEvaluator[] args) {
            super(args);
        }

        @Override
        protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
            TaggedValuePointable tvp = args[0];
            boolean booleanResult = true;
            switch (tvp.getTag()) {
                case ValueTag.SEQUENCE_TAG: {
                    tvp.getValue(seqp);
                    if (seqp.getEntryCount() == 0) {
                        booleanResult = false;
                    }
                    break;
                }

                case ValueTag.XS_BOOLEAN_TAG: {
                    result.set(tvp);
                    return;
                }

                case ValueTag.XS_DECIMAL_TAG: {
                    tvp.getValue(decp);
                    if (decp.longValue() == 0) {
                        booleanResult = false;
                    }
                    break;
                }

                case ValueTag.XS_INTEGER_TAG:
                case ValueTag.XS_LONG_TAG:
                case ValueTag.XS_NEGATIVE_INTEGER_TAG:
                case ValueTag.XS_NON_POSITIVE_INTEGER_TAG:
                case ValueTag.XS_NON_NEGATIVE_INTEGER_TAG:
                case ValueTag.XS_POSITIVE_INTEGER_TAG:
                case ValueTag.XS_UNSIGNED_INT_TAG:
                case ValueTag.XS_UNSIGNED_LONG_TAG: {
                    tvp.getValue(lp);
                    if (lp.longValue() == 0) {
                        booleanResult = false;
                    }
                    break;
                }

                case ValueTag.XS_INT_TAG:
                case ValueTag.XS_UNSIGNED_SHORT_TAG: {
                    tvp.getValue(ip);
                    if (ip.intValue() == 0) {
                        booleanResult = false;
                    }
                    break;
                }

                case ValueTag.XS_SHORT_TAG:
                case ValueTag.XS_UNSIGNED_BYTE_TAG: {
                    tvp.getValue(sp);
                    if (sp.shortValue() == 0) {
                        booleanResult = false;
                    }
                    break;
                }

                case ValueTag.XS_BYTE_TAG: {
                    tvp.getValue(bp);
                    if (bp.byteValue() == 0) {
                        booleanResult = false;
                    }
                    break;
                }

                case ValueTag.XS_DOUBLE_TAG: {
                    tvp.getValue(dp);
                    if (dp.doubleValue() == 0 || Double.isNaN(dp.doubleValue())) {
                        booleanResult = false;
                    }
                    break;
                }

                case ValueTag.XS_FLOAT_TAG: {
                    tvp.getValue(fp);
                    if (fp.floatValue() == 0 || Float.isNaN(fp.floatValue())) {
                        booleanResult = false;
                    }
                    break;
                }

                case ValueTag.XS_ANY_URI_TAG:
                case ValueTag.XS_STRING_TAG: {
                    tvp.getValue(utf8p);
                    if (utf8p.getStringLength() == 0) {
                        booleanResult = false;
                    }
                    break;
                }
                case ValueTag.JS_NULL_TAG: {
                    booleanResult = false;
                    break;
                }
                case ValueTag.ARRAY_TAG:
                case ValueTag.OBJECT_TAG: {
                    booleanResult = true;
                    break;
                }
                default:
                    throw new SystemException(ErrorCode.FORG0006);
            }
            if (booleanResult) {
                XDMConstants.setTrue(result);
            } else {
                XDMConstants.setFalse(result);
            }
        }
    }
}
