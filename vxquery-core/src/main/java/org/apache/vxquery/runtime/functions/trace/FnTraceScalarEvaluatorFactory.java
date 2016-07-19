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
package org.apache.vxquery.runtime.functions.trace;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.BytePointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.accessors.jsonitem.ArrayPointable;
import org.apache.vxquery.datamodel.accessors.jsonitem.ObjectPointable;
import org.apache.vxquery.datamodel.builders.atomic.StringValueBuilder;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;

public class FnTraceScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    public FnTraceScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final SequencePointable seqp = (SequencePointable) SequencePointable.FACTORY.createPointable();
        final LongPointable lp = (LongPointable) LongPointable.FACTORY.createPointable();
        final IntegerPointable ip = (IntegerPointable) IntegerPointable.FACTORY.createPointable();
        final ShortPointable sp = (ShortPointable) ShortPointable.FACTORY.createPointable();
        final BytePointable bp = (BytePointable) BytePointable.FACTORY.createPointable();
        final BooleanPointable blp = (BooleanPointable) BooleanPointable.FACTORY.createPointable();
        final XSDecimalPointable decp = (XSDecimalPointable) XSDecimalPointable.FACTORY.createPointable();
        final DoublePointable dp = (DoublePointable) DoublePointable.FACTORY.createPointable();
        final FloatPointable fp = (FloatPointable) FloatPointable.FACTORY.createPointable();
        final UTF8StringPointable utf8p = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        final ArrayPointable ap = (ArrayPointable) ArrayPointable.FACTORY.createPointable();
        final ObjectPointable op = (ObjectPointable) ObjectPointable.FACTORY.createPointable();
        final StringValueBuilder svb = new StringValueBuilder();
        final SequenceBuilder sb = new SequenceBuilder();
        final UTF8StringPointable string = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        return new AbstractTaggedValueArgumentScalarEvaluator(args) {

            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
                TaggedValuePointable tvp = args[0];
                TaggedValuePointable tvp1 = args[1];
                abvs.reset();
                sb.reset(abvs);
                if (tvp1.getTag() != ValueTag.XS_STRING_TAG) {
                    throw new SystemException(ErrorCode.FORG0006);
                }
                tvp1.getValue(utf8p);
                try {
                    sb.addItem(tvp1);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                switch (tvp.getTag()) {
                    case ValueTag.SEQUENCE_TAG: {
                        tvp.getValue(seqp);
                        string.set(seqp.getByteArray(), seqp.getStartOffset(), seqp.getLength());
                        try {
                            sb.addItem(string);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        break;
                    }

                    case ValueTag.XS_BOOLEAN_TAG: {
                        tvp.getValue(blp);
                        string.set(blp.getByteArray(), blp.getStartOffset(), blp.getLength());
                        try {
                            sb.addItem(string);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        break;
                    }

                    case ValueTag.XS_DECIMAL_TAG: {
                        tvp.getValue(decp);
                        string.set(decp.getByteArray(), decp.getStartOffset(), decp.getLength());
                        try {
                            sb.addItem(string);
                        } catch (IOException e) {
                            e.printStackTrace();
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
                        string.set(lp.getByteArray(), lp.getStartOffset(), lp.getLength());
                        try {
                            sb.addItem(tvp);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        break;
                    }

                    case ValueTag.XS_INT_TAG:
                    case ValueTag.XS_UNSIGNED_SHORT_TAG: {
                        tvp.getValue(ip);
                        string.set(ip.getByteArray(), ip.getStartOffset(), ip.getLength());
                        try {
                            sb.addItem(string);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        break;
                    }

                    case ValueTag.XS_SHORT_TAG:
                    case ValueTag.XS_UNSIGNED_BYTE_TAG: {
                        tvp.getValue(sp);
                        string.set(sp.getByteArray(), sp.getStartOffset(), sp.getLength());
                        try {
                            sb.addItem(string);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        break;
                    }

                    case ValueTag.XS_BYTE_TAG: {
                        tvp.getValue(bp);
                        string.set(bp.getByteArray(), bp.getStartOffset(), bp.getLength());
                        try {
                            sb.addItem(string);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        break;
                    }

                    case ValueTag.XS_DOUBLE_TAG: {
                        tvp.getValue(dp);
                        string.set(dp.getByteArray(), dp.getStartOffset(), dp.getLength());
                        try {
                            sb.addItem(string);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        break;
                    }

                    case ValueTag.XS_FLOAT_TAG: {
                        tvp.getValue(fp);
                        string.set(fp.getByteArray(), fp.getStartOffset(), fp.getLength());
                        try {
                            sb.addItem(string);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        break;
                    }

                    case ValueTag.XS_ANY_URI_TAG:
                    case ValueTag.XS_STRING_TAG: {
                        tvp.getValue(utf8p);
                        string.set(utf8p.getByteArray(), utf8p.getStartOffset(), utf8p.getLength());
                        try {
                            sb.addItem(string);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        break;
                    }
                    case ValueTag.JS_NULL_TAG: {
                        break;
                    }
                    case ValueTag.ARRAY_TAG:
                        tvp.getValue(ap);
                        string.set(ap.getByteArray(), ap.getStartOffset(), ap.getLength());
                        try {
                            sb.addItem(string);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        break;
                    case ValueTag.OBJECT_TAG: {
                        tvp.getValue(op);
                        string.set(op.getByteArray(), op.getStartOffset(), op.getLength());
                        try {
                            sb.addItem(string);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        break;
                    }
                    default:
                        throw new SystemException(ErrorCode.FORG0006);
                }
                try {
                    sb.finish();
                    result.set(abvs);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        };
    }
}
