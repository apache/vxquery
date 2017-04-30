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
package org.apache.vxquery.runtime.functions.type;

import java.io.DataOutput;

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.TypedPointables;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.cast.AbstractCastToOperation;
import org.apache.vxquery.runtime.functions.cast.CastToDoubleOperation;
import org.apache.vxquery.runtime.functions.cast.CastToFloatOperation;
import org.apache.vxquery.runtime.functions.cast.CastToStringOperation;
import org.apache.vxquery.types.BuiltinTypeRegistry;
import org.apache.vxquery.types.SequenceType;

public class PromoteScalarEvaluatorFactory extends AbstractTypeScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public PromoteScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws HyracksDataException {
        return new AbstractTypeScalarEvaluator(args, ctx) {
            final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
            final DataOutput dOut = abvs.getDataOutput();
            final TypedPointables tp = new TypedPointables();
            AbstractCastToOperation aOp = new CastToStringOperation();
            int castToTag = 0;

            @Override
            protected void evaluate(TaggedValuePointable tvp, IPointable result) throws SystemException {
                abvs.reset();
                int tid = tvp.getTag();
                if (castToTag == -1 || castToTag == 0) {
                    // The promote type is not supported. No change.
                    result.set(tvp);
                    return;
                } else if (castToTag > 0) {
                    try {
                        switch (tid) {
                            case ValueTag.XS_ANY_URI_TAG:
                                tvp.getValue(tp.utf8sp);
                                aOp.convertAnyURI(tp.utf8sp, dOut);
                                result.set(abvs.getByteArray(), abvs.getStartOffset(), abvs.getLength());
                                break;

                            case ValueTag.XS_BYTE_TAG:
                                tvp.getValue(tp.bytep);
                                aOp.convertByte(tp.bytep, dOut);
                                result.set(abvs.getByteArray(), abvs.getStartOffset(), abvs.getLength());
                                break;

                            case ValueTag.XS_DECIMAL_TAG:
                                tvp.getValue(tp.decp);
                                aOp.convertDecimal(tp.decp, dOut);
                                result.set(abvs.getByteArray(), abvs.getStartOffset(), abvs.getLength());
                                break;

                            case ValueTag.XS_DOUBLE_TAG:
                                tvp.getValue(tp.doublep);
                                aOp.convertDouble(tp.doublep, dOut);
                                result.set(abvs.getByteArray(), abvs.getStartOffset(), abvs.getLength());
                                break;

                            case ValueTag.XS_FLOAT_TAG:
                                tvp.getValue(tp.floatp);
                                aOp.convertFloat(tp.floatp, dOut);
                                result.set(abvs.getByteArray(), abvs.getStartOffset(), abvs.getLength());
                                break;

                            case ValueTag.XS_INT_TAG:
                                tvp.getValue(tp.intp);
                                aOp.convertInt(tp.intp, dOut);
                                result.set(abvs.getByteArray(), abvs.getStartOffset(), abvs.getLength());
                                break;

                            case ValueTag.XS_INTEGER_TAG:
                                tvp.getValue(tp.longp);
                                aOp.convertInteger(tp.longp, dOut);
                                result.set(abvs.getByteArray(), abvs.getStartOffset(), abvs.getLength());
                                break;

                            case ValueTag.XS_LONG_TAG:
                                tvp.getValue(tp.longp);
                                aOp.convertLong(tp.longp, dOut);
                                result.set(abvs.getByteArray(), abvs.getStartOffset(), abvs.getLength());
                                break;

                            case ValueTag.XS_NEGATIVE_INTEGER_TAG:
                                tvp.getValue(tp.longp);
                                aOp.convertNegativeInteger(tp.longp, dOut);
                                result.set(abvs.getByteArray(), abvs.getStartOffset(), abvs.getLength());
                                break;

                            case ValueTag.XS_NON_NEGATIVE_INTEGER_TAG:
                                tvp.getValue(tp.longp);
                                aOp.convertNonNegativeInteger(tp.longp, dOut);
                                result.set(abvs.getByteArray(), abvs.getStartOffset(), abvs.getLength());
                                break;

                            case ValueTag.XS_NON_POSITIVE_INTEGER_TAG:
                                tvp.getValue(tp.longp);
                                aOp.convertNonPositiveInteger(tp.longp, dOut);
                                result.set(abvs.getByteArray(), abvs.getStartOffset(), abvs.getLength());
                                break;

                            case ValueTag.XS_POSITIVE_INTEGER_TAG:
                                tvp.getValue(tp.longp);
                                aOp.convertPositiveInteger(tp.longp, dOut);
                                result.set(abvs.getByteArray(), abvs.getStartOffset(), abvs.getLength());
                                break;

                            case ValueTag.XS_SHORT_TAG:
                                tvp.getValue(tp.shortp);
                                aOp.convertShort(tp.shortp, dOut);
                                result.set(abvs.getByteArray(), abvs.getStartOffset(), abvs.getLength());
                                break;

                            case ValueTag.XS_STRING_TAG:
                                tvp.getValue(tp.utf8sp);
                                aOp.convertString(tp.utf8sp, dOut);
                                result.set(abvs.getByteArray(), abvs.getStartOffset(), abvs.getLength());
                                break;

                            case ValueTag.XS_UNSIGNED_BYTE_TAG:
                                tvp.getValue(tp.shortp);
                                aOp.convertUnsignedByte(tp.shortp, dOut);
                                result.set(abvs.getByteArray(), abvs.getStartOffset(), abvs.getLength());
                                break;

                            case ValueTag.XS_UNSIGNED_INT_TAG:
                                tvp.getValue(tp.longp);
                                aOp.convertUnsignedInt(tp.longp, dOut);
                                result.set(abvs.getByteArray(), abvs.getStartOffset(), abvs.getLength());
                                break;

                            case ValueTag.XS_UNSIGNED_LONG_TAG:
                                tvp.getValue(tp.longp);
                                aOp.convertUnsignedLong(tp.longp, dOut);
                                result.set(abvs.getByteArray(), abvs.getStartOffset(), abvs.getLength());
                                break;

                            case ValueTag.XS_UNSIGNED_SHORT_TAG:
                                tvp.getValue(tp.intp);
                                aOp.convertUnsignedShort(tp.intp, dOut);
                                result.set(abvs.getByteArray(), abvs.getStartOffset(), abvs.getLength());
                                break;

                            default:
                                // Promote type does not require us to change the value.
                                result.set(tvp);
                                break;
                        }
                    } catch (SystemException se) {
                        throw se;
                    } catch (Exception e) {
                        throw new SystemException(ErrorCode.SYSE0001, e);
                    }
                }
            }

            @Override
            protected void setSequenceType(SequenceType sType) {
                if (sType.getItemType() == BuiltinTypeRegistry.XS_DOUBLE) {
                    castToTag = ValueTag.XS_DOUBLE_TAG;
                    aOp = new CastToDoubleOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_FLOAT) {
                    castToTag = ValueTag.XS_FLOAT_TAG;
                    aOp = new CastToFloatOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_STRING) {
                    castToTag = ValueTag.XS_STRING_TAG;
                    aOp = new CastToStringOperation();
                } else {
                    castToTag = -1;
                }
            }
        };
    }
}
