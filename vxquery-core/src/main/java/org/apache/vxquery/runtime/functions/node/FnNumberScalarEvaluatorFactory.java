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
package org.apache.vxquery.runtime.functions.node;

import java.io.DataOutput;
import java.io.IOException;

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
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.runtime.functions.cast.CastToDoubleOperation;

public class FnNumberScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnNumberScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws HyracksDataException {
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final DataOutput dOut = abvs.getDataOutput();
        final CastToDoubleOperation castToDouble = new CastToDoubleOperation();
        final TypedPointables tp = new TypedPointables();

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                TaggedValuePointable tvp1 = args[0];
                try {
                    abvs.reset();
                    switch (tvp1.getTag()) {
                        case ValueTag.XS_STRING_TAG:
                            tvp1.getValue(tp.utf8sp);
                            castToDouble.convertString(tp.utf8sp, dOut);
                            break;
                        case ValueTag.XS_UNTYPED_ATOMIC_TAG:
                            tvp1.getValue(tp.utf8sp);
                            castToDouble.convertUntypedAtomic(tp.utf8sp, dOut);
                            break;
                        case ValueTag.XS_BOOLEAN_TAG:
                            tvp1.getValue(tp.boolp);
                            castToDouble.convertBoolean(tp.boolp, dOut);
                            break;
                        // case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                        case ValueTag.XS_INTEGER_TAG:
                        case ValueTag.XS_NON_POSITIVE_INTEGER_TAG:
                        case ValueTag.XS_NEGATIVE_INTEGER_TAG:
                        case ValueTag.XS_LONG_TAG:
                        case ValueTag.XS_NON_NEGATIVE_INTEGER_TAG:
                        case ValueTag.XS_UNSIGNED_LONG_TAG:
                        case ValueTag.XS_POSITIVE_INTEGER_TAG:
                        case ValueTag.XS_UNSIGNED_INT_TAG:
                            tvp1.getValue(tp.longp);
                            castToDouble.convertInteger(tp.longp, dOut);
                            break;
                        // case ValueTag.XS_DAY_TIME_DURATION_TAG:
                        case ValueTag.XS_INT_TAG:
                        case ValueTag.XS_UNSIGNED_SHORT_TAG:
                            tvp1.getValue(tp.intp);
                            castToDouble.convertInt(tp.intp, dOut);
                            break;
                        case ValueTag.XS_DECIMAL_TAG:
                            tvp1.getValue(tp.decp);
                            castToDouble.convertDecimal(tp.decp, dOut);
                            break;
                        case ValueTag.XS_DOUBLE_TAG:
                            tvp1.getValue(tp.doublep);
                            castToDouble.convertDouble(tp.doublep, dOut);
                            break;
                        case ValueTag.XS_FLOAT_TAG:
                            tvp1.getValue(tp.floatp);
                            castToDouble.convertFloat(tp.floatp, dOut);
                            break;
                        case ValueTag.XS_SHORT_TAG:
                        case ValueTag.XS_UNSIGNED_BYTE_TAG:
                            tvp1.getValue(tp.shortp);
                            castToDouble.convertShort(tp.shortp, dOut);
                            break;
                        case ValueTag.XS_BYTE_TAG:
                            tvp1.getValue(tp.bytep);
                            castToDouble.convertByte(tp.bytep, dOut);
                            break;
                        default:
                            dOut.write(ValueTag.XS_DOUBLE_TAG);
                            dOut.writeDouble(Double.NaN);
                    }

                    result.set(abvs);
                } catch (SystemException e) {
                    try {
                        abvs.reset();
                        dOut.write(ValueTag.XS_DOUBLE_TAG);
                        dOut.writeDouble(Double.NaN);
                        result.set(abvs);
                    } catch (IOException e1) {
                        throw new SystemException(ErrorCode.SYSE0001, e);
                    }
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
            }

        };
    }
}
