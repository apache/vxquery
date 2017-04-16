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
package org.apache.vxquery.runtime.functions.numeric;

import java.io.DataOutput;
import java.io.IOException;

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
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.TypedPointables;
import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.runtime.functions.cast.CastToDecimalOperation;
import org.apache.vxquery.types.BuiltinTypeRegistry;

public class FnRoundHalfToEvenScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnRoundHalfToEvenScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected AbstractTaggedValueArgumentScalarEvaluator createEvaluator(IHyracksTaskContext ctx,
            IScalarEvaluator[] args) throws HyracksDataException {
        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
            final DataOutput dOut = abvs.getDataOutput();
            final ArrayBackedValueStorage abvsInner = new ArrayBackedValueStorage();
            final DataOutput dOutInner = abvsInner.getDataOutput();
            final TypedPointables tp = new TypedPointables();
            final LongPointable longp = (LongPointable) LongPointable.FACTORY.createPointable();
            final CastToDecimalOperation castToDecimal = new CastToDecimalOperation();

            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                TaggedValuePointable tvp1 = args[0];
                int tid = getBaseTypeForArithmetics(tvp1.getTag());

                long precision = 0;
                if (args.length > 1) {
                    TaggedValuePointable tvp2 = args[1];
                    if (tvp2.getTag() != ValueTag.XS_INTEGER_TAG) {
                        throw new SystemException(ErrorCode.FORG0006);
                    }
                    tvp2.getValue(longp);
                    precision = longp.getLong();
                }

                // Check special cases.
                try {
                    switch (tid) {
                        case ValueTag.XS_FLOAT_TAG:
                            tvp1.getValue(tp.floatp);
                            if (tp.floatp.getFloat() == 0 || Float.isNaN(tp.floatp.getFloat())
                                    || Float.isInfinite(tp.floatp.getFloat())) {
                                result.set(tvp1.getByteArray(), tvp1.getStartOffset(),
                                        FloatPointable.TYPE_TRAITS.getFixedLength() + 1);
                                return;
                            }
                            break;

                        case ValueTag.XS_DOUBLE_TAG:
                            tvp1.getValue(tp.doublep);
                            if (tp.doublep.getDouble() == 0 || Double.isNaN(tp.doublep.getDouble())
                                    || Double.isInfinite(tp.doublep.getDouble())) {
                                result.set(tvp1.getByteArray(), tvp1.getStartOffset(),
                                        DoublePointable.TYPE_TRAITS.getFixedLength() + 1);
                                return;
                            }
                            break;
                    }
                } catch (Exception e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
                // Prepare input.
                try {
                    getDecimalPointable(tp, tvp1);
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
                // Perform rounding on decimal value.
                byte decimalPlace = tp.decp.getDecimalPlace();
                long decimalValue = tp.decp.getDecimalValue();
                long newValue;
                //check if the input needs to rounded to even or normally
                if (decimalPlace - precision == 1 && (Math.abs(decimalValue) % 10 == 5)) {
                    newValue = decimalValue / 10;
                    if (!(newValue % 2 == 0)) {
                        if (newValue > 0) {
                            newValue += 1;
                        } else {
                            newValue -= 1;
                        }
                    }
                    tp.decp.setDecimal(newValue, (byte) precision);
                } else if ((precision - decimalPlace) < 0) {
                    decimalValue = (long) Math.round(decimalValue / Math.pow(10, -(precision - decimalPlace)));
                    tp.decp.setDecimal(decimalValue, (byte) precision);
                }
                // Return result.
                try {
                    switch (tvp1.getTag()) {
                        case ValueTag.XS_DECIMAL_TAG:
                            dOut.write(ValueTag.XS_DECIMAL_TAG);
                            dOut.write(tp.decp.getByteArray(), tp.decp.getStartOffset(),
                                    XSDecimalPointable.TYPE_TRAITS.getFixedLength());
                            result.set(abvs.getByteArray(), abvs.getStartOffset(),
                                    XSDecimalPointable.TYPE_TRAITS.getFixedLength() + 1);
                            return;

                        case ValueTag.XS_INTEGER_TAG:
                        case ValueTag.XS_LONG_TAG:
                        case ValueTag.XS_NEGATIVE_INTEGER_TAG:
                        case ValueTag.XS_NON_POSITIVE_INTEGER_TAG:
                        case ValueTag.XS_NON_NEGATIVE_INTEGER_TAG:
                        case ValueTag.XS_POSITIVE_INTEGER_TAG:
                        case ValueTag.XS_UNSIGNED_INT_TAG:
                        case ValueTag.XS_UNSIGNED_LONG_TAG:
                            dOut.write(tvp1.getTag());
                            dOut.writeLong(tp.decp.longValue());
                            result.set(abvs.getByteArray(), abvs.getStartOffset(),
                                    LongPointable.TYPE_TRAITS.getFixedLength() + 1);
                            return;

                        case ValueTag.XS_INT_TAG:
                        case ValueTag.XS_UNSIGNED_SHORT_TAG:
                            dOut.write(tvp1.getTag());
                            dOut.writeInt(tp.decp.intValue());
                            result.set(abvs.getByteArray(), abvs.getStartOffset(),
                                    IntegerPointable.TYPE_TRAITS.getFixedLength() + 1);
                            return;

                        case ValueTag.XS_SHORT_TAG:
                        case ValueTag.XS_UNSIGNED_BYTE_TAG:
                            dOut.write(tvp1.getTag());
                            dOut.writeShort(tp.decp.shortValue());
                            result.set(abvs.getByteArray(), abvs.getStartOffset(),
                                    ShortPointable.TYPE_TRAITS.getFixedLength() + 1);
                            return;

                        case ValueTag.XS_BYTE_TAG:
                            dOut.write(tvp1.getTag());
                            dOut.writeByte(tp.decp.byteValue());
                            result.set(abvs.getByteArray(), abvs.getStartOffset(),
                                    BytePointable.TYPE_TRAITS.getFixedLength() + 1);
                            return;

                        case ValueTag.XS_FLOAT_TAG:
                            dOut.write(ValueTag.XS_FLOAT_TAG);
                            dOut.writeFloat(tp.decp.floatValue());
                            result.set(abvs.getByteArray(), abvs.getStartOffset(),
                                    FloatPointable.TYPE_TRAITS.getFixedLength() + 1);
                            return;

                        case ValueTag.XS_DOUBLE_TAG:
                            dOut.write(ValueTag.XS_DOUBLE_TAG);
                            dOut.writeDouble(tp.decp.doubleValue());
                            result.set(abvs.getByteArray(), abvs.getStartOffset(),
                                    DoublePointable.TYPE_TRAITS.getFixedLength() + 1);
                            return;
                    }
                } catch (Exception e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
            }

            private void getDecimalPointable(TypedPointables tp, TaggedValuePointable tvp) throws SystemException,
                    IOException {
                abvsInner.reset();
                long value;
                switch (tvp.getTag()) {
                    case ValueTag.XS_DECIMAL_TAG:
                        tvp.getValue(tp.decp);
                        return;

                    case ValueTag.XS_FLOAT_TAG:
                        tvp.getValue(tp.floatp);
                        castToDecimal.convertFloat(tp.floatp, dOutInner);
                        tp.decp.set(abvsInner.getByteArray(), abvsInner.getStartOffset() + 1,
                                XSDecimalPointable.TYPE_TRAITS.getFixedLength());
                        return;

                    case ValueTag.XS_DOUBLE_TAG:
                        tvp.getValue(tp.doublep);
                        castToDecimal.convertDouble(tp.doublep, dOutInner);
                        tp.decp.set(abvsInner.getByteArray(), abvsInner.getStartOffset() + 1,
                                XSDecimalPointable.TYPE_TRAITS.getFixedLength());
                        return;

                    case ValueTag.XS_INTEGER_TAG:
                    case ValueTag.XS_LONG_TAG:
                    case ValueTag.XS_NEGATIVE_INTEGER_TAG:
                    case ValueTag.XS_NON_POSITIVE_INTEGER_TAG:
                    case ValueTag.XS_NON_NEGATIVE_INTEGER_TAG:
                    case ValueTag.XS_POSITIVE_INTEGER_TAG:
                    case ValueTag.XS_UNSIGNED_INT_TAG:
                    case ValueTag.XS_UNSIGNED_LONG_TAG:
                        tvp.getValue(tp.longp);
                        value = tp.longp.longValue();
                        break;

                    case ValueTag.XS_INT_TAG:
                    case ValueTag.XS_UNSIGNED_SHORT_TAG:
                        tvp.getValue(tp.intp);
                        value = tp.intp.longValue();
                        break;

                    case ValueTag.XS_SHORT_TAG:
                    case ValueTag.XS_UNSIGNED_BYTE_TAG:
                        tvp.getValue(tp.shortp);
                        value = tp.shortp.longValue();
                        break;

                    case ValueTag.XS_BYTE_TAG:
                        tvp.getValue(tp.bytep);
                        value = tp.bytep.longValue();
                        break;

                    default:
                        throw new SystemException(ErrorCode.XPTY0004);
                }
                dOutInner.write(ValueTag.XS_DECIMAL_TAG);
                tp.decp.set(abvsInner.getByteArray(), abvsInner.getStartOffset() + 1,
                        XSDecimalPointable.TYPE_TRAITS.getFixedLength());
                tp.decp.setDecimal(value, (byte) 0);
            }

            private int getBaseTypeForArithmetics(int tid) throws SystemException {
                while (true) {
                    switch (tid) {
                        case ValueTag.XS_DECIMAL_TAG:
                        case ValueTag.XS_DOUBLE_TAG:
                        case ValueTag.XS_FLOAT_TAG:
                        case ValueTag.XS_INTEGER_TAG:
                            return tid;

                        case ValueTag.XS_ANY_ATOMIC_TAG:
                            throw new SystemException(ErrorCode.XPTY0004);

                        default:
                            tid = BuiltinTypeRegistry.INSTANCE.getSchemaTypeById(tid).getBaseType().getTypeId();
                    }
                }
            }
        };
    }

}
