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
package org.apache.vxquery.runtime.functions.util;

import java.io.DataOutput;

import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.TypedPointables;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.arithmetic.AbstractArithmeticOperation;
import org.apache.vxquery.runtime.functions.cast.CastToDoubleOperation;
import org.apache.vxquery.types.BuiltinTypeConstants;
import org.apache.vxquery.types.BuiltinTypeRegistry;

import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class ArithmeticHelper {
    private final AbstractArithmeticOperation aOp;
    private final DynamicContext dCtx;
    private final TypedPointables tp1;
    private final TypedPointables tp2;
    private final ArrayBackedValueStorage abvs;
    private final DataOutput dOut;
    private final ArrayBackedValueStorage abvsArgument1;
    private final DataOutput dOutArgument1;
    private final ArrayBackedValueStorage abvsArgument2;
    private final DataOutput dOutArgument2;
    private final CastToDoubleOperation castToDouble;

    public ArithmeticHelper(AbstractArithmeticOperation aOp, DynamicContext dCtx) {
        this.aOp = aOp;
        this.dCtx = dCtx;
        tp1 = new TypedPointables();
        tp2 = new TypedPointables();
        abvs = new ArrayBackedValueStorage();
        dOut = abvs.getDataOutput();
        abvsArgument1 = new ArrayBackedValueStorage();
        dOutArgument1 = abvsArgument1.getDataOutput();
        abvsArgument2 = new ArrayBackedValueStorage();
        dOutArgument2 = abvsArgument2.getDataOutput();
        castToDouble = new CastToDoubleOperation();
    }

    public void compute(TaggedValuePointable tvp1, TaggedValuePointable tvp2, IPointable result) throws SystemException {
        abvs.reset();
        try {
            int tid1 = getBaseTypeForArithmetics(tvp1.getTag());
            int tid2 = getBaseTypeForArithmetics(tvp2.getTag());
            LongPointable longp1 = (LongPointable) LongPointable.FACTORY.createPointable();
            DoublePointable doublep1 = (DoublePointable) DoublePointable.FACTORY.createPointable();
            switch (tvp1.getTag()) {
                case ValueTag.XS_INTEGER_TAG:
                case ValueTag.XS_NON_POSITIVE_INTEGER_TAG:
                case ValueTag.XS_NEGATIVE_INTEGER_TAG:
                case ValueTag.XS_LONG_TAG:
                case ValueTag.XS_NON_NEGATIVE_INTEGER_TAG:
                case ValueTag.XS_UNSIGNED_LONG_TAG:
                case ValueTag.XS_POSITIVE_INTEGER_TAG:
                case ValueTag.XS_INT_TAG:
                case ValueTag.XS_UNSIGNED_INT_TAG:
                case ValueTag.XS_SHORT_TAG:
                case ValueTag.XS_UNSIGNED_SHORT_TAG:
                case ValueTag.XS_BYTE_TAG:
                case ValueTag.XS_UNSIGNED_BYTE_TAG:
                    abvsArgument1.reset();
                    FunctionHelper.getIntegerPointable(tvp1, dOutArgument1, tp1);
                    longp1.set(abvsArgument1.getByteArray(), abvsArgument1.getStartOffset() + 1,
                            LongPointable.TYPE_TRAITS.getFixedLength());
                    break;
                case ValueTag.XS_DOUBLE_TAG:
                    tvp1.getValue(doublep1);
                    break;
                case ValueTag.XS_UNTYPED_ATOMIC_TAG:
                    tid1 = ValueTag.XS_DOUBLE_TAG;
                    tvp1.getValue(tp1.utf8sp);
                    abvsArgument1.reset();
                    castToDouble.convertUntypedAtomic(tp1.utf8sp, dOutArgument1);
                    doublep1.set(abvsArgument1.getByteArray(), abvsArgument1.getStartOffset() + 1,
                            DoublePointable.TYPE_TRAITS.getFixedLength());
                    break;
            }
            LongPointable longp2 = (LongPointable) LongPointable.FACTORY.createPointable();
            DoublePointable doublep2 = (DoublePointable) DoublePointable.FACTORY.createPointable();
            switch (tvp2.getTag()) {
                case ValueTag.XS_INTEGER_TAG:
                case ValueTag.XS_NON_POSITIVE_INTEGER_TAG:
                case ValueTag.XS_NEGATIVE_INTEGER_TAG:
                case ValueTag.XS_LONG_TAG:
                case ValueTag.XS_NON_NEGATIVE_INTEGER_TAG:
                case ValueTag.XS_UNSIGNED_LONG_TAG:
                case ValueTag.XS_POSITIVE_INTEGER_TAG:
                case ValueTag.XS_INT_TAG:
                case ValueTag.XS_UNSIGNED_INT_TAG:
                case ValueTag.XS_SHORT_TAG:
                case ValueTag.XS_UNSIGNED_SHORT_TAG:
                case ValueTag.XS_BYTE_TAG:
                case ValueTag.XS_UNSIGNED_BYTE_TAG:
                    abvsArgument2.reset();
                    FunctionHelper.getIntegerPointable(tvp2, dOutArgument2, tp2);
                    longp2.set(abvsArgument2.getByteArray(), abvsArgument2.getStartOffset() + 1,
                            LongPointable.TYPE_TRAITS.getFixedLength());
                    break;
                case ValueTag.XS_DOUBLE_TAG:
                    tvp2.getValue(doublep2);
                    break;
                case ValueTag.XS_UNTYPED_ATOMIC_TAG:
                    tid2 = ValueTag.XS_DOUBLE_TAG;
                    tvp2.getValue(tp2.utf8sp);
                    abvsArgument2.reset();
                    castToDouble.convertUntypedAtomic(tp2.utf8sp, dOutArgument2);
                    doublep2.set(abvsArgument2.getByteArray(), abvsArgument2.getStartOffset() + 1,
                            DoublePointable.TYPE_TRAITS.getFixedLength());
                    break;
            }
            switch (tid1) {
                case ValueTag.XS_DECIMAL_TAG:
                    tvp1.getValue(tp1.decp);
                    switch (tid2) {
                        case ValueTag.XS_DECIMAL_TAG:
                            tvp2.getValue(tp2.decp);
                            aOp.operateDecimalDecimal(tp1.decp, tp2.decp, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_INTEGER_TAG:
                            aOp.operateDecimalInteger(tp1.decp, longp2, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_FLOAT_TAG:
                            tvp2.getValue(tp2.floatp);
                            aOp.operateDecimalFloat(tp1.decp, tp2.floatp, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_DOUBLE_TAG:
                            aOp.operateDecimalDouble(tp1.decp, doublep2, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_DAY_TIME_DURATION_TAG:
                            tvp2.getValue(tp2.longp);
                            aOp.operateDecimalDTDuration(tp1.decp, tp2.longp, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                            tvp2.getValue(tp2.intp);
                            aOp.operateDecimalYMDuration(tp1.decp, tp2.intp, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;
                    }
                    break;

                case ValueTag.XS_INTEGER_TAG:
                    switch (tid2) {
                        case ValueTag.XS_DECIMAL_TAG:
                            tvp2.getValue(tp2.decp);
                            aOp.operateIntegerDecimal(longp1, tp2.decp, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_INTEGER_TAG:
                            aOp.operateIntegerInteger(longp1, longp2, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_FLOAT_TAG:
                            tvp2.getValue(tp2.floatp);
                            aOp.operateIntegerFloat(longp1, tp2.floatp, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_DOUBLE_TAG:
                            aOp.operateIntegerDouble(longp1, doublep2, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_DAY_TIME_DURATION_TAG:
                            tvp2.getValue(tp2.longp);
                            aOp.operateIntegerDTDuration(longp1, tp2.longp, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                            tvp2.getValue(tp2.intp);
                            aOp.operateIntegerYMDuration(longp1, tp2.intp, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;
                    }
                    break;

                case ValueTag.XS_FLOAT_TAG:
                    tvp1.getValue(tp1.floatp);
                    switch (tid2) {
                        case ValueTag.XS_DECIMAL_TAG:
                            tvp2.getValue(tp2.decp);
                            aOp.operateFloatDecimal(tp1.floatp, tp2.decp, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_INTEGER_TAG:
                            aOp.operateFloatInteger(tp1.floatp, longp2, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_FLOAT_TAG:
                            tvp2.getValue(tp2.floatp);
                            aOp.operateFloatFloat(tp1.floatp, tp2.floatp, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_DOUBLE_TAG:
                            aOp.operateFloatDouble(tp1.floatp, doublep2, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_DAY_TIME_DURATION_TAG:
                            tvp2.getValue(tp2.longp);
                            aOp.operateFloatDTDuration(tp1.floatp, tp2.longp, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                            tvp2.getValue(tp2.intp);
                            aOp.operateFloatYMDuration(tp1.floatp, tp2.intp, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;
                    }
                    break;

                case ValueTag.XS_DOUBLE_TAG:
                    switch (tid2) {
                        case ValueTag.XS_DECIMAL_TAG:
                            tvp2.getValue(tp2.decp);
                            aOp.operateDoubleDecimal(doublep1, tp2.decp, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_INTEGER_TAG:
                            aOp.operateDoubleInteger(doublep1, longp2, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_FLOAT_TAG:
                            tvp2.getValue(tp2.floatp);
                            aOp.operateDoubleFloat(doublep1, tp2.floatp, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_DOUBLE_TAG:
                            aOp.operateDoubleDouble(doublep1, doublep2, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_DAY_TIME_DURATION_TAG:
                            tvp2.getValue(tp2.longp);
                            aOp.operateDoubleDTDuration(doublep1, tp2.longp, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                            tvp2.getValue(tp2.intp);
                            aOp.operateDoubleYMDuration(doublep1, tp2.intp, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;
                    }
                    break;

                case ValueTag.XS_DATE_TAG:
                    tvp1.getValue(tp1.datep);
                    switch (tid2) {
                        case ValueTag.XS_DATE_TAG:
                            tvp2.getValue(tp2.datep);
                            aOp.operateDateDate(tp1.datep, tp2.datep, dCtx, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_DAY_TIME_DURATION_TAG:
                            tvp2.getValue(tp2.longp);
                            aOp.operateDateDTDuration(tp1.datep, tp2.longp, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                            tvp2.getValue(tp2.intp);
                            aOp.operateDateYMDuration(tp1.datep, tp2.intp, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;
                    }
                    break;

                case ValueTag.XS_DATETIME_TAG:
                    tvp1.getValue(tp1.datetimep);
                    switch (tid2) {
                        case ValueTag.XS_DATETIME_TAG:
                            tvp2.getValue(tp2.datetimep);
                            aOp.operateDatetimeDatetime(tp1.datetimep, tp2.datetimep, dCtx, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_DAY_TIME_DURATION_TAG:
                            tvp2.getValue(tp2.longp);
                            aOp.operateDatetimeDTDuration(tp1.datetimep, tp2.longp, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                            tvp2.getValue(tp2.intp);
                            aOp.operateDatetimeYMDuration(tp1.datetimep, tp2.intp, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;
                    }
                    break;

                case ValueTag.XS_TIME_TAG:
                    tvp1.getValue(tp1.timep);
                    switch (tid2) {
                        case ValueTag.XS_TIME_TAG:
                            tvp2.getValue(tp2.timep);
                            aOp.operateTimeTime(tp1.timep, tp2.timep, dCtx, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_DAY_TIME_DURATION_TAG:
                            tvp2.getValue(tp2.longp);
                            aOp.operateTimeDTDuration(tp1.timep, tp2.longp, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                    }
                    break;

                case ValueTag.XS_DAY_TIME_DURATION_TAG:
                    tvp1.getValue(tp1.longp);
                    switch (tid2) {
                        case ValueTag.XS_DECIMAL_TAG:
                            tvp2.getValue(tp2.decp);
                            aOp.operateDTDurationDecimal(tp1.longp, tp2.decp, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_INTEGER_TAG:
                            aOp.operateDTDurationInteger(tp1.longp, longp2, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_FLOAT_TAG:
                            tvp2.getValue(tp2.floatp);
                            aOp.operateDTDurationFloat(tp1.longp, tp2.floatp, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_DOUBLE_TAG:
                            aOp.operateDTDurationDouble(tp1.longp, doublep2, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_DATE_TAG:
                            tvp2.getValue(tp2.datep);
                            aOp.operateDTDurationDate(tp1.longp, tp2.datep, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_TIME_TAG:
                            tvp2.getValue(tp2.timep);
                            aOp.operateDTDurationTime(tp1.longp, tp2.timep, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_DATETIME_TAG:
                            tvp2.getValue(tp2.datetimep);
                            aOp.operateDTDurationDatetime(tp1.longp, tp2.datetimep, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_DAY_TIME_DURATION_TAG:
                            tvp2.getValue(tp2.longp);
                            aOp.operateDTDurationDTDuration(tp1.longp, tp2.longp, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;
                    }
                    break;

                case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                    tvp1.getValue(tp1.intp);
                    switch (tid2) {
                        case ValueTag.XS_DECIMAL_TAG:
                            tvp2.getValue(tp2.decp);
                            aOp.operateYMDurationDecimal(tp1.intp, tp2.decp, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_INTEGER_TAG:
                            aOp.operateYMDurationInteger(tp1.intp, longp2, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_FLOAT_TAG:
                            tvp2.getValue(tp2.floatp);
                            aOp.operateYMDurationFloat(tp1.intp, tp2.floatp, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_DOUBLE_TAG:
                            aOp.operateYMDurationDouble(tp1.intp, doublep2, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_DATE_TAG:
                            tvp2.getValue(tp2.datep);
                            aOp.operateYMDurationDate(tp1.intp, tp2.datep, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_DATETIME_TAG:
                            tvp2.getValue(tp2.datetimep);
                            aOp.operateYMDurationDatetime(tp1.intp, tp2.datetimep, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;

                        case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                            tvp2.getValue(tp2.intp);
                            aOp.operateYMDurationYMDuration(tp1.intp, tp2.intp, dOut);
                            result.set(abvs.getByteArray(), 0, abvs.getLength());
                            return;
                    }
                    break;
            }
        } catch (SystemException se) {
            throw se;
        } catch (Exception e) {
            throw new SystemException(ErrorCode.SYSE0001, e);
        }
        throw new SystemException(ErrorCode.XPTY0004);
    }

    public static int getBaseTypeForArithmetics(int tid) throws SystemException {
        if (tid >= BuiltinTypeConstants.BUILTIN_TYPE_COUNT) {
            throw new SystemException(ErrorCode.XPTY0004);
        }
        while (true) {
            switch (tid) {
                case ValueTag.XS_STRING_TAG:
                case ValueTag.XS_DECIMAL_TAG:
                case ValueTag.XS_INTEGER_TAG:
                case ValueTag.XS_FLOAT_TAG:
                case ValueTag.XS_DOUBLE_TAG:
                case ValueTag.XS_ANY_URI_TAG:
                case ValueTag.XS_BOOLEAN_TAG:
                case ValueTag.XS_DATE_TAG:
                case ValueTag.XS_DATETIME_TAG:
                case ValueTag.XS_TIME_TAG:
                case ValueTag.XS_DAY_TIME_DURATION_TAG:
                case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                case ValueTag.XS_BASE64_BINARY_TAG:
                case ValueTag.XS_HEX_BINARY_TAG:
                case ValueTag.XS_QNAME_TAG:
                case ValueTag.XS_G_DAY_TAG:
                case ValueTag.XS_G_MONTH_DAY_TAG:
                case ValueTag.XS_G_MONTH_TAG:
                case ValueTag.XS_G_YEAR_MONTH_TAG:
                case ValueTag.XS_G_YEAR_TAG:
                case ValueTag.XS_UNTYPED_ATOMIC_TAG:
                    return tid;

                case ValueTag.XS_LONG_TAG:
                case ValueTag.XS_NEGATIVE_INTEGER_TAG:
                case ValueTag.XS_NON_POSITIVE_INTEGER_TAG:
                case ValueTag.XS_NON_NEGATIVE_INTEGER_TAG:
                case ValueTag.XS_POSITIVE_INTEGER_TAG:
                case ValueTag.XS_UNSIGNED_INT_TAG:
                case ValueTag.XS_UNSIGNED_LONG_TAG:
                case ValueTag.XS_INT_TAG:
                case ValueTag.XS_UNSIGNED_SHORT_TAG:
                case ValueTag.XS_SHORT_TAG:
                case ValueTag.XS_UNSIGNED_BYTE_TAG:
                case ValueTag.XS_BYTE_TAG:
                    return ValueTag.XS_INTEGER_TAG;

                case ValueTag.XS_ANY_ATOMIC_TAG:
                    throw new SystemException(ErrorCode.XPTY0004);

                default:
                    tid = BuiltinTypeRegistry.INSTANCE.getSchemaTypeById(tid).getBaseType().getTypeId();
                    return tid;
            }
        }
    }

}
