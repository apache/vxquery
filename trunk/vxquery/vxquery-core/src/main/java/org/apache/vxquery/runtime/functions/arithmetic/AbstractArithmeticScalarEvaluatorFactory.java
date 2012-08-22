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
package org.apache.vxquery.runtime.functions.arithmetic;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDatePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDateTimePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSTimePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.types.BuiltinTypeConstants;
import org.apache.vxquery.types.BuiltinTypeRegistry;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.BytePointable;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.primitive.ShortPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public abstract class AbstractArithmeticScalarEvaluatorFactory extends
        AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public AbstractArithmeticScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final AbstractArithmeticOperation aOp = createArithmeticOperation();
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final DataOutput dOut = abvs.getDataOutput();
        final ArrayBackedValueStorage abvsInteger1 = new ArrayBackedValueStorage();
        final DataOutput dOutInteger1 = abvsInteger1.getDataOutput();
        final ArrayBackedValueStorage abvsInteger2 = new ArrayBackedValueStorage();
        final DataOutput dOutInteger2 = abvsInteger2.getDataOutput();
        final TypedPointables tp1 = new TypedPointables();
        final TypedPointables tp2 = new TypedPointables();
        final SequencePointable seqp = (SequencePointable) SequencePointable.FACTORY.createPointable();
        final DynamicContext dCtx = (DynamicContext) ctx.getJobletContext().getGlobalJobData();

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                TaggedValuePointable tvp1 = args[0];
                TaggedValuePointable tvp2 = args[1];
                if (tvp1.getTag() == ValueTag.SEQUENCE_TAG) {
                    tvp1.getValue(seqp);
                    if (seqp.getEntryCount() == 0) {
                        result.set(tvp1);
                        return;
                    }
                    throw new SystemException(ErrorCode.XPTY0004);
                }
                if (tvp2.getTag() == ValueTag.SEQUENCE_TAG) {
                    tvp2.getValue(seqp);
                    if (seqp.getEntryCount() == 0) {
                        result.set(tvp2);
                        return;
                    }
                    throw new SystemException(ErrorCode.XPTY0004);
                }
                int tid1 = getBaseTypeForArithmetics(tvp1.getTag());
                int tid2 = getBaseTypeForArithmetics(tvp2.getTag());
                if (tid1 == ValueTag.XS_UNTYPED_ATOMIC_TAG) {
                    // TODO Convert to double
                    tid1 = ValueTag.XS_DOUBLE_TAG;
                    throw new UnsupportedOperationException();
                }
                if (tid2 == ValueTag.XS_UNTYPED_ATOMIC_TAG) {
                    // TODO Convert to double
                    tid2 = ValueTag.XS_DOUBLE_TAG;
                    throw new UnsupportedOperationException();
                }
                abvs.reset();
                try {
                    LongPointable longp1 = (LongPointable) LongPointable.FACTORY.createPointable();
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
                            abvsInteger1.reset();
                            getIntegerPointable(tp1, tvp1, dOutInteger1);
                            longp1.set(abvsInteger1.getByteArray(), abvsInteger1.getStartOffset() + 1,
                                    LongPointable.TYPE_TRAITS.getFixedLength());
                    }
                    LongPointable longp2 = (LongPointable) LongPointable.FACTORY.createPointable();
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
                            abvsInteger2.reset();
                            getIntegerPointable(tp2, tvp2, dOutInteger2);
                            longp2.set(abvsInteger2.getByteArray(), abvsInteger2.getStartOffset() + 1,
                                    LongPointable.TYPE_TRAITS.getFixedLength());
                    }
                    switch (tid1) {
                        case ValueTag.XS_DECIMAL_TAG:
                            tvp1.getValue(tp1.decp);
                            switch (tid2) {
                                case ValueTag.XS_DECIMAL_TAG:
                                    tvp2.getValue(tp2.decp);
                                    aOp.operateDecimalDecimal(tp1.decp, tp2.decp, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_INTEGER_TAG:
                                    aOp.operateDecimalInteger(tp1.decp, longp2, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_FLOAT_TAG:
                                    tvp2.getValue(tp2.floatp);
                                    aOp.operateDecimalFloat(tp1.decp, tp2.floatp, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_DOUBLE_TAG:
                                    tvp2.getValue(tp2.doublep);
                                    aOp.operateDecimalDouble(tp1.decp, tp2.doublep, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_DAY_TIME_DURATION_TAG:
                                    tvp2.getValue(tp2.longp);
                                    aOp.operateDecimalDTDuration(tp1.decp, tp2.longp, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                                    tvp2.getValue(tp2.intp);
                                    aOp.operateDecimalYMDuration(tp1.decp, tp2.intp, dOut);
                                    result.set(abvs);
                                    return;
                            }
                            break;

                        case ValueTag.XS_INTEGER_TAG:
                            switch (tid2) {
                                case ValueTag.XS_DECIMAL_TAG:
                                    tvp2.getValue(tp2.decp);
                                    aOp.operateIntegerDecimal(longp1, tp2.decp, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_INTEGER_TAG:
                                    aOp.operateIntegerInteger(longp1, longp2, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_FLOAT_TAG:
                                    tvp2.getValue(tp2.floatp);
                                    aOp.operateIntegerFloat(longp1, tp2.floatp, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_DOUBLE_TAG:
                                    tvp2.getValue(tp2.doublep);
                                    aOp.operateIntegerDouble(longp1, tp2.doublep, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_DAY_TIME_DURATION_TAG:
                                    tvp2.getValue(tp2.longp);
                                    aOp.operateIntegerDTDuration(longp1, tp2.longp, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                                    tvp2.getValue(tp2.intp);
                                    aOp.operateIntegerYMDuration(longp1, tp2.intp, dOut);
                                    result.set(abvs);
                                    return;
                            }
                            break;

                        case ValueTag.XS_FLOAT_TAG:
                            tvp1.getValue(tp1.floatp);
                            switch (tid2) {
                                case ValueTag.XS_DECIMAL_TAG:
                                    tvp2.getValue(tp2.decp);
                                    aOp.operateFloatDecimal(tp1.floatp, tp2.decp, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_INTEGER_TAG:
                                    aOp.operateFloatInteger(tp1.floatp, longp2, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_FLOAT_TAG:
                                    tvp2.getValue(tp2.floatp);
                                    aOp.operateFloatFloat(tp1.floatp, tp2.floatp, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_DOUBLE_TAG:
                                    tvp2.getValue(tp2.doublep);
                                    aOp.operateFloatDouble(tp1.floatp, tp2.doublep, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_DAY_TIME_DURATION_TAG:
                                    tvp2.getValue(tp2.longp);
                                    aOp.operateFloatDTDuration(tp1.floatp, tp2.longp, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                                    tvp2.getValue(tp2.intp);
                                    aOp.operateFloatYMDuration(tp1.floatp, tp2.intp, dOut);
                                    result.set(abvs);
                                    return;
                            }
                            break;

                        case ValueTag.XS_DOUBLE_TAG:
                            tvp1.getValue(tp1.doublep);
                            switch (tid2) {
                                case ValueTag.XS_DECIMAL_TAG:
                                    tvp2.getValue(tp2.decp);
                                    aOp.operateDoubleDecimal(tp1.doublep, tp2.decp, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_INTEGER_TAG:
                                    aOp.operateDoubleInteger(tp1.doublep, longp2, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_FLOAT_TAG:
                                    tvp2.getValue(tp2.floatp);
                                    aOp.operateDoubleFloat(tp1.doublep, tp2.floatp, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_DOUBLE_TAG:
                                    tvp2.getValue(tp2.doublep);
                                    aOp.operateDoubleDouble(tp1.doublep, tp2.doublep, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_DAY_TIME_DURATION_TAG:
                                    tvp2.getValue(tp2.longp);
                                    aOp.operateDoubleDTDuration(tp1.doublep, tp2.longp, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                                    tvp2.getValue(tp2.intp);
                                    aOp.operateDoubleYMDuration(tp1.doublep, tp2.intp, dOut);
                                    result.set(abvs);
                                    return;
                            }
                            break;

                        case ValueTag.XS_DATE_TAG:
                            tvp1.getValue(tp1.datep);
                            switch (tid2) {
                                case ValueTag.XS_DATE_TAG:
                                    tvp2.getValue(tp2.datep);
                                    aOp.operateDateDate(tp1.datep, tp2.datep, dCtx, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_DAY_TIME_DURATION_TAG:
                                    tvp2.getValue(tp2.longp);
                                    aOp.operateDateDTDuration(tp1.datep, tp2.longp, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                                    tvp2.getValue(tp2.intp);
                                    aOp.operateDateYMDuration(tp1.datep, tp2.intp, dOut);
                                    result.set(abvs);
                                    return;
                            }
                            break;

                        case ValueTag.XS_DATETIME_TAG:
                            tvp1.getValue(tp1.datetimep);
                            switch (tid2) {
                                case ValueTag.XS_DATETIME_TAG:
                                    tvp2.getValue(tp2.datetimep);
                                    aOp.operateDatetimeDatetime(tp1.datetimep, tp2.datetimep, dCtx, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_DAY_TIME_DURATION_TAG:
                                    tvp2.getValue(tp2.longp);
                                    aOp.operateDatetimeDTDuration(tp1.datetimep, tp2.longp, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                                    tvp2.getValue(tp2.intp);
                                    aOp.operateDatetimeYMDuration(tp1.datetimep, tp2.intp, dOut);
                                    result.set(abvs);
                                    return;
                            }
                            break;

                        case ValueTag.XS_TIME_TAG:
                            tvp1.getValue(tp1.timep);
                            switch (tid2) {
                                case ValueTag.XS_TIME_TAG:
                                    tvp2.getValue(tp2.timep);
                                    aOp.operateTimeTime(tp1.timep, tp2.timep, dCtx, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_DAY_TIME_DURATION_TAG:
                                    tvp2.getValue(tp2.longp);
                                    aOp.operateTimeDTDuration(tp1.timep, tp2.longp, dOut);
                                    result.set(abvs);
                                    return;

                            }
                            break;

                        case ValueTag.XS_DAY_TIME_DURATION_TAG:
                            tvp1.getValue(tp1.longp);
                            switch (tid2) {
                                case ValueTag.XS_DECIMAL_TAG:
                                    tvp2.getValue(tp2.decp);
                                    aOp.operateDTDurationDecimal(tp1.longp, tp2.decp, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_INTEGER_TAG:
                                    aOp.operateDTDurationInteger(tp1.longp, longp2, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_FLOAT_TAG:
                                    tvp2.getValue(tp2.floatp);
                                    aOp.operateDTDurationFloat(tp1.longp, tp2.floatp, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_DOUBLE_TAG:
                                    tvp2.getValue(tp2.doublep);
                                    aOp.operateDTDurationDouble(tp1.longp, tp2.doublep, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_DATE_TAG:
                                    tvp2.getValue(tp2.datep);
                                    aOp.operateDTDurationDate(tp1.longp, tp2.datep, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_TIME_TAG:
                                    tvp2.getValue(tp2.timep);
                                    aOp.operateDTDurationTime(tp1.longp, tp2.timep, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_DATETIME_TAG:
                                    tvp2.getValue(tp2.datetimep);
                                    aOp.operateDTDurationDatetime(tp1.longp, tp2.datetimep, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_DAY_TIME_DURATION_TAG:
                                    tvp2.getValue(tp2.longp);
                                    aOp.operateDTDurationDTDuration(tp1.longp, tp2.longp, dOut);
                                    result.set(abvs);
                                    return;
                            }
                            break;

                        case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                            tvp1.getValue(tp1.intp);
                            switch (tid2) {
                                case ValueTag.XS_DECIMAL_TAG:
                                    tvp2.getValue(tp2.decp);
                                    aOp.operateYMDurationDecimal(tp1.intp, tp2.decp, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_INTEGER_TAG:
                                    aOp.operateYMDurationInteger(tp1.intp, longp2, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_FLOAT_TAG:
                                    tvp2.getValue(tp2.floatp);
                                    aOp.operateYMDurationFloat(tp1.intp, tp2.floatp, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_DOUBLE_TAG:
                                    tvp2.getValue(tp2.doublep);
                                    aOp.operateYMDurationDouble(tp1.intp, tp2.doublep, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_DATE_TAG:
                                    tvp2.getValue(tp2.datep);
                                    aOp.operateYMDurationDate(tp1.intp, tp2.datep, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_DATETIME_TAG:
                                    tvp2.getValue(tp2.datetimep);
                                    aOp.operateYMDurationDatetime(tp1.intp, tp2.datetimep, dOut);
                                    result.set(abvs);
                                    return;

                                case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                                    tvp2.getValue(tp2.intp);
                                    aOp.operateYMDurationYMDuration(tp1.intp, tp2.intp, dOut);
                                    result.set(abvs);
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

            private void getIntegerPointable(TypedPointables tp, TaggedValuePointable tvp, DataOutput dOutInteger)
                    throws SystemException, IOException {
                long value;
                switch (tvp.getTag()) {
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
                        value = 0;
                }
                dOutInteger.write(ValueTag.XS_INTEGER_TAG);
                dOutInteger.writeLong(value);
            }

            private int getBaseTypeForArithmetics(int tid) throws SystemException {
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

                        case ValueTag.XS_ANY_ATOMIC_TAG:
                            throw new SystemException(ErrorCode.XPTY0004);

                        default:
                            tid = BuiltinTypeRegistry.INSTANCE.getSchemaTypeById(tid).getBaseType().getTypeId();
                    }
                }
            }
        };
    }

    private static class TypedPointables {
        BytePointable bytep = (BytePointable) BytePointable.FACTORY.createPointable();
        ShortPointable shortp = (ShortPointable) ShortPointable.FACTORY.createPointable();
        IntegerPointable intp = (IntegerPointable) IntegerPointable.FACTORY.createPointable();
        LongPointable longp = (LongPointable) LongPointable.FACTORY.createPointable();
        FloatPointable floatp = (FloatPointable) FloatPointable.FACTORY.createPointable();
        DoublePointable doublep = (DoublePointable) DoublePointable.FACTORY.createPointable();
        XSDecimalPointable decp = (XSDecimalPointable) XSDecimalPointable.FACTORY.createPointable();
        XSDateTimePointable datetimep = (XSDateTimePointable) XSDateTimePointable.FACTORY.createPointable();
        XSDatePointable datep = (XSDatePointable) XSDatePointable.FACTORY.createPointable();
        XSTimePointable timep = (XSTimePointable) XSTimePointable.FACTORY.createPointable();
    }

    protected abstract AbstractArithmeticOperation createArithmeticOperation();
}