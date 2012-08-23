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
package org.apache.vxquery.runtime.functions.comparison;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSBinaryPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDatePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDateTimePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDurationPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSQNamePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSTimePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.types.BuiltinTypeRegistry;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.BooleanPointable;
import edu.uci.ics.hyracks.data.std.primitive.BytePointable;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.primitive.ShortPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public abstract class AbstractValueComparisonScalarEvaluatorFactory extends
        AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public AbstractValueComparisonScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final AbstractValueComparisonOperation aOp = createValueComparisonOperation();
        final ArrayBackedValueStorage abvsInteger1 = new ArrayBackedValueStorage();
        final DataOutput dOutInteger1 = abvsInteger1.getDataOutput();
        final ArrayBackedValueStorage abvsInteger2 = new ArrayBackedValueStorage();
        final DataOutput dOutInteger2 = abvsInteger2.getDataOutput();
        final TypedPointables tp1 = new TypedPointables();
        final TypedPointables tp2 = new TypedPointables();
        final DynamicContext dCtx = (DynamicContext) ctx.getJobletContext().getGlobalJobData();

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                boolean booleanResult = false;
                TaggedValuePointable tvp1 = args[0];
                TaggedValuePointable tvp2 = args[1];
                int tid1 = getBaseTypeForComparisons(tvp1.getTag());
                int tid2 = getBaseTypeForComparisons(tvp2.getTag());
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
                                    booleanResult = aOp.operateDecimalDecimal(tp1.decp, tp2.decp);
                                    break;

                                case ValueTag.XS_INTEGER_TAG:
                                    booleanResult = aOp.operateDecimalInteger(tp1.decp, longp2);
                                    break;

                                case ValueTag.XS_FLOAT_TAG:
                                    tvp2.getValue(tp2.floatp);
                                    booleanResult = aOp.operateDecimalFloat(tp1.decp, tp2.floatp);
                                    break;

                                case ValueTag.XS_DOUBLE_TAG:
                                    tvp2.getValue(tp2.doublep);
                                    booleanResult = aOp.operateDecimalDouble(tp1.decp, tp2.doublep);
                                    break;
                            }
                            break;

                        case ValueTag.XS_INTEGER_TAG:
                            switch (tid2) {
                                case ValueTag.XS_DECIMAL_TAG:
                                    tvp2.getValue(tp2.decp);
                                    booleanResult = aOp.operateIntegerDecimal(longp1, tp2.decp);
                                    break;

                                case ValueTag.XS_INTEGER_TAG:
                                    booleanResult = aOp.operateIntegerInteger(longp1, longp2);
                                    break;

                                case ValueTag.XS_FLOAT_TAG:
                                    tvp2.getValue(tp2.floatp);
                                    booleanResult = aOp.operateIntegerFloat(longp1, tp2.floatp);
                                    break;

                                case ValueTag.XS_DOUBLE_TAG:
                                    tvp2.getValue(tp2.doublep);
                                    booleanResult = aOp.operateIntegerDouble(longp1, tp2.doublep);
                                    break;
                            }
                            break;

                        case ValueTag.XS_FLOAT_TAG:
                            tvp1.getValue(tp1.floatp);
                            switch (tid2) {
                                case ValueTag.XS_DECIMAL_TAG:
                                    tvp2.getValue(tp2.decp);
                                    booleanResult = aOp.operateFloatDecimal(tp1.floatp, tp2.decp);
                                    break;

                                case ValueTag.XS_INTEGER_TAG:
                                    booleanResult = aOp.operateFloatInteger(tp1.floatp, longp2);
                                    break;

                                case ValueTag.XS_FLOAT_TAG:
                                    tvp2.getValue(tp2.floatp);
                                    booleanResult = aOp.operateFloatFloat(tp1.floatp, tp2.floatp);
                                    break;

                                case ValueTag.XS_DOUBLE_TAG:
                                    tvp2.getValue(tp2.doublep);
                                    booleanResult = aOp.operateFloatDouble(tp1.floatp, tp2.doublep);
                                    break;
                            }
                            break;

                        case ValueTag.XS_DOUBLE_TAG:
                            tvp1.getValue(tp1.doublep);
                            switch (tid2) {
                                case ValueTag.XS_DECIMAL_TAG:
                                    tvp2.getValue(tp2.decp);
                                    booleanResult = aOp.operateDoubleDecimal(tp1.doublep, tp2.decp);
                                    break;

                                case ValueTag.XS_INTEGER_TAG:
                                    booleanResult = aOp.operateDoubleInteger(tp1.doublep, longp2);
                                    break;

                                case ValueTag.XS_FLOAT_TAG:
                                    tvp2.getValue(tp2.floatp);
                                    booleanResult = aOp.operateDoubleFloat(tp1.doublep, tp2.floatp);
                                    break;

                                case ValueTag.XS_DOUBLE_TAG:
                                    tvp2.getValue(tp2.doublep);
                                    booleanResult = aOp.operateDoubleDouble(tp1.doublep, tp2.doublep);
                                    break;
                            }
                            break;

                        case ValueTag.XS_BOOLEAN_TAG:
                            tvp1.getValue(tp1.boolp);
                            switch (tid2) {
                                case ValueTag.XS_BOOLEAN_TAG:
                                    tvp2.getValue(tp2.boolp);
                                    booleanResult = aOp.operateBooleanBoolean(tp1.boolp, tp2.boolp);
                                    break;
                            }
                            break;

                        case ValueTag.XS_STRING_TAG:
                            tvp1.getValue(tp1.utf8sp);
                            switch (tid2) {
                                case ValueTag.XS_STRING_TAG:
                                    tvp2.getValue(tp2.utf8sp);
                                    booleanResult = aOp.operateStringString(tp1.utf8sp, tp2.utf8sp);
                                    break;
                            }
                            break;

                        case ValueTag.XS_DATE_TAG:
                            tvp1.getValue(tp1.datep);
                            switch (tid2) {
                                case ValueTag.XS_DATE_TAG:
                                    tvp2.getValue(tp2.datep);
                                    booleanResult = aOp.operateDateDate(tp1.datep, tp2.datep, dCtx);
                                    break;
                                default:
                                    // Cross comparisons between DateTime, Date and Time are not supported.
                                    throw new SystemException(ErrorCode.XPTY0004);
                            }
                            break;

                        case ValueTag.XS_DATETIME_TAG:
                            tvp1.getValue(tp1.datetimep);
                            switch (tid2) {
                                case ValueTag.XS_DATETIME_TAG:
                                    tvp2.getValue(tp2.datetimep);
                                    booleanResult = aOp.operateDatetimeDatetime(tp1.datetimep, tp2.datetimep, dCtx);
                                    break;
                                default:
                                    // Cross comparisons between DateTime, Date and Time are not supported.
                                    throw new SystemException(ErrorCode.XPTY0004);
                            }
                            break;

                        case ValueTag.XS_TIME_TAG:
                            tvp1.getValue(tp1.timep);
                            switch (tid2) {
                                case ValueTag.XS_TIME_TAG:
                                    tvp2.getValue(tp2.timep);
                                    booleanResult = aOp.operateTimeTime(tp1.timep, tp2.timep, dCtx);
                                    break;
                                default:
                                    // Cross comparisons between DateTime, Date and Time are not supported.
                                    throw new SystemException(ErrorCode.XPTY0004);
                            }
                            break;

                        case ValueTag.XS_DURATION_TAG:
                            tvp1.getValue(tp1.durationp);
                            switch (tid2) {
                                case ValueTag.XS_DAY_TIME_DURATION_TAG:
                                    tvp2.getValue(tp2.longp);
                                    booleanResult = aOp.operateDurationDTDuration(tp1.durationp, tp2.longp);
                                    break;
                                case ValueTag.XS_DURATION_TAG:
                                    tvp2.getValue(tp2.durationp);
                                    booleanResult = aOp.operateDurationDuration(tp1.durationp, tp2.durationp);
                                    break;
                                case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                                    tvp2.getValue(tp2.intp);
                                    booleanResult = aOp.operateDurationYMDuration(tp1.durationp, tp2.intp);
                                    break;
                            }
                            break;

                        case ValueTag.XS_DAY_TIME_DURATION_TAG:
                            tvp1.getValue(tp1.longp);
                            switch (tid2) {
                                case ValueTag.XS_DAY_TIME_DURATION_TAG:
                                    tvp2.getValue(tp2.longp);
                                    booleanResult = aOp.operateDTDurationDTDuration(tp1.longp, tp2.longp);
                                    break;
                                case ValueTag.XS_DURATION_TAG:
                                    tvp2.getValue(tp2.durationp);
                                    booleanResult = aOp.operateDTDurationDuration(tp1.longp, tp2.durationp);
                                    break;
                            }
                            break;

                        case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                            tvp1.getValue(tp1.intp);
                            switch (tid2) {
                                case ValueTag.XS_DURATION_TAG:
                                    tvp2.getValue(tp2.durationp);
                                    booleanResult = aOp.operateYMDurationDuration(tp1.intp, tp2.durationp);
                                    break;
                                case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                                    tvp2.getValue(tp2.intp);
                                    booleanResult = aOp.operateYMDurationYMDuration(tp1.intp, tp2.intp);
                                    break;
                            }
                            break;

                        case ValueTag.XS_G_DAY_TAG:
                            tvp1.getValue(tp1.datep);
                            switch (tid2) {
                                case ValueTag.XS_G_DAY_TAG:
                                    tvp2.getValue(tp2.datep);
                                    booleanResult = aOp.operateGDayGDay(tp1.datep, tp2.datep, dCtx);
                                    break;
                            }
                            break;

                        case ValueTag.XS_G_MONTH_DAY_TAG:
                            tvp1.getValue(tp1.datep);
                            switch (tid2) {
                                case ValueTag.XS_G_MONTH_DAY_TAG:
                                    tvp2.getValue(tp2.datep);
                                    booleanResult = aOp.operateGMonthDayGMonthDay(tp1.datep, tp2.datep, dCtx);
                                    break;
                            }
                            break;

                        case ValueTag.XS_G_MONTH_TAG:
                            tvp1.getValue(tp1.datep);
                            switch (tid2) {
                                case ValueTag.XS_G_MONTH_TAG:
                                    tvp2.getValue(tp2.datep);
                                    booleanResult = aOp.operateGMonthGMonth(tp1.datep, tp2.datep, dCtx);
                                    break;
                            }
                            break;

                        case ValueTag.XS_G_YEAR_MONTH_TAG:
                            tvp1.getValue(tp1.datep);
                            switch (tid2) {
                                case ValueTag.XS_G_YEAR_MONTH_TAG:
                                    tvp2.getValue(tp2.datep);
                                    booleanResult = aOp.operateGYearMonthGYearMonth(tp1.datep, tp2.datep, dCtx);
                                    break;
                            }
                            break;

                        case ValueTag.XS_G_YEAR_TAG:
                            tvp1.getValue(tp1.datep);
                            switch (tid2) {
                                case ValueTag.XS_G_YEAR_TAG:
                                    tvp2.getValue(tp2.datep);
                                    booleanResult = aOp.operateGYearGYear(tp1.datep, tp2.datep, dCtx);
                                    break;
                            }
                            break;

                        case ValueTag.XS_HEX_BINARY_TAG:
                            tvp1.getValue(tp1.binaryp);
                            switch (tid2) {
                                case ValueTag.XS_HEX_BINARY_TAG:
                                    tvp2.getValue(tp2.binaryp);
                                    booleanResult = aOp.operateHexBinaryHexBinary(tp1.binaryp, tp2.binaryp);
                                    break;
                            }
                            break;

                        case ValueTag.XS_BASE64_BINARY_TAG:
                            tvp1.getValue(tp1.binaryp);
                            switch (tid2) {
                                case ValueTag.XS_BASE64_BINARY_TAG:
                                    tvp2.getValue(tp2.binaryp);
                                    booleanResult = aOp.operateBase64BinaryBase64Binary(tp1.binaryp, tp2.binaryp);
                                    break;
                            }
                            break;

                        case ValueTag.XS_ANY_URI_TAG:
                            tvp1.getValue(tp1.utf8sp);
                            switch (tid2) {
                                case ValueTag.XS_ANY_URI_TAG:
                                    tvp2.getValue(tp2.utf8sp);
                                    booleanResult = aOp.operateAnyURIAnyURI(tp1.utf8sp, tp2.utf8sp);
                                    break;
                            }
                            break;

                        case ValueTag.XS_QNAME_TAG:
                            tvp1.getValue(tp1.qnamep);
                            switch (tid2) {
                                case ValueTag.XS_QNAME_TAG:
                                    tvp2.getValue(tp2.qnamep);
                                    booleanResult = aOp.operateQNameQName(tp1.qnamep, tp2.qnamep);
                                    break;
                            }
                            break;

                        case ValueTag.XS_NOTATION_TAG:
                            tvp1.getValue(tp1.utf8sp);
                            switch (tid2) {
                                case ValueTag.XS_NOTATION_TAG:
                                    tvp2.getValue(tp2.utf8sp);
                                    booleanResult = aOp.operateNotationNotation(tp1.utf8sp, tp2.utf8sp);
                                    break;
                            }
                            break;
                    }

                    byte[] byteResult = new byte[2];
                    byteResult[0] = ValueTag.XS_BOOLEAN_TAG;
                    byteResult[1] = (byte) (booleanResult ? 1 : 0);
                    result.set(byteResult, 0, 2);
                } catch (SystemException se) {
                    throw se;
                } catch (Exception e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
            }

            private void getIntegerPointable(TypedPointables tp, TaggedValuePointable tvp, DataOutput dOut)
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
                dOut.write(ValueTag.XS_INTEGER_TAG);
                dOut.writeLong(value);
            }

            private int getBaseTypeForComparisons(int tid) throws SystemException {
                while (true) {
                    switch (tid) {
                        case ValueTag.XS_ANY_URI_TAG:
                        case ValueTag.XS_BASE64_BINARY_TAG:
                        case ValueTag.XS_BOOLEAN_TAG:
                        case ValueTag.XS_DATE_TAG:
                        case ValueTag.XS_DATETIME_TAG:
                        case ValueTag.XS_DAY_TIME_DURATION_TAG:
                        case ValueTag.XS_DECIMAL_TAG:
                        case ValueTag.XS_DOUBLE_TAG:
                        case ValueTag.XS_DURATION_TAG:
                        case ValueTag.XS_FLOAT_TAG:
                        case ValueTag.XS_HEX_BINARY_TAG:
                        case ValueTag.XS_INTEGER_TAG:
                        case ValueTag.XS_G_DAY_TAG:
                        case ValueTag.XS_G_MONTH_DAY_TAG:
                        case ValueTag.XS_G_MONTH_TAG:
                        case ValueTag.XS_G_YEAR_MONTH_TAG:
                        case ValueTag.XS_G_YEAR_TAG:
                        case ValueTag.XS_QNAME_TAG:
                        case ValueTag.XS_STRING_TAG:
                        case ValueTag.XS_TIME_TAG:
                        case ValueTag.XS_UNTYPED_ATOMIC_TAG:
                        case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
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
        BooleanPointable boolp = (BooleanPointable) BooleanPointable.FACTORY.createPointable();
        BytePointable bytep = (BytePointable) BytePointable.FACTORY.createPointable();
        DoublePointable doublep = (DoublePointable) DoublePointable.FACTORY.createPointable();
        FloatPointable floatp = (FloatPointable) FloatPointable.FACTORY.createPointable();
        IntegerPointable intp = (IntegerPointable) IntegerPointable.FACTORY.createPointable();
        LongPointable longp = (LongPointable) LongPointable.FACTORY.createPointable();
        ShortPointable shortp = (ShortPointable) ShortPointable.FACTORY.createPointable();
        UTF8StringPointable utf8sp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        XSBinaryPointable binaryp = (XSBinaryPointable) XSBinaryPointable.FACTORY.createPointable();
        XSDatePointable datep = (XSDatePointable) XSDatePointable.FACTORY.createPointable();
        XSDateTimePointable datetimep = (XSDateTimePointable) XSDateTimePointable.FACTORY.createPointable();
        XSDecimalPointable decp = (XSDecimalPointable) XSDecimalPointable.FACTORY.createPointable();
        XSDurationPointable durationp = (XSDurationPointable) XSDurationPointable.FACTORY.createPointable();
        XSTimePointable timep = (XSTimePointable) XSTimePointable.FACTORY.createPointable();
        XSQNamePointable qnamep = (XSQNamePointable) XSQNamePointable.FACTORY.createPointable();
    }

    protected abstract AbstractValueComparisonOperation createValueComparisonOperation();
}