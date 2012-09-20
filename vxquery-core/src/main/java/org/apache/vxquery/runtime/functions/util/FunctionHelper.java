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
import java.io.IOException;

import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSBinaryPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDatePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDateTimePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDurationPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSQNamePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSTimePointable;
import org.apache.vxquery.datamodel.api.ITimezone;
import org.apache.vxquery.datamodel.util.DateTime;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.comparison.AbstractValueComparisonOperation;
import org.apache.vxquery.runtime.functions.strings.ICharacterIterator;
import org.apache.vxquery.runtime.functions.strings.UTF8StringCharacterIterator;
import org.apache.vxquery.types.BuiltinTypeConstants;
import org.apache.vxquery.types.BuiltinTypeRegistry;

import edu.uci.ics.hyracks.data.std.primitive.BooleanPointable;
import edu.uci.ics.hyracks.data.std.primitive.BytePointable;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.primitive.ShortPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

public class FunctionHelper {

    public static class TypedPointables {
        public BooleanPointable boolp = (BooleanPointable) BooleanPointable.FACTORY.createPointable();
        public BytePointable bytep = (BytePointable) BytePointable.FACTORY.createPointable();
        public DoublePointable doublep = (DoublePointable) DoublePointable.FACTORY.createPointable();
        public FloatPointable floatp = (FloatPointable) FloatPointable.FACTORY.createPointable();
        public IntegerPointable intp = (IntegerPointable) IntegerPointable.FACTORY.createPointable();
        public LongPointable longp = (LongPointable) LongPointable.FACTORY.createPointable();
        public ShortPointable shortp = (ShortPointable) ShortPointable.FACTORY.createPointable();
        public SequencePointable seqp = (SequencePointable) SequencePointable.FACTORY.createPointable();
        public UTF8StringPointable utf8sp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        public XSBinaryPointable binaryp = (XSBinaryPointable) XSBinaryPointable.FACTORY.createPointable();
        public XSDatePointable datep = (XSDatePointable) XSDatePointable.FACTORY.createPointable();
        public XSDateTimePointable datetimep = (XSDateTimePointable) XSDateTimePointable.FACTORY.createPointable();
        public XSDecimalPointable decp = (XSDecimalPointable) XSDecimalPointable.FACTORY.createPointable();
        public XSDurationPointable durationp = (XSDurationPointable) XSDurationPointable.FACTORY.createPointable();
        public XSTimePointable timep = (XSTimePointable) XSTimePointable.FACTORY.createPointable();
        public XSQNamePointable qnamep = (XSQNamePointable) XSQNamePointable.FACTORY.createPointable();
    }

    public static boolean compareTaggedValues(AbstractValueComparisonOperation aOp, TaggedValuePointable tvp1,
            TaggedValuePointable tvp2, DynamicContext dCtx) throws SystemException {
        final TypedPointables tp1 = new TypedPointables();
        final TypedPointables tp2 = new TypedPointables();

        boolean booleanResult = false;
        int tid1 = getBaseTypeForComparisons(tvp1.getTag());
        int tid2 = getBaseTypeForComparisons(tvp2.getTag());
        try {
            switch (tid1) {
                case ValueTag.XS_DECIMAL_TAG:
                    tvp1.getValue(tp1.decp);
                    switch (tid2) {
                        case ValueTag.XS_DECIMAL_TAG:
                            tvp2.getValue(tp2.decp);
                            booleanResult = aOp.operateDecimalDecimal(tp1.decp, tp2.decp);
                            break;

                        case ValueTag.XS_INTEGER_TAG:
                            tvp2.getValue(tp2.longp);
                            booleanResult = aOp.operateDecimalInteger(tp1.decp, tp2.longp);
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
                    tvp1.getValue(tp1.longp);
                    switch (tid2) {
                        case ValueTag.XS_DECIMAL_TAG:
                            tvp2.getValue(tp2.decp);
                            booleanResult = aOp.operateIntegerDecimal(tp1.longp, tp2.decp);
                            break;

                        case ValueTag.XS_INTEGER_TAG:
                            tvp2.getValue(tp2.longp);
                            booleanResult = aOp.operateIntegerInteger(tp1.longp, tp2.longp);
                            break;

                        case ValueTag.XS_FLOAT_TAG:
                            tvp2.getValue(tp2.floatp);
                            booleanResult = aOp.operateIntegerFloat(tp1.longp, tp2.floatp);
                            break;

                        case ValueTag.XS_DOUBLE_TAG:
                            tvp2.getValue(tp2.doublep);
                            booleanResult = aOp.operateIntegerDouble(tp1.longp, tp2.doublep);
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
                            tvp2.getValue(tp2.longp);
                            booleanResult = aOp.operateFloatInteger(tp1.floatp, tp2.longp);
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
                            tvp2.getValue(tp2.longp);
                            booleanResult = aOp.operateDoubleInteger(tp1.doublep, tp2.longp);
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
                case ValueTag.XS_NORMALIZED_STRING_TAG:
                case ValueTag.XS_TOKEN_TAG:
                case ValueTag.XS_LANGUAGE_TAG:
                case ValueTag.XS_NMTOKEN_TAG:
                case ValueTag.XS_NAME_TAG:
                case ValueTag.XS_NCNAME_TAG:
                case ValueTag.XS_ID_TAG:
                case ValueTag.XS_IDREF_TAG:
                case ValueTag.XS_ENTITY_TAG:
                case ValueTag.XS_UNTYPED_ATOMIC_TAG:
                    tvp1.getValue(tp1.utf8sp);
                    switch (tid2) {
                        case ValueTag.XS_STRING_TAG:
                        case ValueTag.XS_NORMALIZED_STRING_TAG:
                        case ValueTag.XS_TOKEN_TAG:
                        case ValueTag.XS_LANGUAGE_TAG:
                        case ValueTag.XS_NMTOKEN_TAG:
                        case ValueTag.XS_NAME_TAG:
                        case ValueTag.XS_NCNAME_TAG:
                        case ValueTag.XS_ID_TAG:
                        case ValueTag.XS_IDREF_TAG:
                        case ValueTag.XS_ENTITY_TAG:
                        case ValueTag.XS_UNTYPED_ATOMIC_TAG:
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
                        case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                            tvp2.getValue(tp2.intp);
                            booleanResult = aOp.operateDTDurationYMDuration(tp1.longp, tp2.intp);
                            break;
                    }
                    break;

                case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                    tvp1.getValue(tp1.intp);
                    switch (tid2) {
                        case ValueTag.XS_DAY_TIME_DURATION_TAG:
                            tvp2.getValue(tp2.longp);
                            booleanResult = aOp.operateYMDurationDTDuration(tp1.intp, tp2.longp);
                            break;
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
            return booleanResult;

        } catch (SystemException se) {
            throw se;
        } catch (Exception e) {
            throw new SystemException(ErrorCode.SYSE0001, e);
        }
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

    public static int getBaseTypeForComparisons(int tid) throws SystemException {
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
            }
        }
    }

    public static int getBaseTypeForGeneralComparisons(int tid) throws SystemException {
        while (true) {
            switch (tid) {
                case ValueTag.XS_ANY_URI_TAG:
                case ValueTag.XS_BASE64_BINARY_TAG:
                case ValueTag.XS_BOOLEAN_TAG:
                case ValueTag.XS_DATE_TAG:
                case ValueTag.XS_DATETIME_TAG:
                case ValueTag.XS_DAY_TIME_DURATION_TAG:
                case ValueTag.XS_DURATION_TAG:
                case ValueTag.XS_HEX_BINARY_TAG:
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
                case ValueTag.XS_DECIMAL_TAG:
                case ValueTag.XS_DOUBLE_TAG:
                case ValueTag.XS_FLOAT_TAG:
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
                    return ValueTag.XS_DOUBLE_TAG;

                case ValueTag.XS_ANY_ATOMIC_TAG:
                    throw new SystemException(ErrorCode.XPTY0004);

                default:
                    tid = BuiltinTypeRegistry.INSTANCE.getSchemaTypeById(tid).getBaseType().getTypeId();
            }
        }
    }

    public static void getDoublePointable(TaggedValuePointable tvp, DataOutput dOut) throws SystemException,
            IOException {
        TypedPointables tp = new TypedPointables();
        double value;
        switch (tvp.getTag()) {
            case ValueTag.XS_DECIMAL_TAG:
                tvp.getValue(tp.decp);
                value = tp.decp.doubleValue();
                break;

            case ValueTag.XS_DOUBLE_TAG:
                tvp.getValue(tp.doublep);
                value = tp.doublep.doubleValue();
                break;

            case ValueTag.XS_FLOAT_TAG:
                tvp.getValue(tp.floatp);
                value = tp.floatp.doubleValue();
                break;

            case ValueTag.XS_INTEGER_TAG:
            case ValueTag.XS_LONG_TAG:
            case ValueTag.XS_NEGATIVE_INTEGER_TAG:
            case ValueTag.XS_NON_POSITIVE_INTEGER_TAG:
            case ValueTag.XS_NON_NEGATIVE_INTEGER_TAG:
            case ValueTag.XS_POSITIVE_INTEGER_TAG:
            case ValueTag.XS_UNSIGNED_INT_TAG:
            case ValueTag.XS_UNSIGNED_LONG_TAG:
                tvp.getValue(tp.longp);
                value = tp.longp.doubleValue();
                break;

            case ValueTag.XS_INT_TAG:
            case ValueTag.XS_UNSIGNED_SHORT_TAG:
                tvp.getValue(tp.intp);
                value = tp.intp.doubleValue();
                break;

            case ValueTag.XS_SHORT_TAG:
            case ValueTag.XS_UNSIGNED_BYTE_TAG:
                tvp.getValue(tp.shortp);
                value = tp.shortp.doubleValue();
                break;

            case ValueTag.XS_BYTE_TAG:
                tvp.getValue(tp.bytep);
                value = tp.bytep.doubleValue();
                break;

            default:
                value = 0;
        }
        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.writeDouble(value);
    }

    public static void getIntegerPointable(TaggedValuePointable tvp, DataOutput dOut) throws SystemException,
            IOException {
        TypedPointables tp = new TypedPointables();
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

    public static long getTimezone(ITimezone timezonep) {
        return timezonep.getTimezoneHour() * DateTime.CHRONON_OF_HOUR + timezonep.getTimezoneMinute()
                * DateTime.CHRONON_OF_MINUTE;
    }

    public static boolean isDerivedFromDouble(int tid) {
        switch (tid) {
            case ValueTag.XS_DECIMAL_TAG:
            case ValueTag.XS_DOUBLE_TAG:
            case ValueTag.XS_FLOAT_TAG:
                return true;
        }
        return isDerivedFromInteger(tid);
    }

    public static boolean isDerivedFromInteger(int tid) {
        switch (tid) {
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
                return true;
        }
        return false;
    }

    public static boolean isDerivedFromString(int tid) {
        switch (tid) {
            case ValueTag.XS_STRING_TAG:
            case ValueTag.XS_NORMALIZED_STRING_TAG:
            case ValueTag.XS_TOKEN_TAG:
            case ValueTag.XS_LANGUAGE_TAG:
            case ValueTag.XS_NMTOKEN_TAG:
            case ValueTag.XS_NAME_TAG:
            case ValueTag.XS_NCNAME_TAG:
            case ValueTag.XS_ID_TAG:
            case ValueTag.XS_IDREF_TAG:
            case ValueTag.XS_ENTITY_TAG:
                return true;
        }
        return false;
    }

    public static void printUTF8String(UTF8StringPointable stringp) {
        System.err.println(" printUTF8String START length = " + stringp.getUTFLength());
        ICharacterIterator charIterator = new UTF8StringCharacterIterator(stringp);
        charIterator.reset();
        for (int c = charIterator.next(); c != ICharacterIterator.EOS_CHAR; c = charIterator.next()) {
            System.err.println("   parse value '" + c + "' as '" + Character.valueOf((char) c) + "'");
        }
        System.err.println(" printUTF8String END");
    }
}
