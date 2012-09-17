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

    public static void getIntegerPointable(TypedPointables tp, TaggedValuePointable tvp, DataOutput dOut)
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
}
