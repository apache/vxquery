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
import org.apache.vxquery.datamodel.util.DateTime;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;
import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class ValueEqComparisonOperation extends AbstractValueComparisonOperation {
    protected final ArrayBackedValueStorage abvsInner = new ArrayBackedValueStorage();
    protected final DataOutput dOutInner = abvsInner.getDataOutput();
    private XSDateTimePointable ctxDatetimep = (XSDateTimePointable) XSDateTimePointable.FACTORY.createPointable();

    @Override
    public boolean operateAnyURIAnyURI(UTF8StringPointable stringp1, UTF8StringPointable stringp2)
            throws SystemException, IOException {
        return (stringp1.compareTo(stringp2) == 0);
    }

    @Override
    public boolean operateBase64BinaryBase64Binary(XSBinaryPointable binaryp1, XSBinaryPointable binaryp2)
            throws SystemException, IOException {
        return FunctionHelper.arraysEqual(binaryp1, binaryp2);
    }

    @Override
    public boolean operateBooleanBoolean(BooleanPointable boolp1, BooleanPointable boolp2)
            throws SystemException, IOException {
        return (boolp1.compareTo(boolp2) == 0);
    }

    @Override
    public boolean operateDateDate(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx)
            throws SystemException, IOException {
        abvsInner.reset();
        dCtx.getCurrentDateTime(ctxDatetimep);
        DateTime.getUtcTimezoneDateTime(datep1, ctxDatetimep, dOutInner);
        DateTime.getUtcTimezoneDateTime(datep2, ctxDatetimep, dOutInner);
        int startOffset1 = abvsInner.getStartOffset() + 1;
        int startOffset2 = startOffset1 + 1 + XSDateTimePointable.TYPE_TRAITS.getFixedLength();
        if (XSDateTimePointable.getYearMonth(abvsInner.getByteArray(), startOffset1) == XSDateTimePointable
                .getYearMonth(abvsInner.getByteArray(), startOffset2)
                && XSDateTimePointable.getDay(abvsInner.getByteArray(), startOffset1) == XSDateTimePointable
                        .getDay(abvsInner.getByteArray(), startOffset2)) {
            return true;
        }
        return false;
    }

    @Override
    public boolean operateDatetimeDatetime(XSDateTimePointable datetimep1, XSDateTimePointable datetimep2,
            DynamicContext dCtx) throws SystemException, IOException {
        abvsInner.reset();
        dCtx.getCurrentDateTime(ctxDatetimep);
        DateTime.getUtcTimezoneDateTime(datetimep1, ctxDatetimep, dOutInner);
        DateTime.getUtcTimezoneDateTime(datetimep2, ctxDatetimep, dOutInner);
        int startOffset1 = abvsInner.getStartOffset() + 1;
        int startOffset2 = startOffset1 + 1 + XSDateTimePointable.TYPE_TRAITS.getFixedLength();
        if (XSDateTimePointable.getYearMonth(abvsInner.getByteArray(), startOffset1) == XSDateTimePointable
                .getYearMonth(abvsInner.getByteArray(), startOffset2)
                && XSDateTimePointable.getDayTime(abvsInner.getByteArray(), startOffset1) == XSDateTimePointable
                        .getDayTime(abvsInner.getByteArray(), startOffset2)) {
            return true;
        }
        return false;
    }

    @Override
    public boolean operateDecimalDecimal(XSDecimalPointable decp1, XSDecimalPointable decp2)
            throws SystemException, IOException {
        return (decp1.compareTo(decp2) == 0);
    }

    @Override
    public boolean operateDecimalDouble(XSDecimalPointable decp1, DoublePointable doublep2)
            throws SystemException, IOException {
        double double1 = decp1.doubleValue();
        double double2 = doublep2.doubleValue();
        if (Double.isNaN(doublep2.getDouble())) {
            return false;
        }
        return (double1 == double2);
    }

    @Override
    public boolean operateDecimalFloat(XSDecimalPointable decp1, FloatPointable floatp2)
            throws SystemException, IOException {
        float float1 = decp1.floatValue();
        float float2 = floatp2.floatValue();
        if (Float.isNaN(floatp2.getFloat())) {
            return false;
        }
        return (float1 == float2);
    }

    @Override
    public boolean operateDecimalInteger(XSDecimalPointable decp1, LongPointable longp2)
            throws SystemException, IOException {
        abvsInner.reset();
        XSDecimalPointable decp2 = (XSDecimalPointable) XSDecimalPointable.FACTORY.createPointable();
        decp2.set(abvsInner.getByteArray(), abvsInner.getStartOffset(),
                XSDecimalPointable.TYPE_TRAITS.getFixedLength());
        decp2.setDecimal(longp2.getLong(), (byte) 0);
        return (decp1.compareTo(decp2) == 0);
    }

    @Override
    public boolean operateDoubleDecimal(DoublePointable doublep1, XSDecimalPointable decp2)
            throws SystemException, IOException {
        double double1 = doublep1.doubleValue();
        double double2 = decp2.doubleValue();
        if (Double.isNaN(doublep1.getDouble())) {
            return false;
        }
        return (double1 == double2);
    }

    @Override
    public boolean operateDoubleDouble(DoublePointable doublep1, DoublePointable doublep2)
            throws SystemException, IOException {
        if (Double.isNaN(doublep1.getDouble()) || Double.isNaN(doublep2.getDouble())) {
            return false;
        }
        return (doublep1.compareTo(doublep2) == 0);
    }

    @Override
    public boolean operateDoubleFloat(DoublePointable doublep1, FloatPointable floatp2)
            throws SystemException, IOException {
        double double1 = doublep1.doubleValue();
        double double2 = floatp2.doubleValue();
        if (Double.isNaN(doublep1.getDouble()) || Float.isNaN(floatp2.getFloat())) {
            return false;
        }
        return (double1 == double2);
    }

    @Override
    public boolean operateDoubleInteger(DoublePointable doublep1, LongPointable longp2)
            throws SystemException, IOException {
        double double1 = doublep1.doubleValue();
        double double2 = longp2.doubleValue();
        if (Double.isNaN(doublep1.getDouble())) {
            return false;
        }
        return (double1 == double2);
    }

    @Override
    public boolean operateDTDurationDTDuration(LongPointable longp1, LongPointable longp2)
            throws SystemException, IOException {
        return (longp1.compareTo(longp2) == 0);
    }

    @Override
    public boolean operateDTDurationDuration(LongPointable longp1, XSDurationPointable durationp2)
            throws SystemException, IOException {
        if (durationp2.getYearMonth() == 0 && durationp2.getDayTime() == longp1.getLong()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean operateDTDurationYMDuration(LongPointable longp1, IntegerPointable intp2)
            throws SystemException, IOException {
        if (longp1.getLong() == 0 && intp2.getInteger() == 0) {
            return true;
        }
        return false;
    }

    @Override
    public boolean operateDurationDTDuration(XSDurationPointable durationp1, LongPointable longp2)
            throws SystemException, IOException {
        if (durationp1.getYearMonth() == 0 && durationp1.getDayTime() == longp2.getLong()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean operateDurationDuration(XSDurationPointable durationp1, XSDurationPointable durationp2)
            throws SystemException, IOException {
        return FunctionHelper.arraysEqual(durationp1, durationp2);
    }

    @Override
    public boolean operateDurationYMDuration(XSDurationPointable durationp1, IntegerPointable intp2)
            throws SystemException, IOException {
        if (durationp1.getYearMonth() == intp2.getInteger() && durationp1.getDayTime() == 0) {
            return true;
        }
        return false;
    }

    @Override
    public boolean operateFloatDecimal(FloatPointable floatp1, XSDecimalPointable decp2)
            throws SystemException, IOException {
        float float1 = floatp1.floatValue();
        float float2 = decp2.floatValue();
        if (Float.isNaN(floatp1.getFloat())) {
            return false;
        }
        return (float1 == float2);
    }

    @Override
    public boolean operateFloatDouble(FloatPointable floatp1, DoublePointable doublep2)
            throws SystemException, IOException {
        double double1 = floatp1.doubleValue();
        double double2 = doublep2.doubleValue();
        if (Float.isNaN(floatp1.getFloat()) || Double.isNaN(double2)) {
            return false;
        }
        return (double1 == double2);
    }

    @Override
    public boolean operateFloatFloat(FloatPointable floatp1, FloatPointable floatp2)
            throws SystemException, IOException {
        if (Float.isNaN(floatp1.getFloat()) || Float.isNaN(floatp2.getFloat())) {
            return false;
        }
        return (floatp1.compareTo(floatp2) == 0);
    }

    @Override
    public boolean operateFloatInteger(FloatPointable floatp1, LongPointable longp2)
            throws SystemException, IOException {
        float float1 = floatp1.floatValue();
        float float2 = longp2.floatValue();
        if (Float.isNaN(floatp1.getFloat())) {
            return false;
        }
        return (float1 == float2);
    }

    @Override
    public boolean operateGDayGDay(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx)
            throws SystemException, IOException {
        return operateDateDate(datep1, datep2, dCtx);
    }

    @Override
    public boolean operateGMonthDayGMonthDay(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx)
            throws SystemException, IOException {
        return operateDateDate(datep1, datep2, dCtx);
    }

    @Override
    public boolean operateGMonthGMonth(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx)
            throws SystemException, IOException {
        return operateDateDate(datep1, datep2, dCtx);
    }

    @Override
    public boolean operateGYearGYear(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx)
            throws SystemException, IOException {
        return operateDateDate(datep1, datep2, dCtx);
    }

    @Override
    public boolean operateGYearMonthGYearMonth(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx)
            throws SystemException, IOException {
        return operateDateDate(datep1, datep2, dCtx);
    }

    @Override
    public boolean operateHexBinaryHexBinary(XSBinaryPointable binaryp1, XSBinaryPointable binaryp2)
            throws SystemException, IOException {
        return FunctionHelper.arraysEqual(binaryp1, binaryp2);
    }

    @Override
    public boolean operateIntegerDecimal(LongPointable longp1, XSDecimalPointable decp2)
            throws SystemException, IOException {
        abvsInner.reset();
        XSDecimalPointable decp1 = (XSDecimalPointable) XSDecimalPointable.FACTORY.createPointable();
        decp1.set(abvsInner.getByteArray(), abvsInner.getStartOffset(),
                XSDecimalPointable.TYPE_TRAITS.getFixedLength());
        decp1.setDecimal(longp1.getLong(), (byte) 0);
        return (decp1.compareTo(decp2) == 0);
    }

    @Override
    public boolean operateIntegerDouble(LongPointable longp1, DoublePointable doublep2)
            throws SystemException, IOException {
        double double1 = longp1.doubleValue();
        double double2 = doublep2.doubleValue();
        if (Double.isNaN(double2)) {
            return false;
        }
        return (double1 == double2);
    }

    @Override
    public boolean operateIntegerFloat(LongPointable longp1, FloatPointable floatp2)
            throws SystemException, IOException {
        float float1 = longp1.floatValue();
        float float2 = floatp2.floatValue();
        if (Float.isNaN(float2)) {
            return false;
        }
        return (float1 == float2);
    }

    @Override
    public boolean operateIntegerInteger(LongPointable longp1, LongPointable longp2)
            throws SystemException, IOException {
        return (longp1.compareTo(longp2) == 0);
    }

    @Override
    public boolean operateNotationNotation(UTF8StringPointable stringp1, UTF8StringPointable stringp2)
            throws SystemException, IOException {
        return (stringp1.compareTo(stringp2) == 0);
    }

    @Override
    public boolean operateQNameQName(XSQNamePointable qnamep1, XSQNamePointable qnamep2)
            throws SystemException, IOException {
        int startOffsetLocalName1 = qnamep1.getStartOffset() + qnamep1.getUriLength() + qnamep1.getPrefixLength();
        int startOffsetLocalName2 = qnamep2.getStartOffset() + qnamep2.getUriLength() + qnamep2.getPrefixLength();
        // Only compare URI and LocalName.
        return FunctionHelper.arraysEqual(qnamep1.getByteArray(), qnamep1.getStartOffset(), qnamep1.getUriLength(),
                qnamep2.getByteArray(), qnamep2.getStartOffset(), qnamep2.getUriLength())
                && FunctionHelper.arraysEqual(qnamep1.getByteArray(), startOffsetLocalName1,
                        qnamep1.getLocalNameLength(), qnamep2.getByteArray(), startOffsetLocalName2,
                        qnamep2.getLocalNameLength());
    }

    @Override
    public boolean operateStringString(UTF8StringPointable stringp1, UTF8StringPointable stringp2)
            throws SystemException, IOException {
        return (stringp1.compareTo(stringp2) == 0);
    }

    @Override
    public boolean operateTimeTime(XSTimePointable timep1, XSTimePointable timep2, DynamicContext dCtx)
            throws SystemException, IOException {
        abvsInner.reset();
        dCtx.getCurrentDateTime(ctxDatetimep);
        DateTime.getUtcTimezoneDateTime(timep1, ctxDatetimep, dOutInner);
        DateTime.getUtcTimezoneDateTime(timep2, ctxDatetimep, dOutInner);
        int startOffset1 = abvsInner.getStartOffset() + 1;
        int startOffset2 = startOffset1 + 1 + XSDateTimePointable.TYPE_TRAITS.getFixedLength();
        if (XSDateTimePointable.getYearMonth(abvsInner.getByteArray(), startOffset1) == XSDateTimePointable
                .getYearMonth(abvsInner.getByteArray(), startOffset2)
                && XSDateTimePointable.getDayTime(abvsInner.getByteArray(), startOffset1) == XSDateTimePointable
                        .getDayTime(abvsInner.getByteArray(), startOffset2)) {
            return true;
        }
        return false;
    }

    @Override
    public boolean operateYMDurationDTDuration(IntegerPointable intp1, LongPointable longp2)
            throws SystemException, IOException {
        if (intp1.getInteger() == 0 && longp2.getLong() == 0) {
            return true;
        }
        return false;
    }

    @Override
    public boolean operateYMDurationDuration(IntegerPointable intp1, XSDurationPointable durationp2)
            throws SystemException, IOException {
        if (durationp2.getYearMonth() == intp1.getInteger() && durationp2.getDayTime() == 0) {
            return true;
        }
        return false;
    }

    @Override
    public boolean operateYMDurationYMDuration(IntegerPointable intp1, IntegerPointable intp2)
            throws SystemException, IOException {
        return (intp1.compareTo(intp2) == 0);
    }

    @Override
    public boolean operateNull(TaggedValuePointable tvp1, TaggedValuePointable tvp2)
            throws SystemException, IOException {
        return tvp1.getTag() == ValueTag.JS_NULL_TAG && tvp2.getTag() == ValueTag.JS_NULL_TAG;
    }

}
