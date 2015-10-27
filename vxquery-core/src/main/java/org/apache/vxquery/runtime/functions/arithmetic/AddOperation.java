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
import org.apache.vxquery.datamodel.accessors.atomic.XSDatePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDateTimePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSTimePointable;
import org.apache.vxquery.datamodel.util.DateTime;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;

import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class AddOperation extends AbstractArithmeticOperation {
    protected final ArrayBackedValueStorage abvsInner = new ArrayBackedValueStorage();
    protected final DataOutput dOutInner = abvsInner.getDataOutput();
    private XSDecimalPointable decp1 = (XSDecimalPointable) XSDecimalPointable.FACTORY.createPointable();
    private XSDecimalPointable decp2 = (XSDecimalPointable) XSDecimalPointable.FACTORY.createPointable();
    private XSDateTimePointable datetimep1 = (XSDateTimePointable) XSDateTimePointable.FACTORY.createPointable();

    @Override
    public void operateDateDate(XSDatePointable datep, XSDatePointable datep2, DynamicContext dCtx, DataOutput dOut)
            throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public void operateDateDTDuration(XSDatePointable datep1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException {
        abvsInner.reset();
        // Add duration.
        DateTime.normalizeDateTime(datep1.getYearMonth(), datep1.getDayTime() + longp2.getLong(),
                datep1.getTimezoneHour(), datep1.getTimezoneMinute(), dOutInner);
        byte[] bytes = abvsInner.getByteArray();
        int startOffset = abvsInner.getStartOffset() + 1;
        // Convert to date.
        bytes[startOffset + XSDatePointable.TIMEZONE_HOUR_OFFSET] = bytes[startOffset
                + XSDateTimePointable.TIMEZONE_HOUR_OFFSET];
        bytes[startOffset + XSDatePointable.TIMEZONE_MINUTE_OFFSET] = bytes[startOffset
                + XSDateTimePointable.TIMEZONE_MINUTE_OFFSET];
        dOut.write(ValueTag.XS_DATE_TAG);
        dOut.write(bytes, startOffset, XSDatePointable.TYPE_TRAITS.getFixedLength());
    }

    @Override
    public void operateDatetimeDatetime(XSDateTimePointable datetimep, XSDateTimePointable datetimep2,
            DynamicContext dCtx, DataOutput dOut) throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public void operateDatetimeDTDuration(XSDateTimePointable datetimep1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException {
        // Add duration.
        abvsInner.reset();
        DateTime.normalizeDateTime(datetimep1.getYearMonth(), datetimep1.getDayTime() + longp2.getLong(),
                datetimep1.getTimezoneHour(), datetimep1.getTimezoneMinute(), dOutInner);
        dOut.write(ValueTag.XS_DATETIME_TAG);
        dOut.write(abvsInner.getByteArray(), abvsInner.getStartOffset() + 1,
                XSDateTimePointable.TYPE_TRAITS.getFixedLength());
    }

    @Override
    public void operateDatetimeYMDuration(XSDateTimePointable datetimep, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        // Add duration.
        abvsInner.reset();
        DateTime.normalizeDateTime(datetimep.getYearMonth() + intp.getInteger(), datetimep.getDayTime(),
                datetimep.getTimezoneHour(), datetimep.getTimezoneMinute(), dOutInner);
        dOut.write(ValueTag.XS_DATETIME_TAG);
        dOut.write(abvsInner.getByteArray(), abvsInner.getStartOffset() + 1,
                XSDateTimePointable.TYPE_TRAITS.getFixedLength());
    }

    @Override
    public void operateDateYMDuration(XSDatePointable datep, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        abvsInner.reset();
        // Add duration.
        DateTime.normalizeDateTime(datep.getYearMonth() + intp.getInteger(), datep.getDayTime(),
                datep.getTimezoneHour(), datep.getTimezoneMinute(), dOutInner);
        byte[] bytes = abvsInner.getByteArray();
        int startOffset = abvsInner.getStartOffset() + 1;
        // Convert to date.
        bytes[startOffset + XSDatePointable.TIMEZONE_HOUR_OFFSET] = bytes[startOffset
                + XSDateTimePointable.TIMEZONE_HOUR_OFFSET];
        bytes[startOffset + XSDatePointable.TIMEZONE_MINUTE_OFFSET] = bytes[startOffset
                + XSDateTimePointable.TIMEZONE_MINUTE_OFFSET];
        dOut.write(ValueTag.XS_DATE_TAG);
        dOut.write(bytes, startOffset, XSDatePointable.TYPE_TRAITS.getFixedLength());
    }

    @Override
    public void operateDecimalDecimal(XSDecimalPointable decp1, XSDecimalPointable decp2, DataOutput dOut)
            throws SystemException, IOException {
        // Prepare
        long value1 = decp1.getDecimalValue();
        long value2 = decp2.getDecimalValue();
        byte place1 = decp1.getDecimalPlace();
        byte place2 = decp2.getDecimalPlace();
        byte count1 = decp1.getDigitCount();
        byte count2 = decp2.getDigitCount();
        // Convert to matching values
        while (place1 > place2) {
            ++place2;
            value2 *= 10;
            ++count2;
        }
        while (place1 < place2) {
            ++place1;
            value1 *= 10;
            ++count1;
        }
        // Add
        if (count1 > XSDecimalPointable.PRECISION || count2 > XSDecimalPointable.PRECISION) {
            throw new SystemException(ErrorCode.XPDY0002);
        }
        value1 += value2;

        // Save
        dOut.write(ValueTag.XS_DECIMAL_TAG);
        dOut.writeByte(place1);
        dOut.writeLong(value1);
    }

    @Override
    public void operateDecimalDouble(XSDecimalPointable decp, DoublePointable doublep, DataOutput dOut)
            throws SystemException, IOException {
        double value = decp.doubleValue();
        value += doublep.doubleValue();
        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.writeDouble(value);
    }

    @Override
    public void operateDecimalDTDuration(XSDecimalPointable decp, LongPointable longp, DataOutput dOut)
            throws SystemException, IOException {
        long value = operateLongDecimal(longp.longValue(), decp);
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeLong(value);
    }

    @Override
    public void operateDecimalFloat(XSDecimalPointable decp, FloatPointable floatp, DataOutput dOut)
            throws SystemException, IOException {
        float value = decp.floatValue();
        value += floatp.floatValue();
        dOut.write(ValueTag.XS_FLOAT_TAG);
        dOut.writeFloat(value);
    }

    @Override
    public void operateDecimalInteger(XSDecimalPointable decp1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException {
        abvsInner.reset();
        decp2.set(abvsInner.getByteArray(), abvsInner.getStartOffset(), XSDecimalPointable.TYPE_TRAITS.getFixedLength());
        decp2.setDecimal(longp2.longValue(), (byte) 0);
        operateDecimalDecimal(decp1, decp2, dOut);
    }

    @Override
    public void operateDecimalYMDuration(XSDecimalPointable decp, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        int value = (int) operateLongDecimal(intp.intValue(), decp);
        dOut.write(ValueTag.XS_YEAR_MONTH_DURATION_TAG);
        dOut.writeInt(value);
    }

    @Override
    public void operateDoubleDecimal(DoublePointable doublep1, XSDecimalPointable decp2, DataOutput dOut)
            throws SystemException, IOException {
        operateDecimalDouble(decp2, doublep1, dOut);
    }

    @Override
    public void operateDoubleDouble(DoublePointable doublep1, DoublePointable doublep2, DataOutput dOut)
            throws SystemException, IOException {
        double value = doublep1.doubleValue();
        value += doublep2.doubleValue();
        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.writeDouble(value);
    }

    @Override
    public void operateDoubleDTDuration(DoublePointable doublep1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException {
        operateDTDurationDouble(longp2, doublep1, dOut);
    }

    @Override
    public void operateDoubleFloat(DoublePointable doublep, FloatPointable floatp, DataOutput dOut)
            throws SystemException, IOException {
        double value = doublep.doubleValue();
        value += floatp.doubleValue();
        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.writeDouble(value);
    }

    @Override
    public void operateDoubleInteger(DoublePointable doublep, LongPointable longp, DataOutput dOut)
            throws SystemException, IOException {
        double value = doublep.doubleValue();
        value += longp.doubleValue();
        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.writeDouble(value);
    }

    @Override
    public void operateDoubleYMDuration(DoublePointable doublep, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        int value = doublep.intValue();
        value += intp.intValue();
        dOut.write(ValueTag.XS_YEAR_MONTH_DURATION_TAG);
        dOut.writeInt(value);
    }

    @Override
    public void operateDTDurationDate(LongPointable longp, XSDatePointable datep, DataOutput dOut)
            throws SystemException, IOException {
        operateDateDTDuration(datep, longp, dOut);
    }

    @Override
    public void operateDTDurationDatetime(LongPointable longp, XSDateTimePointable datetimep, DataOutput dOut)
            throws SystemException, IOException {
        operateDatetimeDTDuration(datetimep, longp, dOut);
    }

    @Override
    public void operateDTDurationDecimal(LongPointable longp, XSDecimalPointable decp, DataOutput dOut)
            throws SystemException, IOException {
        operateDecimalDTDuration(decp, longp, dOut);
    }

    @Override
    public void operateDTDurationDouble(LongPointable longp, DoublePointable doublep, DataOutput dOut)
            throws SystemException, IOException {
        long value = longp.longValue();
        value += doublep.longValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeLong(value);
    }

    @Override
    public void operateDTDurationDTDuration(LongPointable longp1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException {
        long value = longp1.longValue();
        value += longp2.longValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeLong(value);
    }

    @Override
    public void operateDTDurationFloat(LongPointable longp1, FloatPointable floatp2, DataOutput dOut)
            throws SystemException, IOException {
        long value = longp1.longValue();
        value += floatp2.longValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeLong(value);
    }

    @Override
    public void operateDTDurationInteger(LongPointable longp1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException {
        long value = longp1.longValue();
        value += longp2.longValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeLong(value);
    }

    @Override
    public void operateDTDurationTime(LongPointable longp1, XSTimePointable timep2, DataOutput dOut)
            throws SystemException, IOException {
        // Get time into a datetime value.
        abvsInner.reset();
        datetimep1.set(abvsInner.getByteArray(), abvsInner.getStartOffset(),
                XSDateTimePointable.TYPE_TRAITS.getFixedLength());
        datetimep1.setDateTime(DateTime.TIME_DEFAULT_YEAR, DateTime.TIME_DEFAULT_MONTH, DateTime.TIME_DEFAULT_DAY,
                timep2.getHour(), timep2.getMinute(), timep2.getMilliSecond(), timep2.getTimezoneHour(),
                timep2.getTimezoneMinute());

        // Subtract.
        DateTime.normalizeDateTime(datetimep1.getYearMonth(), datetimep1.getDayTime() + longp1.getLong(),
                timep2.getTimezoneHour(), timep2.getTimezoneMinute(), dOutInner);

        // Convert to time.
        int startOffset = abvsInner.getStartOffset() + 1 + XSDateTimePointable.HOUR_OFFSET;
        dOut.write(ValueTag.XS_TIME_TAG);
        dOut.write(abvsInner.getByteArray(), startOffset, XSTimePointable.TYPE_TRAITS.getFixedLength());
    }

    @Override
    public void operateFloatDecimal(FloatPointable floatp, XSDecimalPointable decp, DataOutput dOut)
            throws SystemException, IOException {
        operateDecimalFloat(decp, floatp, dOut);
    }

    @Override
    public void operateFloatDouble(FloatPointable floatp, DoublePointable doublep, DataOutput dOut)
            throws SystemException, IOException {
        operateDoubleFloat(doublep, floatp, dOut);
    }

    @Override
    public void operateFloatDTDuration(FloatPointable floatp, LongPointable longp, DataOutput dOut)
            throws SystemException, IOException {
        operateDTDurationFloat(longp, floatp, dOut);
    }

    @Override
    public void operateFloatFloat(FloatPointable floatp, FloatPointable floatp2, DataOutput dOut)
            throws SystemException, IOException {
        float value = floatp.floatValue();
        value += floatp2.floatValue();
        dOut.write(ValueTag.XS_FLOAT_TAG);
        dOut.writeFloat(value);
    }

    @Override
    public void operateFloatInteger(FloatPointable floatp, LongPointable longp, DataOutput dOut)
            throws SystemException, IOException {
        float value = floatp.floatValue();
        value += longp.floatValue();
        dOut.write(ValueTag.XS_FLOAT_TAG);
        dOut.writeFloat(value);
    }

    @Override
    public void operateFloatYMDuration(FloatPointable floatp, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        operateYMDurationFloat(intp, floatp, dOut);
    }

    @Override
    public void operateIntegerDecimal(LongPointable longp, XSDecimalPointable decp, DataOutput dOut)
            throws SystemException, IOException {
        operateDecimalInteger(decp, longp, dOut);
    }

    @Override
    public void operateIntegerDouble(LongPointable longp, DoublePointable doublep, DataOutput dOut)
            throws SystemException, IOException {
        operateDoubleInteger(doublep, longp, dOut);
    }

    @Override
    public void operateIntegerDTDuration(LongPointable longp1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException {
        operateDTDurationInteger(longp2, longp1, dOut);
    }

    @Override
    public void operateIntegerFloat(LongPointable longp, FloatPointable floatp, DataOutput dOut)
            throws SystemException, IOException {
        operateFloatInteger(floatp, longp, dOut);
    }

    @Override
    public void operateIntegerInteger(LongPointable longp, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException {
        long value = longp.getLong();
        value += longp2.getLong();
        dOut.write(ValueTag.XS_INTEGER_TAG);
        dOut.writeLong(value);
    }

    @Override
    public void operateIntegerYMDuration(LongPointable longp, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        int value = longp.intValue();
        value += intp.intValue();
        dOut.write(ValueTag.XS_YEAR_MONTH_DURATION_TAG);
        dOut.writeInt(value);
    }

    @Override
    public void operateTimeDTDuration(XSTimePointable timep1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException {
        operateDTDurationTime(longp2, timep1, dOut);
    }

    @Override
    public void operateTimeTime(XSTimePointable timep, XSTimePointable timep2, DynamicContext dCtx, DataOutput dOut)
            throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public void operateYMDurationDate(IntegerPointable intp, XSDatePointable datep, DataOutput dOut)
            throws SystemException, IOException {
        operateDateYMDuration(datep, intp, dOut);
    }

    @Override
    public void operateYMDurationDatetime(IntegerPointable intp, XSDateTimePointable datetimep, DataOutput dOut)
            throws SystemException, IOException {
        operateDatetimeYMDuration(datetimep, intp, dOut);
    }

    @Override
    public void operateYMDurationDecimal(IntegerPointable intp, XSDecimalPointable decp, DataOutput dOut)
            throws SystemException, IOException {
        operateDecimalYMDuration(decp, intp, dOut);
    }

    @Override
    public void operateYMDurationDouble(IntegerPointable intp, DoublePointable doublep, DataOutput dOut)
            throws SystemException, IOException {
        operateDoubleYMDuration(doublep, intp, dOut);
    }

    @Override
    public void operateYMDurationFloat(IntegerPointable intp, FloatPointable floatp, DataOutput dOut)
            throws SystemException, IOException {
        operateFloatYMDuration(floatp, intp, dOut);
    }

    @Override
    public void operateYMDurationInteger(IntegerPointable intp, LongPointable longp, DataOutput dOut)
            throws SystemException, IOException {
        operateIntegerYMDuration(longp, intp, dOut);
    }

    @Override
    public void operateYMDurationYMDuration(IntegerPointable intp, IntegerPointable intp2, DataOutput dOut)
            throws SystemException, IOException {
        int value = intp.intValue();
        value += intp2.intValue();
        dOut.write(ValueTag.XS_YEAR_MONTH_DURATION_TAG);
        dOut.writeInt(value);
    }

    public long operateLongDecimal(long longValue, XSDecimalPointable decp2) throws SystemException, IOException {
        abvsInner.reset();
        decp1.set(abvsInner.getByteArray(), abvsInner.getStartOffset(), XSDecimalPointable.TYPE_TRAITS.getFixedLength());
        decp1.setDecimal(longValue, (byte) 0);
        // Prepare
        long value1 = decp1.getDecimalValue();
        long value2 = decp2.getDecimalValue();
        byte place1 = decp1.getDecimalPlace();
        byte place2 = decp2.getDecimalPlace();
        byte count1 = decp1.getDigitCount();
        byte count2 = decp2.getDigitCount();
        // Convert to matching values
        while (place1 > place2) {
            ++place2;
            value2 *= 10;
            ++count2;
        }
        while (place1 < place2) {
            ++place1;
            value1 *= 10;
            ++count1;
        }
        // Add
        if (count1 > XSDecimalPointable.PRECISION || count2 > XSDecimalPointable.PRECISION) {
            throw new SystemException(ErrorCode.XPDY0002);
        }
        value1 += value2;
        // Save
        decp2.setDecimal(value1, place1);
        return decp2.longValue();
    }
}
