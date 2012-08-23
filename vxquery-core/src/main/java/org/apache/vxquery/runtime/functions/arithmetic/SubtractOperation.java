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

import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class SubtractOperation extends AbstractArithmeticOperation {
    protected final ArrayBackedValueStorage abvsInner1 = new ArrayBackedValueStorage();
    protected final DataOutput dOutInner1 = abvsInner1.getDataOutput();
    protected final ArrayBackedValueStorage abvsInner2 = new ArrayBackedValueStorage();
    protected final DataOutput dOutInner2 = abvsInner2.getDataOutput();

    public void operateDateDate(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx, DataOutput dOut)
            throws SystemException, IOException {
        abvsInner1.reset();
        DateTime.getTimezoneDateTime(datep1, dCtx, dOutInner1);
        XSDateTimePointable datetimep1 = (XSDateTimePointable) XSDateTimePointable.FACTORY.createPointable();
        datetimep1.set(abvsInner1.getByteArray(), abvsInner1.getStartOffset() + 1, abvsInner1.getLength());

        abvsInner2.reset();
        DateTime.getTimezoneDateTime(datep2, dCtx, dOutInner2);
        XSDateTimePointable datetimep2 = (XSDateTimePointable) XSDateTimePointable.FACTORY.createPointable();
        datetimep2.set(abvsInner2.getByteArray(), abvsInner2.getStartOffset() + 1, abvsInner2.getLength());

        operateDatetimeDatetime(datetimep1, datetimep2, dCtx, dOut);
    }

    @Override
    public void operateDateDTDuration(XSDatePointable datep1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException {
        // Add duration.
        abvsInner1.reset();
        DateTime.normalizeDateTime(datep1.getYearMonth(), datep1.getDayTime() - longp2.getLong(), dOutInner1);
        byte[] bytes = abvsInner1.getByteArray();
        int startOffset = abvsInner1.getStartOffset() + 1;
        // Convert to date.
        bytes[startOffset + XSDatePointable.TIMEZONE_HOUR_OFFSET] = bytes[startOffset
                + XSDateTimePointable.TIMEZONE_HOUR_OFFSET];
        bytes[startOffset + XSDatePointable.TIMEZONE_MINUTE_OFFSET] = bytes[startOffset
                + XSDateTimePointable.TIMEZONE_MINUTE_OFFSET];
        dOut.write(ValueTag.XS_DATE_TAG);
        dOut.write(bytes, startOffset, XSDatePointable.TYPE_TRAITS.getFixedLength());
    }

    @Override
    public void operateDatetimeDatetime(XSDateTimePointable datetimep1, XSDateTimePointable datetimep2,
            DynamicContext dCtx, DataOutput dOut) throws SystemException, IOException {
        abvsInner1.reset();
        DateTime.getTimezoneDateTime(datetimep1, dCtx, dOutInner1);
        byte[] bytes1 = abvsInner1.getByteArray();
        int startOffset1 = abvsInner1.getStartOffset() + 1;
        
        abvsInner2.reset();
        DateTime.getTimezoneDateTime(datetimep2, dCtx, dOutInner2);
        byte[] bytes2 = abvsInner2.getByteArray();
        int startOffset2 = abvsInner2.getStartOffset() + 1;
        
        long dayTime1 = XSDateTimePointable.getDayTime(bytes1, startOffset1);
        long dayTime2 = XSDateTimePointable.getDayTime(bytes2, startOffset2);
        long yearMonth = XSDateTimePointable.getYearMonth(bytes1, startOffset1)
                - XSDateTimePointable.getYearMonth(bytes2, startOffset2);
        // Find duration.
        dayTime1 -= dayTime2;
        // Default
        long year = datetimep1.getYear();
        long month = datetimep1.getMonth();
        int change = 1;
        if (yearMonth > 0) {
            change = -1;
        }
        long[] monthDayLimits = (DateTime.isLeapYear(year) ? DateTime.DAYS_OF_MONTH_LEAP : DateTime.DAYS_OF_MONTH_ORDI);
        while (yearMonth != 0) {
            dayTime1 += monthDayLimits[(int) month - 1] * DateTime.CHRONON_OF_DAY;
            month += change;
            yearMonth += change;

            if (month < DateTime.FIELD_MINS[DateTime.MONTH_FIELD_INDEX]) {
                // Too small
                month = DateTime.FIELD_MAXS[DateTime.MONTH_FIELD_INDEX];
                --year;
            } else if (month > DateTime.FIELD_MAXS[DateTime.MONTH_FIELD_INDEX]) {
                // Too large
                month = DateTime.FIELD_MINS[DateTime.MONTH_FIELD_INDEX];
                ++year;
            }
            monthDayLimits = (DateTime.isLeapYear(year) ? DateTime.DAYS_OF_MONTH_LEAP : DateTime.DAYS_OF_MONTH_ORDI);
        }
        // Save.
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeLong(dayTime1);
    }

    @Override
    public void operateDatetimeDTDuration(XSDateTimePointable datetimep, LongPointable longp, DataOutput dOut)
            throws SystemException, IOException {
        // Add duration.
        abvsInner1.reset();
        DateTime.normalizeDateTime(datetimep.getYearMonth(), datetimep.getDayTime() - longp.getLong(), dOutInner1);
        dOut.write(ValueTag.XS_DATETIME_TAG);
        dOut.write(abvsInner1.getByteArray(), abvsInner1.getStartOffset() + 1,
                XSDateTimePointable.TYPE_TRAITS.getFixedLength());
    }

    @Override
    public void operateDatetimeYMDuration(XSDateTimePointable datetimep, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        // Add duration.
        abvsInner1.reset();
        DateTime.normalizeDateTime(datetimep.getYearMonth() - intp.getInteger(), datetimep.getDayTime(), dOutInner1);
        dOut.write(ValueTag.XS_DATETIME_TAG);
        dOut.write(abvsInner1.getByteArray(), abvsInner1.getStartOffset() + 1,
                XSDateTimePointable.TYPE_TRAITS.getFixedLength());
    }

    @Override
    public void operateDateYMDuration(XSDatePointable datep, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        // Add duration.
        abvsInner1.reset();
        DateTime.normalizeDateTime(datep.getYearMonth() - intp.getInteger(), datep.getDayTime(), dOutInner1);
        byte[] bytes = abvsInner1.getByteArray();
        int startOffset = abvsInner1.getStartOffset() + 1;
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
        value1 -= value2;
        // Save
        dOut.write(ValueTag.XS_DECIMAL_TAG);
        dOut.writeByte(place1);
        dOut.writeLong(value1);
    }

    @Override
    public void operateDecimalDouble(XSDecimalPointable decp, DoublePointable doublep, DataOutput dOut)
            throws SystemException, IOException {
        double value = decp.doubleValue();
        value -= doublep.doubleValue();
        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.writeDouble(value);
    }

    @Override
    public void operateDecimalDTDuration(XSDecimalPointable decp, LongPointable longp, DataOutput dOut)
            throws SystemException, IOException {
        long value = operateDecimalInt(decp, longp.longValue());
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeLong(value);
    }

    @Override
    public void operateDecimalFloat(XSDecimalPointable decp, FloatPointable floatp, DataOutput dOut)
            throws SystemException, IOException {
        float value = decp.floatValue();
        value -= floatp.floatValue();
        dOut.write(ValueTag.XS_FLOAT_TAG);
        dOut.writeFloat(value);
    }

    @Override
    public void operateDecimalInteger(XSDecimalPointable decp1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException {
        abvsInner1.reset();
        XSDecimalPointable decp2 = (XSDecimalPointable) XSDecimalPointable.FACTORY.createPointable();
        decp2.set(abvsInner1.getByteArray(), abvsInner1.getStartOffset(),
                XSDecimalPointable.TYPE_TRAITS.getFixedLength());
        decp2.setDecimal(longp2.longValue(), (byte) 0);
        operateDecimalDecimal(decp1, decp2, dOut);
    }

    @Override
    public void operateDecimalYMDuration(XSDecimalPointable decp, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        int value = (int) operateDecimalInt(decp, intp.intValue());
        dOut.write(ValueTag.XS_YEAR_MONTH_DURATION_TAG);
        dOut.writeInt(value);
    }

    @Override
    public void operateDoubleDecimal(DoublePointable doublep, XSDecimalPointable decp, DataOutput dOut)
            throws SystemException, IOException {
        double value = doublep.doubleValue();
        value -= decp.doubleValue();
        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.writeDouble(value);
    }

    @Override
    public void operateDoubleDouble(DoublePointable doublep, DoublePointable doublep2, DataOutput dOut)
            throws SystemException, IOException {
        double value = doublep.doubleValue();
        value -= doublep2.doubleValue();
        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.writeDouble(value);
    }

    @Override
    public void operateDoubleDTDuration(DoublePointable doublep, LongPointable longp, DataOutput dOut)
            throws SystemException, IOException {
        long value = doublep.intValue();
        value -= longp.intValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeLong(value);
    }

    @Override
    public void operateDoubleFloat(DoublePointable doublep, FloatPointable floatp, DataOutput dOut)
            throws SystemException, IOException {
        double value = doublep.doubleValue();
        value -= floatp.doubleValue();
        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.writeDouble(value);
    }

    @Override
    public void operateDoubleInteger(DoublePointable doublep, LongPointable longp, DataOutput dOut)
            throws SystemException, IOException {
        double value = doublep.doubleValue();
        value -= longp.doubleValue();
        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.writeDouble(value);
    }

    @Override
    public void operateDoubleYMDuration(DoublePointable doublep, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        int value = doublep.intValue();
        value -= intp.intValue();
        dOut.write(ValueTag.XS_YEAR_MONTH_DURATION_TAG);
        dOut.writeInt(value);
    }

    @Override
    public void operateDTDurationDate(LongPointable longp, XSDatePointable datep, DataOutput dOut)
            throws SystemException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void operateDTDurationDatetime(LongPointable longp, XSDateTimePointable datetimep, DataOutput dOut)
            throws SystemException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void operateDTDurationDecimal(LongPointable longp, XSDecimalPointable decp, DataOutput dOut)
            throws SystemException, IOException {
        long value = longp.longValue();
        value -= decp.longValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeLong(value);
    }

    @Override
    public void operateDTDurationDouble(LongPointable longp, DoublePointable doublep, DataOutput dOut)
            throws SystemException, IOException {
        long value = longp.longValue();
        value -= doublep.longValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeLong(value);
    }

    @Override
    public void operateDTDurationDTDuration(LongPointable longp1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException {
        long value = longp1.longValue();
        value -= longp2.longValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeLong(value);
    }

    @Override
    public void operateDTDurationFloat(LongPointable longp, FloatPointable floatp, DataOutput dOut)
            throws SystemException, IOException {
        long value = longp.longValue();
        value -= floatp.longValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeLong(value);
    }

    @Override
    public void operateDTDurationInteger(LongPointable longp1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException {
        long value = longp1.longValue();
        value -= longp2.longValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeLong(value);
    }

    @Override
    public void operateDTDurationTime(LongPointable longp, XSTimePointable timep, DataOutput dOut)
            throws SystemException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void operateFloatDecimal(FloatPointable floatp, XSDecimalPointable decp, DataOutput dOut)
            throws SystemException, IOException {
        float value = floatp.floatValue();
        value -= decp.floatValue();
        dOut.write(ValueTag.XS_FLOAT_TAG);
        dOut.writeDouble(value);
    }

    @Override
    public void operateFloatDouble(FloatPointable floatp, DoublePointable doublep, DataOutput dOut)
            throws SystemException, IOException {
        double value = floatp.doubleValue();
        value -= doublep.doubleValue();
        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.writeDouble(value);
    }

    @Override
    public void operateFloatDTDuration(FloatPointable floatp, LongPointable longp, DataOutput dOut)
            throws SystemException, IOException {
        long value = floatp.longValue();
        value -= longp.longValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeLong(value);
    }

    @Override
    public void operateFloatFloat(FloatPointable floatp, FloatPointable floatp2, DataOutput dOut)
            throws SystemException, IOException {
        float value = floatp.floatValue();
        value -= floatp2.floatValue();
        dOut.write(ValueTag.XS_FLOAT_TAG);
        dOut.writeFloat(value);
    }

    @Override
    public void operateFloatInteger(FloatPointable floatp, LongPointable longp, DataOutput dOut)
            throws SystemException, IOException {
        float value = floatp.floatValue();
        value -= longp.floatValue();
        dOut.write(ValueTag.XS_FLOAT_TAG);
        dOut.writeFloat(value);
    }

    @Override
    public void operateFloatYMDuration(FloatPointable floatp, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        int value = floatp.intValue();
        value -= intp.intValue();
        dOut.write(ValueTag.XS_YEAR_MONTH_DURATION_TAG);
        dOut.writeInt(value);
    }

    @Override
    public void operateIntegerDecimal(LongPointable longp, XSDecimalPointable decp, DataOutput dOut)
            throws SystemException, IOException {
        double value = longp.doubleValue();
        value -= decp.doubleValue();
        dOut.write(ValueTag.XS_DECIMAL_TAG);
        dOut.writeDouble(value);
    }

    @Override
    public void operateIntegerDouble(LongPointable longp, DoublePointable doublep, DataOutput dOut)
            throws SystemException, IOException {
        double value = longp.doubleValue();
        value -= doublep.doubleValue();
        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.writeDouble(value);
    }

    @Override
    public void operateIntegerDTDuration(LongPointable longp1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException {
        long value = longp1.longValue();
        value -= longp2.longValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeLong(value);
    }

    @Override
    public void operateIntegerFloat(LongPointable longp, FloatPointable floatp, DataOutput dOut)
            throws SystemException, IOException {
        float value = longp.floatValue();
        value -= floatp.floatValue();
        dOut.write(ValueTag.XS_FLOAT_TAG);
        dOut.writeFloat(value);
    }

    @Override
    public void operateIntegerInteger(LongPointable longp, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException {
        long value = longp.getLong();
        value -= longp2.getLong();
        dOut.write(ValueTag.XS_INTEGER_TAG);
        dOut.writeLong(value);
    }

    @Override
    public void operateIntegerYMDuration(LongPointable longp, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        int value = longp.intValue();
        value -= intp.intValue();
        dOut.write(ValueTag.XS_YEAR_MONTH_DURATION_TAG);
        dOut.writeInt(value);
    }

    @Override
    public void operateTimeDTDuration(XSTimePointable timep1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException {
        // Add duration.
        abvsInner1.reset();
        DateTime.normalizeDateTime(0, timep1.getDayTime() - longp2.getLong(), dOutInner1);
        byte[] bytes = abvsInner1.getByteArray();
        // Convert to date.
        int startOffset1 = abvsInner1.getStartOffset() + 1;
        bytes[startOffset1 + XSTimePointable.HOUR_OFFSET] = bytes[startOffset1 + XSDateTimePointable.HOUR_OFFSET];
        bytes[startOffset1 + XSTimePointable.MINUTE_OFFSET] = bytes[startOffset1 + XSDateTimePointable.MINUTE_OFFSET];
        bytes[startOffset1 + XSTimePointable.MILLISECOND_OFFSET] = bytes[startOffset1
                + XSDateTimePointable.MILLISECOND_OFFSET];
        bytes[startOffset1 + XSTimePointable.TIMEZONE_HOUR_OFFSET] = (byte) timep1.getTimezoneHour();
        bytes[startOffset1 + XSTimePointable.TIMEZONE_MINUTE_OFFSET] = (byte) timep1.getTimezoneMinute();
        dOut.write(ValueTag.XS_TIME_TAG);
        dOut.write(bytes, startOffset1, XSDatePointable.TYPE_TRAITS.getFixedLength());
    }

    @Override
    public void operateTimeTime(XSTimePointable timep1, XSTimePointable timep2, DynamicContext dCtx, DataOutput dOut)
            throws SystemException, IOException {
        abvsInner1.reset();
        DateTime.getTimezoneDateTime(timep1, dCtx, dOutInner1);
        XSDateTimePointable datetimep1 = (XSDateTimePointable) XSDateTimePointable.FACTORY.createPointable();
        datetimep1.set(abvsInner1.getByteArray(), abvsInner1.getStartOffset(), abvsInner1.getLength());

        abvsInner2.reset();
        DateTime.getTimezoneDateTime(timep2, dCtx, dOutInner2);
        XSDateTimePointable datetimep2 = (XSDateTimePointable) XSDateTimePointable.FACTORY.createPointable();
        datetimep2.set(abvsInner2.getByteArray(), abvsInner2.getStartOffset(), abvsInner2.getLength());

        operateDatetimeDatetime(datetimep1, datetimep2, dCtx, dOut);
    }

    @Override
    public void operateYMDurationDate(IntegerPointable intp, XSDatePointable datep, DataOutput dOut)
            throws SystemException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void operateYMDurationDatetime(IntegerPointable intp, XSDateTimePointable datetimep, DataOutput dOut)
            throws SystemException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void operateYMDurationDecimal(IntegerPointable intp, XSDecimalPointable decp, DataOutput dOut)
            throws SystemException, IOException {
        int value = intp.intValue();
        value -= decp.intValue();
        dOut.write(ValueTag.XS_YEAR_MONTH_DURATION_TAG);
        dOut.writeInt(value);
    }

    @Override
    public void operateYMDurationDouble(IntegerPointable intp, DoublePointable doublep, DataOutput dOut)
            throws SystemException, IOException {
        int value = intp.intValue();
        value -= doublep.intValue();
        dOut.write(ValueTag.XS_YEAR_MONTH_DURATION_TAG);
        dOut.writeInt(value);
    }

    @Override
    public void operateYMDurationFloat(IntegerPointable intp, FloatPointable floatp, DataOutput dOut)
            throws SystemException, IOException {
        int value = intp.intValue();
        value -= floatp.intValue();
        dOut.write(ValueTag.XS_YEAR_MONTH_DURATION_TAG);
        dOut.writeInt(value);
    }

    @Override
    public void operateYMDurationInteger(IntegerPointable intp, LongPointable longp, DataOutput dOut)
            throws SystemException, IOException {
        int value = intp.intValue();
        value -= longp.intValue();
        dOut.write(ValueTag.XS_YEAR_MONTH_DURATION_TAG);
        dOut.writeInt(value);
    }

    @Override
    public void operateYMDurationYMDuration(IntegerPointable intp, IntegerPointable intp2, DataOutput dOut)
            throws SystemException, IOException {
        int value = intp.intValue();
        value -= intp2.intValue();
        dOut.write(ValueTag.XS_YEAR_MONTH_DURATION_TAG);
        dOut.writeInt(value);
    }

    public long operateIntDecimal(long longValue, XSDecimalPointable decp2) throws SystemException, IOException {
        abvsInner1.reset();
        XSDecimalPointable decp1 = (XSDecimalPointable) XSDecimalPointable.FACTORY.createPointable();
        decp1.set(abvsInner1.getByteArray(), abvsInner1.getStartOffset(),
                XSDecimalPointable.TYPE_TRAITS.getFixedLength());
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
        value1 -= value2;
        // Save
        decp2.setDecimal(value1, place1);
        return decp2.longValue();
    }

    public long operateDecimalInt(XSDecimalPointable decp1, long longValue) throws SystemException, IOException {
        abvsInner1.reset();
        XSDecimalPointable decp2 = (XSDecimalPointable) XSDecimalPointable.FACTORY.createPointable();
        decp2.set(abvsInner1.getByteArray(), abvsInner1.getStartOffset(),
                XSDecimalPointable.TYPE_TRAITS.getFixedLength());
        decp2.setDecimal(longValue, (byte) 0);
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
        value1 -= value2;
        // Save
        decp2.setDecimal(value1, place1);
        return decp2.longValue();
    }

}