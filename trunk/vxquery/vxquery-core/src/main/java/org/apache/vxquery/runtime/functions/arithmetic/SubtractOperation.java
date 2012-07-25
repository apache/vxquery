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
    protected final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
    protected final DataOutput dataOutput = abvs.getDataOutput();

    public void operateDateDate(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx, DataOutput dOut)
            throws SystemException, IOException {
        abvs.reset();
        DateTime.getTimezoneDateTime(datep1, dCtx, dataOutput);
        XSDateTimePointable datetimep1 = new XSDateTimePointable();
        datetimep1.set(abvs.getByteArray(), 0, abvs.getLength());

        abvs.reset();
        DateTime.getTimezoneDateTime(datep2, dCtx, dataOutput);
        XSDateTimePointable datetimep2 = new XSDateTimePointable();
        datetimep1.set(abvs.getByteArray(), 0, abvs.getLength());

        operateDatetimeDatetime(datetimep1, datetimep2, dCtx, dOut);
    }

    @Override
    public void operateDateDTDuration(XSDatePointable datep1, IntegerPointable intp2, DataOutput dOut)
            throws SystemException, IOException {
        // Add duration.
        abvs.reset();
        DateTime.normalizeDateTime(datep1.getYearMonth(), datep1.getDayTime() - intp2.getInteger(), dataOutput);
        byte[] bytes = abvs.getByteArray();
        // Convert to date.
        bytes[XSDatePointable.TIMEZONE_HOUR_OFFSET] = bytes[XSDateTimePointable.TIMEZONE_HOUR_OFFSET];
        bytes[XSDatePointable.TIMEZONE_MINUTE_OFFSET] = bytes[XSDateTimePointable.TIMEZONE_MINUTE_OFFSET];
        dOut.write(ValueTag.XS_DATE_TAG);
        dOut.write(bytes, 0, XSDatePointable.TYPE_TRAITS.getFixedLength());
    }

    @Override
    public void operateDatetimeDatetime(XSDateTimePointable datetimep1, XSDateTimePointable datetimep2,
            DynamicContext dCtx, DataOutput dOut) throws SystemException, IOException {
        abvs.reset();
        DateTime.getTimezoneDateTime(datetimep1, dCtx, dataOutput);
        byte[] bytes1 = abvs.getByteArray();

        abvs.reset();
        DateTime.getTimezoneDateTime(datetimep2, dCtx, dataOutput);
        byte[] bytes2 = abvs.getByteArray();

        long dayTime1 = XSDateTimePointable.getDayTime(bytes1, 0);
        long dayTime2 = XSDateTimePointable.getDayTime(bytes2, 0);
        long yearMonth = XSDateTimePointable.getYearMonth(bytes1, 0) - XSDateTimePointable.getYearMonth(bytes2, 0);
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
        dOut.write((int) dayTime1);
    }

    @Override
    public void operateDatetimeDTDuration(XSDateTimePointable datetimep, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        // Add duration.
        abvs.reset();
        DateTime.normalizeDateTime(datetimep.getYearMonth(), datetimep.getDayTime() - intp.getInteger(), dataOutput);
        dOut.write(ValueTag.XS_DATETIME_TAG);
        dOut.write(abvs.getByteArray());
    }

    @Override
    public void operateDatetimeYMDuration(XSDateTimePointable datetimep, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        // Add duration.
        abvs.reset();
        DateTime.normalizeDateTime(datetimep.getYearMonth() - intp.getInteger(), datetimep.getDayTime(), dataOutput);
        dOut.write(ValueTag.XS_DATE_TAG);
        dOut.write(abvs.getByteArray());
    }

    @Override
    public void operateDateYMDuration(XSDatePointable datep, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        // Add duration.
        abvs.reset();
        DateTime.normalizeDateTime(datep.getYearMonth() - intp.getInteger(), datep.getDayTime(), dataOutput);
        byte[] bytes = abvs.getByteArray();
        // Convert to date.
        bytes[XSDatePointable.TIMEZONE_HOUR_OFFSET] = bytes[XSDateTimePointable.TIMEZONE_HOUR_OFFSET];
        bytes[XSDatePointable.TIMEZONE_MINUTE_OFFSET] = bytes[XSDateTimePointable.TIMEZONE_MINUTE_OFFSET];
        dOut.write(ValueTag.XS_DATE_TAG);
        dOut.write(bytes, 0, XSDatePointable.TYPE_TRAITS.getFixedLength());
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
        dOut.writeDouble(value1);
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
    public void operateDecimalDTDuration(XSDecimalPointable decp, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        int value = operateDecimalInt(decp, intp.intValue());
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeInt(value);
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
        XSDecimalPointable decp2 = new XSDecimalPointable();
        decp2.setDecimal(longp2.longValue(), (byte) 0);
        operateDecimalDecimal(decp1, decp2, dOut);
    }

    @Override
    public void operateDecimalYMDuration(XSDecimalPointable decp, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        int value = operateDecimalInt(decp, intp.intValue());
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
    public void operateDoubleDTDuration(DoublePointable doublep, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        int value = doublep.intValue();
        value -= intp.intValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeInt(value);
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
    public void operateDTDurationDate(IntegerPointable intp, XSDatePointable datep, DataOutput dOut)
            throws SystemException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void operateDTDurationDatetime(IntegerPointable intp, XSDateTimePointable datetimep, DataOutput dOut)
            throws SystemException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void operateDTDurationDecimal(IntegerPointable intp, XSDecimalPointable decp, DataOutput dOut)
            throws SystemException, IOException {
        int value = intp.intValue();
        value -= decp.intValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeInt(value);
    }

    @Override
    public void operateDTDurationDouble(IntegerPointable intp, DoublePointable doublep, DataOutput dOut)
            throws SystemException, IOException {
        int value = intp.intValue();
        value -= doublep.intValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeInt(value);
    }

    @Override
    public void operateDTDurationDTDuration(IntegerPointable intp, IntegerPointable intp2, DataOutput dOut)
            throws SystemException, IOException {
        int value = intp.intValue();
        value -= intp2.intValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeInt(value);
    }

    @Override
    public void operateDTDurationFloat(IntegerPointable intp, FloatPointable floatp, DataOutput dOut)
            throws SystemException, IOException {
        int value = intp.intValue();
        value -= floatp.intValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeInt(value);
    }

    @Override
    public void operateDTDurationInteger(IntegerPointable intp, LongPointable longp, DataOutput dOut)
            throws SystemException, IOException {
        int value = intp.intValue();
        value -= longp.intValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeInt(value);
    }

    @Override
    public void operateDTDurationTime(IntegerPointable intp, XSTimePointable timep, DataOutput dOut)
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
    public void operateFloatDTDuration(FloatPointable floatp, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        int value = floatp.intValue();
        value -= intp.intValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeInt(value);
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
    public void operateIntegerDTDuration(LongPointable longp, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        int value = longp.intValue();
        value -= intp.intValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeInt(value);
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
    public void operateTimeDTDuration(XSTimePointable timep1, IntegerPointable intp2, DataOutput dOut)
            throws SystemException, IOException {
        // Add duration.
        abvs.reset();
        DateTime.normalizeDateTime(0, timep1.getDayTime() - intp2.getInteger(), dataOutput);
        byte[] bytes = abvs.getByteArray();
        // Convert to date.
        bytes[XSTimePointable.HOUR_OFFSET] = bytes[XSDateTimePointable.HOUR_OFFSET];
        bytes[XSTimePointable.MINUTE_OFFSET] = bytes[XSDateTimePointable.MINUTE_OFFSET];
        bytes[XSTimePointable.MILLISECOND_OFFSET] = bytes[XSDateTimePointable.MILLISECOND_OFFSET];
        bytes[XSTimePointable.TIMEZONE_HOUR_OFFSET] = (byte) timep1.getTimezoneHour();
        bytes[XSTimePointable.TIMEZONE_MINUTE_OFFSET] = (byte) timep1.getTimezoneMinute();
        dOut.write(ValueTag.XS_TIME_TAG);
        dOut.write(bytes, 0, XSDatePointable.TYPE_TRAITS.getFixedLength());
    }

    @Override
    public void operateTimeTime(XSTimePointable timep1, XSTimePointable timep2, DynamicContext dCtx, DataOutput dOut)
            throws SystemException, IOException {
        abvs.reset();
        DateTime.getTimezoneDateTime(timep1, dCtx, dataOutput);
        XSDateTimePointable datetimep1 = new XSDateTimePointable();
        datetimep1.set(abvs.getByteArray(), 0, abvs.getLength());

        abvs.reset();
        DateTime.getTimezoneDateTime(timep2, dCtx, dataOutput);
        XSDateTimePointable datetimep2 = new XSDateTimePointable();
        datetimep1.set(abvs.getByteArray(), 0, abvs.getLength());

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

    public int operateIntDecimal(int intValue, XSDecimalPointable decp2) throws SystemException, IOException {
        XSDecimalPointable decp1 = new XSDecimalPointable();
        decp1.setDecimal(intValue, (byte) 0);
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
        return decp2.intValue();
    }

    public int operateDecimalInt(XSDecimalPointable decp1, int intValue) throws SystemException, IOException {
        XSDecimalPointable decp2 = new XSDecimalPointable();
        decp2.setDecimal(intValue, (byte) 0);
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
        return decp2.intValue();
    }

}