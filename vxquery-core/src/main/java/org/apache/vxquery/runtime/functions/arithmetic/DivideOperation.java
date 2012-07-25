package org.apache.vxquery.runtime.functions.arithmetic;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.datamodel.accessors.atomic.XSDatePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDateTimePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSTimePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;

import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;

public class DivideOperation extends AbstractArithmeticOperation {
    @Override
    public void operateDateDate(XSDatePointable datep, XSDatePointable datep2, DynamicContext dCtx, DataOutput dOut)
            throws SystemException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void operateDateDTDuration(XSDatePointable datep, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void operateDatetimeDatetime(XSDateTimePointable datetimep, XSDateTimePointable datetimep2,
            DynamicContext dCtx, DataOutput dOut) throws SystemException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void operateDatetimeDTDuration(XSDateTimePointable datetimep, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void operateDatetimeYMDuration(XSDateTimePointable datetimep, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void operateDateYMDuration(XSDatePointable datep, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void operateDecimalDecimal(XSDecimalPointable decp1, XSDecimalPointable decp2, DataOutput dOut)
            throws SystemException, IOException {
        // Prepare
        long value1 = decp1.getDecimalValue();
        long value2 = decp2.getDecimalValue();
        byte place1 = decp1.getDecimalPlace();
        byte place2 = decp2.getDecimalPlace();
        // Divide
        if (value1 > Long.MAX_VALUE * value2) {
            throw new SystemException(ErrorCode.XPDY0002);
        }
        value1 /= value2;
        place1 -= place2;
        // Save
        dOut.write(ValueTag.XS_DECIMAL_TAG);
        dOut.writeByte(place1);
        dOut.writeLong(value1);
    }

    @Override
    public void operateDecimalDouble(XSDecimalPointable decp1, DoublePointable doublep2, DataOutput dOut)
            throws SystemException, IOException {
        double value = decp1.doubleValue();
        value /= doublep2.doubleValue();
        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.writeDouble(value);
    }

    @Override
    public void operateDecimalDTDuration(XSDecimalPointable decp1, IntegerPointable intp2, DataOutput dOut)
            throws SystemException, IOException {
        int value = operateDecimalInt(decp1, intp2.intValue());
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeInt(value);
    }

    @Override
    public void operateDecimalFloat(XSDecimalPointable decp1, FloatPointable floatp2, DataOutput dOut)
            throws SystemException, IOException {
        float value = decp1.floatValue();
        value /= floatp2.floatValue();
        dOut.write(ValueTag.XS_FLOAT_TAG);
        dOut.writeDouble(value);
    }

    @Override
    public void operateDecimalInteger(XSDecimalPointable decp1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException {
        // Convert
        XSDecimalPointable decp2 = new XSDecimalPointable();
        decp2.setDecimal(longp2.longValue(), (byte) 0);
        operateDecimalDecimal(decp1, decp2, dOut);
    }

    @Override
    public void operateDecimalYMDuration(XSDecimalPointable decp1, IntegerPointable intp2, DataOutput dOut)
            throws SystemException, IOException {
        int value = operateDecimalInt(decp1, intp2.intValue());
        dOut.write(ValueTag.XS_YEAR_MONTH_DURATION_TAG);
        dOut.writeInt(value);
    }

    @Override
    public void operateDoubleDecimal(DoublePointable doublep, XSDecimalPointable decp, DataOutput dOut)
            throws SystemException, IOException {
        double value = doublep.doubleValue();
        value /= decp.doubleValue();
        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.writeDouble(value);
    }

    @Override
    public void operateDoubleDouble(DoublePointable doublep, DoublePointable doublep2, DataOutput dOut)
            throws SystemException, IOException {
        double value = doublep.doubleValue();
        value /= doublep2.doubleValue();
        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.writeDouble(value);
    }

    @Override
    public void operateDoubleDTDuration(DoublePointable doublep, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        double value = doublep.doubleValue();
        value /= intp.intValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeInt((int) value);
    }

    @Override
    public void operateDoubleFloat(DoublePointable doublep, FloatPointable floatp, DataOutput dOut)
            throws SystemException, IOException {
        double value = doublep.doubleValue();
        value /= floatp.doubleValue();
        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.writeDouble(value);
    }

    @Override
    public void operateDoubleInteger(DoublePointable doublep, LongPointable longp, DataOutput dOut)
            throws SystemException, IOException {
        double value = doublep.doubleValue();
        value /= longp.doubleValue();
        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.writeDouble(value);
    }

    @Override
    public void operateDoubleYMDuration(DoublePointable doublep, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        double value = doublep.doubleValue();
        value /= intp.intValue();
        dOut.write(ValueTag.XS_YEAR_MONTH_DURATION_TAG);
        dOut.writeInt((int) value);
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
        value /= decp.intValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeInt(value);
    }

    @Override
    public void operateDTDurationDouble(IntegerPointable intp, DoublePointable doublep, DataOutput dOut)
            throws SystemException, IOException {
        int value = intp.intValue();
        value /= doublep.intValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeInt(value);
    }

    @Override
    public void operateDTDurationDTDuration(IntegerPointable intp, IntegerPointable intp2, DataOutput dOut)
            throws SystemException, IOException {
        int value = intp.intValue();
        value /= intp2.intValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeInt(value);
    }

    @Override
    public void operateDTDurationFloat(IntegerPointable intp, FloatPointable floatp, DataOutput dOut)
            throws SystemException, IOException {
        float value = intp.floatValue();
        value /= floatp.intValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeInt((int) value);
    }

    @Override
    public void operateDTDurationInteger(IntegerPointable intp, LongPointable longp, DataOutput dOut)
            throws SystemException, IOException {
        int value = intp.intValue();
        value /= longp.intValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeInt(value);
    }

    @Override
    public void operateDTDurationTime(IntegerPointable intp, XSTimePointable timep, DataOutput dOut)
            throws SystemException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void operateFloatDecimal(FloatPointable floatp1, XSDecimalPointable decp2, DataOutput dOut)
            throws SystemException, IOException {
        float value = floatp1.floatValue();
        value /= decp2.floatValue();
        dOut.write(ValueTag.XS_FLOAT_TAG);
        dOut.writeDouble(value);
    }

    @Override
    public void operateFloatDouble(FloatPointable floatp1, DoublePointable doublep2, DataOutput dOut)
            throws SystemException, IOException {
        double value = floatp1.doubleValue();
        value /= doublep2.doubleValue();
        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.writeDouble(value);
    }

    @Override
    public void operateFloatDTDuration(FloatPointable floatp1, IntegerPointable intp2, DataOutput dOut)
            throws SystemException, IOException {
        int value = floatp1.intValue();
        value /= intp2.intValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeInt(value);
    }

    @Override
    public void operateFloatFloat(FloatPointable floatp1, FloatPointable floatp2, DataOutput dOut)
            throws SystemException, IOException {
        float value = floatp1.floatValue();
        value /= floatp2.floatValue();
        dOut.write(ValueTag.XS_FLOAT_TAG);
        dOut.writeFloat(value);
    }

    @Override
    public void operateFloatInteger(FloatPointable floatp1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException {
        float value = floatp1.floatValue();
        value /= longp2.floatValue();
        dOut.write(ValueTag.XS_FLOAT_TAG);
        dOut.writeFloat(value);
    }

    @Override
    public void operateFloatYMDuration(FloatPointable floatp1, IntegerPointable intp2, DataOutput dOut)
            throws SystemException, IOException {
        long value = floatp1.longValue();
        value /= intp2.longValue();
        dOut.write(ValueTag.XS_YEAR_MONTH_DURATION_TAG);
        dOut.writeLong(value);
    }

    @Override
    public void operateIntegerDecimal(LongPointable longp1, XSDecimalPointable decp2, DataOutput dOut)
            throws SystemException, IOException {
        XSDecimalPointable decp1 = new XSDecimalPointable();
        decp1.setDecimal(longp1.longValue(), (byte) 0);
        operateDecimalDecimal(decp1, decp2, dOut);
    }

    @Override
    public void operateIntegerDouble(LongPointable longp, DoublePointable doublep, DataOutput dOut)
            throws SystemException, IOException {
        double value = longp.doubleValue();
        value /= doublep.doubleValue();
        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.writeDouble(value);
    }

    @Override
    public void operateIntegerDTDuration(LongPointable longp, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        int value = longp.intValue();
        value /= intp.intValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeInt(value);
    }

    @Override
    public void operateIntegerFloat(LongPointable longp, FloatPointable floatp, DataOutput dOut)
            throws SystemException, IOException {
        float value = longp.floatValue();
        value /= floatp.floatValue();
        dOut.write(ValueTag.XS_FLOAT_TAG);
        dOut.writeFloat(value);
    }

    @Override
    public void operateIntegerInteger(LongPointable longp, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException {
        // This is an exception for integer integer operations. The divide operation returns a decimal.
        double value = longp.doubleValue();
        value /= longp2.doubleValue();
        dOut.write(ValueTag.XS_DECIMAL_TAG);
        dOut.writeDouble(value);
    }

    @Override
    public void operateIntegerYMDuration(LongPointable longp, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        int value = longp.intValue();
        value /= intp.intValue();
        dOut.write(ValueTag.XS_YEAR_MONTH_DURATION_TAG);
        dOut.writeInt(value);
    }

    @Override
    public void operateTimeDTDuration(XSTimePointable timep, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void operateTimeTime(XSTimePointable timep, XSTimePointable timep2, DynamicContext dCtx, DataOutput dOut)
            throws SystemException, IOException {
        throw new UnsupportedOperationException();
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
    public void operateYMDurationDecimal(IntegerPointable intp1, XSDecimalPointable decp2, DataOutput dOut)
            throws SystemException, IOException {
        int value = operateIntDecimal(intp1.intValue(), decp2);
        dOut.write(ValueTag.XS_YEAR_MONTH_DURATION_TAG);
        dOut.writeInt(value);
    }

    @Override
    public void operateYMDurationDouble(IntegerPointable intp, DoublePointable doublep, DataOutput dOut)
            throws SystemException, IOException {
        int value = intp.intValue();
        value /= doublep.intValue();
        dOut.write(ValueTag.XS_YEAR_MONTH_DURATION_TAG);
        dOut.writeInt(value);
    }

    @Override
    public void operateYMDurationFloat(IntegerPointable intp, FloatPointable floatp, DataOutput dOut)
            throws SystemException, IOException {
        int value = intp.intValue();
        value /= floatp.intValue();
        dOut.write(ValueTag.XS_YEAR_MONTH_DURATION_TAG);
        dOut.writeInt(value);
    }

    @Override
    public void operateYMDurationInteger(IntegerPointable intp, LongPointable longp, DataOutput dOut)
            throws SystemException, IOException {
        int value = intp.intValue();
        value /= longp.intValue();
        dOut.write(ValueTag.XS_YEAR_MONTH_DURATION_TAG);
        dOut.writeInt(value);
    }

    @Override
    public void operateYMDurationYMDuration(IntegerPointable intp, IntegerPointable intp2, DataOutput dOut)
            throws SystemException, IOException {
        int value = intp.intValue();
        value /= intp2.intValue();
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
        // Divide
        if (value1 > Long.MAX_VALUE * value2) {
            throw new SystemException(ErrorCode.XPDY0002);
        }
        value1 /= value2;
        place1 -= place2;
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
        // Divide
        if (value1 > Long.MAX_VALUE * value2) {
            throw new SystemException(ErrorCode.XPDY0002);
        }
        value1 /= value2;
        place1 -= place2;
        // Save
        decp2.setDecimal(value1, place1);
        return decp2.intValue();
    }

}