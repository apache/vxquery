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
import org.apache.vxquery.runtime.functions.cast.CastToDecimalOperation;

import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class DivideOperation extends AbstractArithmeticOperation {
    protected final ArrayBackedValueStorage abvsInner = new ArrayBackedValueStorage();
    private DoublePointable doublep = (DoublePointable) DoublePointable.FACTORY.createPointable();
    private XSDecimalPointable decp1 = (XSDecimalPointable) XSDecimalPointable.FACTORY.createPointable();
    private XSDecimalPointable decp2 = (XSDecimalPointable) XSDecimalPointable.FACTORY.createPointable();
    
    @Override
    public void operateDateDate(XSDatePointable datep, XSDatePointable datep2, DynamicContext dCtx, DataOutput dOut)
            throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public void operateDateDTDuration(XSDatePointable datep, LongPointable longp, DataOutput dOut)
            throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public void operateDatetimeDatetime(XSDateTimePointable datetimep, XSDateTimePointable datetimep2,
            DynamicContext dCtx, DataOutput dOut) throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public void operateDatetimeDTDuration(XSDateTimePointable datetimep, LongPointable longp, DataOutput dOut)
            throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public void operateDatetimeYMDuration(XSDateTimePointable datetimep, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public void operateDateYMDuration(XSDatePointable datep, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public void operateDecimalDecimal(XSDecimalPointable decp1, XSDecimalPointable decp2, DataOutput dOut)
            throws SystemException, IOException {
        // Prepare
        double value1 = decp1.doubleValue();
        double value2 = decp2.doubleValue();
        if (value2 == 0) {
            throw new SystemException(ErrorCode.FOAR0001);
        }
        // Divide
        value1 /= value2;
        // Save
        abvsInner.reset();
        doublep.set(abvsInner.getByteArray(), abvsInner.getStartOffset(), DoublePointable.TYPE_TRAITS.getFixedLength());
        doublep.setDouble(value1);
        CastToDecimalOperation castToDecmial = new CastToDecimalOperation();
        castToDecmial.convertDouble(doublep, dOut);
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
    public void operateDecimalDTDuration(XSDecimalPointable decp1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
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
        if (longp2.getLong() == 0) {
            throw new SystemException(ErrorCode.FOAR0001);
        }
        // Convert
        abvsInner.reset();
        decp2.set(abvsInner.getByteArray(), abvsInner.getStartOffset(), XSDecimalPointable.TYPE_TRAITS.getFixedLength());
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
        if (decp.getDecimalValue() == 0) {
            throw new SystemException(ErrorCode.FOAR0001);
        }
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
    public void operateDoubleDTDuration(DoublePointable doublep, LongPointable longp, DataOutput dOut)
            throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
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
        if (longp.getLong() == 0) {
            throw new SystemException(ErrorCode.FOAR0001);
        }
        double value = doublep.doubleValue();
        value /= longp.doubleValue();
        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.writeDouble(value);
    }

    @Override
    public void operateDoubleYMDuration(DoublePointable doublep, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public void operateDTDurationDate(LongPointable longp, XSDatePointable datep, DataOutput dOut)
            throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public void operateDTDurationDatetime(LongPointable longp, XSDateTimePointable datetimep, DataOutput dOut)
            throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public void operateDTDurationDecimal(LongPointable longp, XSDecimalPointable decp, DataOutput dOut)
            throws SystemException, IOException {
        if (decp.getDecimalValue() == 0) {
            throw new SystemException(ErrorCode.FODT0002);
        }
        long value = longp.longValue();
        value /= decp.longValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeLong(value);
    }

    @Override
    public void operateDTDurationDouble(LongPointable longp, DoublePointable doublep, DataOutput dOut)
            throws SystemException, IOException {
        if (Double.isNaN(doublep.doubleValue())) {
            throw new SystemException(ErrorCode.FOCA0005);
        }
        if (doublep.doubleValue() == 0.0) {
            throw new SystemException(ErrorCode.FODT0002);
        }
        double value = longp.doubleValue();
        value /= doublep.doubleValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeLong((long) value);
    }

    @Override
    public void operateDTDurationDTDuration(LongPointable longp1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException {
        double value = longp1.longValue();
        value /= longp2.longValue();

        abvsInner.reset();
        doublep.set(abvsInner.getByteArray(), abvsInner.getStartOffset(), DoublePointable.TYPE_TRAITS.getFixedLength());
        doublep.setDouble(value);
        CastToDecimalOperation castToDecmial = new CastToDecimalOperation();
        castToDecmial.convertDouble(doublep, dOut);
    }

    @Override
    public void operateDTDurationFloat(LongPointable longp, FloatPointable floatp, DataOutput dOut)
            throws SystemException, IOException {
        if (Float.isNaN(floatp.floatValue())) {
            throw new SystemException(ErrorCode.FOCA0005);
        }
        if (floatp.floatValue() == 0.0f) {
            throw new SystemException(ErrorCode.FODT0002);
        }
        float value = longp.floatValue();
        value /= floatp.intValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeLong((long) value);
    }

    @Override
    public void operateDTDurationInteger(LongPointable longp1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException {
        if (longp2.getLong() == 0) {
            throw new SystemException(ErrorCode.FODT0002);
        }
        long value = longp1.longValue();
        value /= longp2.longValue();
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeLong(value);
    }

    @Override
    public void operateDTDurationTime(LongPointable longp, XSTimePointable timep, DataOutput dOut)
            throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public void operateFloatDecimal(FloatPointable floatp1, XSDecimalPointable decp2, DataOutput dOut)
            throws SystemException, IOException {
        if (decp2.getDecimalValue() == 0) {
            throw new SystemException(ErrorCode.FOAR0001);
        }
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
    public void operateFloatDTDuration(FloatPointable floatp1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
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
        if (longp2.getLong() == 0) {
            throw new SystemException(ErrorCode.FOAR0001);
        }
        float value = floatp1.floatValue();
        value /= longp2.floatValue();
        dOut.write(ValueTag.XS_FLOAT_TAG);
        dOut.writeFloat(value);
    }

    @Override
    public void operateFloatYMDuration(FloatPointable floatp1, IntegerPointable intp2, DataOutput dOut)
            throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public void operateIntegerDecimal(LongPointable longp1, XSDecimalPointable decp2, DataOutput dOut)
            throws SystemException, IOException {
        if (decp2.getDecimalValue() == 0) {
            throw new SystemException(ErrorCode.FOAR0001);
        }
        abvsInner.reset();
        decp1.set(abvsInner.getByteArray(), abvsInner.getStartOffset(), XSDecimalPointable.TYPE_TRAITS.getFixedLength());
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
    public void operateIntegerDTDuration(LongPointable longp1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
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
        if (longp2.getLong() == 0) {
            throw new SystemException(ErrorCode.FOAR0001);
        }
        // This is an exception for integer integer operations. The divide operation returns a decimal.
        double value = longp.doubleValue();
        value /= longp2.doubleValue();

        abvsInner.reset();
        doublep.set(abvsInner.getByteArray(), abvsInner.getStartOffset(), DoublePointable.TYPE_TRAITS.getFixedLength());
        doublep.setDouble(value);
        CastToDecimalOperation castToDecmial = new CastToDecimalOperation();
        castToDecmial.convertDouble(doublep, dOut);
    }

    @Override
    public void operateIntegerYMDuration(LongPointable longp, IntegerPointable intp, DataOutput dOut)
            throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public void operateTimeDTDuration(XSTimePointable timep, LongPointable longp, DataOutput dOut)
            throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public void operateTimeTime(XSTimePointable timep, XSTimePointable timep2, DynamicContext dCtx, DataOutput dOut)
            throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public void operateYMDurationDate(IntegerPointable intp, XSDatePointable datep, DataOutput dOut)
            throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public void operateYMDurationDatetime(IntegerPointable intp, XSDateTimePointable datetimep, DataOutput dOut)
            throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
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
        double value = intp.doubleValue();
        value /= doublep.doubleValue();
        dOut.write(ValueTag.XS_YEAR_MONTH_DURATION_TAG);
        dOut.writeInt((int) value);
    }

    @Override
    public void operateYMDurationFloat(IntegerPointable intp, FloatPointable floatp, DataOutput dOut)
            throws SystemException, IOException {
        float value = intp.floatValue();
        value /= floatp.floatValue();
        dOut.write(ValueTag.XS_YEAR_MONTH_DURATION_TAG);
        dOut.writeInt((int) value);
    }

    @Override
    public void operateYMDurationInteger(IntegerPointable intp, LongPointable longp, DataOutput dOut)
            throws SystemException, IOException {
        if (longp.getLong() == 0) {
            throw new SystemException(ErrorCode.FOAR0001);
        }
        int value = intp.intValue();
        value /= longp.intValue();
        dOut.write(ValueTag.XS_YEAR_MONTH_DURATION_TAG);
        dOut.writeInt(value);
    }

    @Override
    public void operateYMDurationYMDuration(IntegerPointable intp, IntegerPointable intp2, DataOutput dOut)
            throws SystemException, IOException {
        if (intp2.getInteger() == 0) {
            throw new SystemException(ErrorCode.FOAR0001);
        }
        double value = intp.intValue();
        value /= intp2.intValue();

        abvsInner.reset();
        doublep.set(abvsInner.getByteArray(), abvsInner.getStartOffset(), DoublePointable.TYPE_TRAITS.getFixedLength());
        doublep.setDouble(value);
        CastToDecimalOperation castToDecmial = new CastToDecimalOperation();
        castToDecmial.convertDouble(doublep, dOut);
    }

    public int operateIntDecimal(int intValue, XSDecimalPointable decp2) throws SystemException, IOException {
        abvsInner.reset();
        // Prepare
        double value1 = intValue;
        double value2 = decp2.doubleValue();
        if (value2 == 0) {
            throw new SystemException(ErrorCode.FOAR0001);
        }
        // Divide
        value1 /= value2;
        // Save
        return (int) value1;
    }

    public int operateDecimalInt(XSDecimalPointable decp1, int intValue) throws SystemException, IOException {
        abvsInner.reset();
        // Prepare
        double value1 = decp1.doubleValue();
        double value2 = intValue;
        if (value2 == 0) {
            throw new SystemException(ErrorCode.FOAR0001);
        }
        // Divide
        value1 /= value2;
        // Save
        return (int) value1;
    }

}