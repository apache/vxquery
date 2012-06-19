package org.apache.vxquery.runtime.functions.arithmetic;

import java.io.DataOutput;

import org.apache.vxquery.datamodel.accessors.atomic.XSDatePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDateTimePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSTimePointable;
import org.apache.vxquery.types.BuiltinTypeConstants;

import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;

public class MultiplyOperation extends AbstractArithmeticOperation {
    @Override
    public void operateDecimalDecimal(XSDecimalPointable decp1, XSDecimalPointable decp2, DataOutput dOut)
            throws Exception {
        double value = decp1.doubleValue();
        value *= decp2.doubleValue();
        dOut.write(BuiltinTypeConstants.XS_DECIMAL_TYPE_ID);
        dOut.writeDouble(value);
    }

    @Override
    public void operateDecimalInteger(XSDecimalPointable decp, LongPointable longp, DataOutput dOut) throws Exception {
        double value = decp.doubleValue();
        value *= longp.doubleValue();
        dOut.write(BuiltinTypeConstants.XS_DECIMAL_TYPE_ID);
        dOut.writeDouble(value);
    }

    @Override
    public void operateDecimalFloat(XSDecimalPointable decp, FloatPointable floatp, DataOutput dOut) throws Exception {
        float value = decp.floatValue();
        value *= floatp.floatValue();
        dOut.write(BuiltinTypeConstants.XS_FLOAT_TYPE_ID);
        dOut.writeFloat(value);
    }

    @Override
    public void operateDecimalDouble(XSDecimalPointable decp, DoublePointable doublep, DataOutput dOut)
            throws Exception {
        double value = decp.doubleValue();
        value *= doublep.doubleValue();
        dOut.write(BuiltinTypeConstants.XS_DOUBLE_TYPE_ID);
        dOut.writeDouble(value);
    }

    @Override
    public void operateDecimalDTDuration(XSDecimalPointable decp, IntegerPointable intp, DataOutput dOut)
            throws Exception {
        long value = decp.longValue();
        value *= intp.longValue();
        dOut.write(BuiltinTypeConstants.XS_DAY_TIME_DURATION_TYPE_ID);
        dOut.writeLong(value);
    }

    @Override
    public void operateDecimalYMDuration(XSDecimalPointable decp, IntegerPointable intp, DataOutput dOut)
            throws Exception {
        long value = decp.longValue();
        value *= intp.longValue();
        dOut.write(BuiltinTypeConstants.XS_YEAR_MONTH_DURATION_TYPE_ID);
        dOut.writeLong(value);
    }

    @Override
    public void operateIntegerDecimal(LongPointable longp, XSDecimalPointable decp, DataOutput dOut) throws Exception {
        operateDecimalInteger(decp, longp, dOut);
    }

    @Override
    public void operateIntegerInteger(LongPointable longp, LongPointable longp2, DataOutput dOut) throws Exception {
        long value = longp.getLong();
        value *= longp2.getLong();
        dOut.write(BuiltinTypeConstants.XS_INTEGER_TYPE_ID);
        dOut.writeLong(value);
    }

    @Override
    public void operateIntegerFloat(LongPointable longp, FloatPointable floatp, DataOutput dOut) throws Exception {
        operateFloatInteger(floatp, longp, dOut);
    }

    @Override
    public void operateIntegerDouble(LongPointable longp, DoublePointable doublep, DataOutput dOut) throws Exception {
        operateDoubleInteger(doublep, longp, dOut);
    }

    @Override
    public void operateIntegerDTDuration(LongPointable longp, IntegerPointable intp, DataOutput dOut) throws Exception {
        operateDTDurationInteger(intp, longp, dOut);
    }

    @Override
    public void operateIntegerYMDuration(LongPointable longp, IntegerPointable intp, DataOutput dOut) throws Exception {
        long value = longp.longValue();
        value *= intp.longValue();
        dOut.write(BuiltinTypeConstants.XS_YEAR_MONTH_DURATION_TYPE_ID);
        dOut.writeLong(value);
    }

    @Override
    public void operateFloatDecimal(FloatPointable floatp, XSDecimalPointable decp, DataOutput dOut) throws Exception {
        operateDecimalFloat(decp, floatp, dOut);
    }

    @Override
    public void operateFloatInteger(FloatPointable floatp, LongPointable longp, DataOutput dOut) throws Exception {
        float value = floatp.floatValue();
        value *= longp.floatValue();
        dOut.write(BuiltinTypeConstants.XS_FLOAT_TYPE_ID);
        dOut.writeFloat(value);
    }

    @Override
    public void operateFloatFloat(FloatPointable floatp, FloatPointable floatp2, DataOutput dOut) throws Exception {
        float value = floatp.floatValue();
        value *= floatp2.floatValue();
        dOut.write(BuiltinTypeConstants.XS_FLOAT_TYPE_ID);
        dOut.writeFloat(value);
    }

    @Override
    public void operateFloatDouble(FloatPointable floatp, DoublePointable doublep, DataOutput dOut) throws Exception {
        operateDoubleFloat(doublep, floatp, dOut);
    }

    @Override
    public void operateFloatDTDuration(FloatPointable floatp, IntegerPointable intp, DataOutput dOut) throws Exception {
        operateDTDurationFloat(intp, floatp, dOut);
    }

    @Override
    public void operateFloatYMDuration(FloatPointable floatp, IntegerPointable intp, DataOutput dOut) throws Exception {
        operateYMDurationFloat(intp, floatp, dOut);
    }

    @Override
    public void operateDoubleDecimal(DoublePointable doublep, XSDecimalPointable decp, DataOutput dOut)
            throws Exception {
        operateDecimalDouble(decp, doublep, dOut);
    }

    @Override
    public void operateDoubleInteger(DoublePointable doublep, LongPointable longp, DataOutput dOut) throws Exception {
        double value = doublep.doubleValue();
        value *= longp.doubleValue();
        dOut.write(BuiltinTypeConstants.XS_DOUBLE_TYPE_ID);
        dOut.writeDouble(value);
    }

    @Override
    public void operateDoubleFloat(DoublePointable doublep, FloatPointable floatp, DataOutput dOut) throws Exception {
        double value = doublep.doubleValue();
        value *= floatp.doubleValue();
        dOut.write(BuiltinTypeConstants.XS_DOUBLE_TYPE_ID);
        dOut.writeDouble(value);
    }

    @Override
    public void operateDoubleDouble(DoublePointable doublep, DoublePointable doublep2, DataOutput dOut)
            throws Exception {
        double value = doublep.doubleValue();
        value *= doublep2.doubleValue();
        dOut.write(BuiltinTypeConstants.XS_DOUBLE_TYPE_ID);
        dOut.writeDouble(value);
    }

    @Override
    public void operateDoubleDTDuration(DoublePointable doublep, IntegerPointable intp, DataOutput dOut)
            throws Exception {
operateDTDurationDouble(intp, doublep, dOut);
    }

    @Override
    public void operateDoubleYMDuration(DoublePointable doublep, IntegerPointable intp, DataOutput dOut)
            throws Exception {
        long value = doublep.longValue();
        value *= intp.longValue();
        dOut.write(BuiltinTypeConstants.XS_YEAR_MONTH_DURATION_TYPE_ID);
        dOut.writeLong(value);
    }

    @Override
    public void operateDateDate(XSDatePointable datep, XSDatePointable datep2, DataOutput dOut) throws Exception {

    }

    @Override
    public void operateDateDTDuration(XSDatePointable datep, IntegerPointable intp, DataOutput dOut) throws Exception {

    }

    @Override
    public void operateDateYMDuration(XSDatePointable datep, IntegerPointable intp, DataOutput dOut) throws Exception {

    }

    @Override
    public void operateDatetimeDatetime(XSDateTimePointable datetimep, XSDateTimePointable datetimep2, DataOutput dOut)
            throws Exception {

    }

    @Override
    public void operateDatetimeDTDuration(XSDateTimePointable datetimep, IntegerPointable intp, DataOutput dOut)
            throws Exception {

    }

    @Override
    public void operateDatetimeYMDuration(XSDateTimePointable datetimep, IntegerPointable intp, DataOutput dOut)
            throws Exception {

    }

    @Override
    public void operateTimeTime(XSTimePointable timep, XSTimePointable timep2, DataOutput dOut) throws Exception {

    }

    @Override
    public void operateTimeDTDuration(XSTimePointable timep, IntegerPointable intp, DataOutput dOut) throws Exception {

    }

    @Override
    public void operateTimeYMDuration(XSTimePointable timep, IntegerPointable intp, DataOutput dOut) throws Exception {

    }

    @Override
    public void operateDTDurationDecimal(IntegerPointable intp, XSDecimalPointable decp, DataOutput dOut)
            throws Exception {
        operateDecimalDTDuration(decp, intp, dOut);
    }

    @Override
    public void operateDTDurationInteger(IntegerPointable intp, LongPointable longp, DataOutput dOut) throws Exception {
        long value = intp.longValue();
        value *= longp.longValue();
        dOut.write(BuiltinTypeConstants.XS_DAY_TIME_DURATION_TYPE_ID);
        dOut.writeLong(value);
    }

    @Override
    public void operateDTDurationFloat(IntegerPointable intp, FloatPointable floatp, DataOutput dOut) throws Exception {
        long value = intp.longValue();
        value *= floatp.longValue();
        dOut.write(BuiltinTypeConstants.XS_DAY_TIME_DURATION_TYPE_ID);
        dOut.writeLong(value);
    }

    @Override
    public void operateDTDurationDouble(IntegerPointable intp, DoublePointable doublep, DataOutput dOut)
            throws Exception {
        long value = intp.longValue();
        value *= doublep.longValue();
        dOut.write(BuiltinTypeConstants.XS_DAY_TIME_DURATION_TYPE_ID);
        dOut.writeLong(value);
    }

    @Override
    public void operateDTDurationDate(IntegerPointable intp, XSDatePointable datep, DataOutput dOut) throws Exception {

    }

    @Override
    public void operateDTDurationTime(IntegerPointable intp, XSTimePointable timep, DataOutput dOut) throws Exception {

    }

    @Override
    public void operateDTDurationDatetime(IntegerPointable intp, XSDateTimePointable datetimep, DataOutput dOut)
            throws Exception {

    }

    @Override
    public void operateDTDurationDTDuration(IntegerPointable intp, IntegerPointable intp2, DataOutput dOut)
            throws Exception {
        long value = intp.longValue();
        value *= intp2.longValue();
        dOut.write(BuiltinTypeConstants.XS_DAY_TIME_DURATION_TYPE_ID);
        dOut.writeLong(value);
    }

    @Override
    public void operateYMDurationDecimal(IntegerPointable intp, XSDecimalPointable decp, DataOutput dOut)
            throws Exception {
        operateDecimalYMDuration(decp, intp, dOut);
    }

    @Override
    public void operateYMDurationInteger(IntegerPointable intp, LongPointable longp, DataOutput dOut) throws Exception {
        operateIntegerYMDuration(longp, intp, dOut);
    }

    @Override
    public void operateYMDurationFloat(IntegerPointable intp, FloatPointable floatp, DataOutput dOut) throws Exception {
        operateFloatYMDuration(floatp, intp, dOut);
    }

    @Override
    public void operateYMDurationDouble(IntegerPointable intp, DoublePointable doublep, DataOutput dOut)
            throws Exception {
        operateDoubleYMDuration(doublep, intp, dOut);
    }

    @Override
    public void operateYMDurationDate(IntegerPointable intp, XSDatePointable datep, DataOutput dOut) throws Exception {

    }

    @Override
    public void operateYMDurationTime(IntegerPointable intp, XSTimePointable timep, DataOutput dOut) throws Exception {

    }

    @Override
    public void operateYMDurationDatetime(IntegerPointable intp, XSDateTimePointable datetimep, DataOutput dOut)
            throws Exception {

    }

    @Override
    public void operateYMDurationYMDuration(IntegerPointable intp, IntegerPointable intp2, DataOutput dOut)
            throws Exception {
        long value = intp.longValue();
        value *= intp2.longValue();
        dOut.write(BuiltinTypeConstants.XS_YEAR_MONTH_DURATION_TYPE_ID);
        dOut.writeLong(value);
    }
}