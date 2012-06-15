package org.apache.vxquery.runtime.functions.arithmetic;

import java.io.DataOutput;

import org.apache.vxquery.datamodel.accessors.atomic.XSDatePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDateTimePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSTimePointable;

import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;

public abstract class AbstractArithmeticOperation {
    public abstract void operateDecimalDecimal(XSDecimalPointable decp1, XSDecimalPointable decp2, DataOutput dOut)
            throws Exception;

    public abstract void operateDecimalInteger(XSDecimalPointable decp1, LongPointable longp2, DataOutput dOut)
            throws Exception;

    public abstract void operateDecimalFloat(XSDecimalPointable decp, FloatPointable floatp, DataOutput dOut)
            throws Exception;

    public abstract void operateDecimalDouble(XSDecimalPointable decp, DoublePointable doublep, DataOutput dOut)
            throws Exception;

    public abstract void operateDecimalDTDuration(XSDecimalPointable decp, IntegerPointable intp, DataOutput dOut)
            throws Exception;

    public abstract void operateDecimalYMDuration(XSDecimalPointable decp, IntegerPointable intp, DataOutput dOut)
            throws Exception;

    public abstract void operateIntegerDecimal(LongPointable longp, XSDecimalPointable decp, DataOutput dOut)
            throws Exception;

    public abstract void operateIntegerInteger(LongPointable longp, LongPointable longp2, DataOutput dOut)
            throws Exception;

    public abstract void operateIntegerFloat(LongPointable longp, FloatPointable floatp, DataOutput dOut)
            throws Exception;

    public abstract void operateIntegerDouble(LongPointable longp, DoublePointable doublep, DataOutput dOut)
            throws Exception;

    public abstract void operateIntegerDTDuration(LongPointable longp, IntegerPointable intp, DataOutput dOut)
            throws Exception;

    public abstract void operateIntegerYMDuration(LongPointable longp, IntegerPointable intp, DataOutput dOut)
            throws Exception;

    public abstract void operateFloatDecimal(FloatPointable floatp, XSDecimalPointable decp, DataOutput dOut)
            throws Exception;

    public abstract void operateFloatInteger(FloatPointable floatp, LongPointable longp, DataOutput dOut)
            throws Exception;

    public abstract void operateFloatFloat(FloatPointable floatp, FloatPointable floatp2, DataOutput dOut)
            throws Exception;

    public abstract void operateFloatDouble(FloatPointable floatp, DoublePointable doublep, DataOutput dOut)
            throws Exception;

    public abstract void operateFloatDTDuration(FloatPointable floatp, IntegerPointable intp, DataOutput dOut)
            throws Exception;

    public abstract void operateFloatYMDuration(FloatPointable floatp, IntegerPointable intp, DataOutput dOut)
            throws Exception;

    public abstract void operateDoubleDecimal(DoublePointable doublep, XSDecimalPointable decp, DataOutput dOut)
            throws Exception;

    public abstract void operateDoubleInteger(DoublePointable doublep, LongPointable longp, DataOutput dOut)
            throws Exception;

    public abstract void operateDoubleFloat(DoublePointable doublep, FloatPointable floatp, DataOutput dOut)
            throws Exception;

    public abstract void operateDoubleDouble(DoublePointable doublep, DoublePointable doublep2, DataOutput dOut)
            throws Exception;

    public abstract void operateDoubleDTDuration(DoublePointable doublep, IntegerPointable intp, DataOutput dOut)
            throws Exception;

    public abstract void operateDoubleYMDuration(DoublePointable doublep, IntegerPointable intp, DataOutput dOut)
            throws Exception;

    public abstract void operateDateDate(XSDatePointable datep, XSDatePointable datep2, DataOutput dOut)
            throws Exception;

    public abstract void operateDateDTDuration(XSDatePointable datep, IntegerPointable intp, DataOutput dOut)
            throws Exception;

    public abstract void operateDateYMDuration(XSDatePointable datep, IntegerPointable intp, DataOutput dOut)
            throws Exception;

    public abstract void operateDatetimeDatetime(XSDateTimePointable datetimep, XSDateTimePointable datetimep2,
            DataOutput dOut) throws Exception;

    public abstract void operateDatetimeDTDuration(XSDateTimePointable datetimep, IntegerPointable intp, DataOutput dOut)
            throws Exception;

    public abstract void operateDatetimeYMDuration(XSDateTimePointable datetimep, IntegerPointable intp, DataOutput dOut)
            throws Exception;

    public abstract void operateTimeTime(XSTimePointable timep, XSTimePointable timep2, DataOutput dOut)
            throws Exception;

    public abstract void operateTimeDTDuration(XSTimePointable timep, IntegerPointable intp, DataOutput dOut)
            throws Exception;

    public abstract void operateTimeYMDuration(XSTimePointable timep, IntegerPointable intp, DataOutput dOut)
            throws Exception;

    public abstract void operateDTDurationDecimal(IntegerPointable intp, XSDecimalPointable decp, DataOutput dOut)
            throws Exception;

    public abstract void operateDTDurationInteger(IntegerPointable intp, LongPointable longp, DataOutput dOut)
            throws Exception;

    public abstract void operateDTDurationFloat(IntegerPointable intp, FloatPointable floatp, DataOutput dOut)
            throws Exception;

    public abstract void operateDTDurationDouble(IntegerPointable intp, DoublePointable doublep, DataOutput dOut)
            throws Exception;

    public abstract void operateDTDurationDate(IntegerPointable intp, XSDatePointable datep, DataOutput dOut)
            throws Exception;

    public abstract void operateDTDurationTime(IntegerPointable intp, XSTimePointable timep, DataOutput dOut)
            throws Exception;

    public abstract void operateDTDurationDatetime(IntegerPointable intp, XSDateTimePointable datetimep, DataOutput dOut)
            throws Exception;

    public abstract void operateDTDurationDTDuration(IntegerPointable intp, IntegerPointable intp2, DataOutput dOut)
            throws Exception;

    public abstract void operateYMDurationDecimal(IntegerPointable intp, XSDecimalPointable decp, DataOutput dOut)
            throws Exception;

    public abstract void operateYMDurationInteger(IntegerPointable intp, LongPointable longp, DataOutput dOut)
            throws Exception;

    public abstract void operateYMDurationFloat(IntegerPointable intp, FloatPointable floatp, DataOutput dOut)
            throws Exception;

    public abstract void operateYMDurationDouble(IntegerPointable intp, DoublePointable doublep, DataOutput dOut)
            throws Exception;

    public abstract void operateYMDurationDate(IntegerPointable intp, XSDatePointable datep, DataOutput dOut)
            throws Exception;

    public abstract void operateYMDurationTime(IntegerPointable intp, XSTimePointable timep, DataOutput dOut)
            throws Exception;

    public abstract void operateYMDurationDatetime(IntegerPointable intp, XSDateTimePointable datetimep, DataOutput dOut)
            throws Exception;

    public abstract void operateYMDurationYMDuration(IntegerPointable intp, IntegerPointable intp2, DataOutput dOut)
            throws Exception;
}