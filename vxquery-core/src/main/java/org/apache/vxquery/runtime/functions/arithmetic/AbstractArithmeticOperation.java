package org.apache.vxquery.runtime.functions.arithmetic;

import java.io.DataOutput;

import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.datamodel.accessors.atomic.XSDatePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDateTimePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSTimePointable;

import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;

public abstract class AbstractArithmeticOperation {
    public abstract void operateDateDate(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx,
            DataOutput dOut) throws Exception;

    public abstract void operateDateDTDuration(XSDatePointable datep1, IntegerPointable intp2, DataOutput dOut)
            throws Exception;

    public abstract void operateDatetimeDatetime(XSDateTimePointable datetimep1, XSDateTimePointable datetimep2,
            DynamicContext dCtx, DataOutput dOut) throws Exception;

    public abstract void operateDatetimeDTDuration(XSDateTimePointable datetimep1, IntegerPointable intp2, DataOutput dOut)
            throws Exception;

    public abstract void operateDatetimeYMDuration(XSDateTimePointable datetimep1, IntegerPointable intp2, DataOutput dOut)
            throws Exception;

    public abstract void operateDateYMDuration(XSDatePointable datep1, IntegerPointable intp2, DataOutput dOut)
            throws Exception;

    public abstract void operateDecimalDecimal(XSDecimalPointable decp1, XSDecimalPointable decp2, DataOutput dOut)
            throws Exception;

    public abstract void operateDecimalDouble(XSDecimalPointable decp1, DoublePointable doublep2, DataOutput dOut)
            throws Exception;

    public abstract void operateDecimalDTDuration(XSDecimalPointable decp1, IntegerPointable intp2, DataOutput dOut)
            throws Exception;

    public abstract void operateDecimalFloat(XSDecimalPointable decp1, FloatPointable floatp2, DataOutput dOut)
            throws Exception;

    public abstract void operateDecimalInteger(XSDecimalPointable decp1, LongPointable longp2, DataOutput dOut)
            throws Exception;

    public abstract void operateDecimalYMDuration(XSDecimalPointable decp1, IntegerPointable intp2, DataOutput dOut)
            throws Exception;

    public abstract void operateDoubleDecimal(DoublePointable doublep1, XSDecimalPointable decp2, DataOutput dOut)
            throws Exception;

    public abstract void operateDoubleDouble(DoublePointable doublep1, DoublePointable doublep2, DataOutput dOut)
            throws Exception;

    public abstract void operateDoubleDTDuration(DoublePointable doublep1, IntegerPointable intp2, DataOutput dOut)
            throws Exception;

    public abstract void operateDoubleFloat(DoublePointable doublep1, FloatPointable floatp2, DataOutput dOut)
            throws Exception;

    public abstract void operateDoubleInteger(DoublePointable doublep1, LongPointable longp2, DataOutput dOut)
            throws Exception;

    public abstract void operateDoubleYMDuration(DoublePointable doublep1, IntegerPointable intp2, DataOutput dOut)
            throws Exception;

    public abstract void operateDTDurationDate(IntegerPointable intp1, XSDatePointable datep2, DataOutput dOut)
            throws Exception;

    public abstract void operateDTDurationDatetime(IntegerPointable intp1, XSDateTimePointable datetimep2, DataOutput dOut)
            throws Exception;

    public abstract void operateDTDurationDecimal(IntegerPointable intp1, XSDecimalPointable decp2, DataOutput dOut)
            throws Exception;

    public abstract void operateDTDurationDouble(IntegerPointable intp1, DoublePointable doublep2, DataOutput dOut)
            throws Exception;

    public abstract void operateDTDurationDTDuration(IntegerPointable intp1, IntegerPointable intp2, DataOutput dOut)
            throws Exception;

    public abstract void operateDTDurationFloat(IntegerPointable intp1, FloatPointable floatp2, DataOutput dOut)
            throws Exception;

    public abstract void operateDTDurationInteger(IntegerPointable intp1, LongPointable longp2, DataOutput dOut)
            throws Exception;

    public abstract void operateDTDurationTime(IntegerPointable intp1, XSTimePointable timep2, DataOutput dOut)
            throws Exception;

    public abstract void operateFloatDecimal(FloatPointable floatp1, XSDecimalPointable decp2, DataOutput dOut)
            throws Exception;

    public abstract void operateFloatDouble(FloatPointable floatp1, DoublePointable doublep2, DataOutput dOut)
            throws Exception;

    public abstract void operateFloatDTDuration(FloatPointable floatp1, IntegerPointable intp2, DataOutput dOut)
            throws Exception;

    public abstract void operateFloatFloat(FloatPointable floatp1, FloatPointable floatp2, DataOutput dOut)
            throws Exception;

    public abstract void operateFloatInteger(FloatPointable floatp1, LongPointable longp2, DataOutput dOut)
            throws Exception;

    public abstract void operateFloatYMDuration(FloatPointable floatp1, IntegerPointable intp2, DataOutput dOut)
            throws Exception;

    public abstract void operateIntegerDecimal(LongPointable longp1, XSDecimalPointable decp2, DataOutput dOut)
            throws Exception;

    public abstract void operateIntegerDouble(LongPointable longp1, DoublePointable doublep2, DataOutput dOut)
            throws Exception;

    public abstract void operateIntegerDTDuration(LongPointable longp1, IntegerPointable intp2, DataOutput dOut)
            throws Exception;

    public abstract void operateIntegerFloat(LongPointable longp1, FloatPointable floatp2, DataOutput dOut)
            throws Exception;

    public abstract void operateIntegerInteger(LongPointable longp1, LongPointable longp2, DataOutput dOut)
            throws Exception;

    public abstract void operateIntegerYMDuration(LongPointable longp1, IntegerPointable intp2, DataOutput dOut)
            throws Exception;

    public abstract void operateTimeDTDuration(XSTimePointable timep1, IntegerPointable intp2, DataOutput dOut)
            throws Exception;

    public abstract void operateTimeTime(XSTimePointable timep1, XSTimePointable timep2, DynamicContext dCtx,
            DataOutput dOut) throws Exception;

    public abstract void operateYMDurationDate(IntegerPointable intp1, XSDatePointable datep2, DataOutput dOut)
            throws Exception;

    public abstract void operateYMDurationDatetime(IntegerPointable intp1, XSDateTimePointable datetimep2, DataOutput dOut)
            throws Exception;

    public abstract void operateYMDurationDecimal(IntegerPointable intp1, XSDecimalPointable decp2, DataOutput dOut)
            throws Exception;

    public abstract void operateYMDurationDouble(IntegerPointable intp1, DoublePointable doublep2, DataOutput dOut)
            throws Exception;

    public abstract void operateYMDurationFloat(IntegerPointable intp1, FloatPointable floatp2, DataOutput dOut)
            throws Exception;

    public abstract void operateYMDurationInteger(IntegerPointable intp1, LongPointable longp2, DataOutput dOut)
            throws Exception;

    public abstract void operateYMDurationYMDuration(IntegerPointable intp1, IntegerPointable intp2, DataOutput dOut)
            throws Exception;
}