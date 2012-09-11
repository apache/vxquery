package org.apache.vxquery.runtime.functions.arithmetic;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.datamodel.accessors.atomic.XSDatePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDateTimePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSTimePointable;
import org.apache.vxquery.exceptions.SystemException;

import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;

public abstract class AbstractArithmeticOperation {
    public abstract void operateDateDate(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx,
            DataOutput dOut) throws SystemException, IOException;

    public abstract void operateDateDTDuration(XSDatePointable datep1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateDatetimeDatetime(XSDateTimePointable datetimep1, XSDateTimePointable datetimep2,
            DynamicContext dCtx, DataOutput dOut) throws SystemException, IOException;

    public abstract void operateDatetimeDTDuration(XSDateTimePointable datetimep1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateDatetimeYMDuration(XSDateTimePointable datetimep1, IntegerPointable intp2,
            DataOutput dOut) throws SystemException, IOException;

    public abstract void operateDateYMDuration(XSDatePointable datep1, IntegerPointable intp2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateDecimalDecimal(XSDecimalPointable decp1, XSDecimalPointable decp2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateDecimalDouble(XSDecimalPointable decp1, DoublePointable doublep2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateDecimalDTDuration(XSDecimalPointable decp1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateDecimalFloat(XSDecimalPointable decp1, FloatPointable floatp2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateDecimalInteger(XSDecimalPointable decp1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateDecimalYMDuration(XSDecimalPointable decp1, IntegerPointable intp2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateDoubleDecimal(DoublePointable doublep1, XSDecimalPointable decp2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateDoubleDouble(DoublePointable doublep1, DoublePointable doublep2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateDoubleDTDuration(DoublePointable doublep1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateDoubleFloat(DoublePointable doublep1, FloatPointable floatp2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateDoubleInteger(DoublePointable doublep1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateDoubleYMDuration(DoublePointable doublep1, IntegerPointable intp2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateDTDurationDate(LongPointable longp1, XSDatePointable datep2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateDTDurationDatetime(LongPointable longp1, XSDateTimePointable datetimep2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateDTDurationDecimal(LongPointable longp1, XSDecimalPointable decp2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateDTDurationDouble(LongPointable longp1, DoublePointable doublep2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateDTDurationDTDuration(LongPointable longp1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateDTDurationFloat(LongPointable longp1, FloatPointable floatp2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateDTDurationInteger(LongPointable longp1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateDTDurationTime(LongPointable longp1, XSTimePointable timep2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateFloatDecimal(FloatPointable floatp1, XSDecimalPointable decp2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateFloatDouble(FloatPointable floatp1, DoublePointable doublep2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateFloatDTDuration(FloatPointable floatp1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateFloatFloat(FloatPointable floatp1, FloatPointable floatp2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateFloatInteger(FloatPointable floatp1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateFloatYMDuration(FloatPointable floatp1, IntegerPointable intp2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateIntegerDecimal(LongPointable longp1, XSDecimalPointable decp2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateIntegerDouble(LongPointable longp1, DoublePointable doublep2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateIntegerDTDuration(LongPointable longp1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateIntegerFloat(LongPointable longp1, FloatPointable floatp2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateIntegerInteger(LongPointable longp1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateIntegerYMDuration(LongPointable longp1, IntegerPointable intp2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateTimeDTDuration(XSTimePointable timep1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateTimeTime(XSTimePointable timep1, XSTimePointable timep2, DynamicContext dCtx,
            DataOutput dOut) throws SystemException, IOException;

    public abstract void operateYMDurationDate(IntegerPointable intp1, XSDatePointable datep2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateYMDurationDatetime(IntegerPointable intp1, XSDateTimePointable datetimep2,
            DataOutput dOut) throws SystemException, IOException;

    public abstract void operateYMDurationDecimal(IntegerPointable intp1, XSDecimalPointable decp2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateYMDurationDouble(IntegerPointable intp1, DoublePointable doublep2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateYMDurationFloat(IntegerPointable intp1, FloatPointable floatp2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateYMDurationInteger(IntegerPointable intp1, LongPointable longp2, DataOutput dOut)
            throws SystemException, IOException;

    public abstract void operateYMDurationYMDuration(IntegerPointable intp1, IntegerPointable intp2, DataOutput dOut)
            throws SystemException, IOException;
}