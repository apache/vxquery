package org.apache.vxquery.runtime.functions.comparison;

import java.io.IOException;

import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.datamodel.accessors.atomic.XSBinaryPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDatePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDateTimePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDurationPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSQNamePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSTimePointable;
import org.apache.vxquery.exceptions.SystemException;

import edu.uci.ics.hyracks.data.std.primitive.BooleanPointable;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

public abstract class AbstractValueComparisonOperation {
    public abstract boolean operateAnyURIAnyURI(UTF8StringPointable stringp, UTF8StringPointable stringp2)
            throws SystemException, IOException;

    public abstract boolean operateBase64BinaryBase64Binary(XSBinaryPointable binaryp1, XSBinaryPointable binaryp2)
            throws SystemException, IOException;

    public abstract boolean operateBooleanBoolean(BooleanPointable boolp1, BooleanPointable boolp2)
            throws SystemException, IOException;

    public abstract boolean operateDateDate(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx)
            throws SystemException, IOException;

    public abstract boolean operateDatetimeDatetime(XSDateTimePointable datetimep1, XSDateTimePointable datetimep2,
            DynamicContext dCtx) throws SystemException, IOException;

    public abstract boolean operateDecimalDecimal(XSDecimalPointable decp1, XSDecimalPointable decp2)
            throws SystemException, IOException;

    public abstract boolean operateDecimalDouble(XSDecimalPointable decp1, DoublePointable doublep2)
            throws SystemException, IOException;

    public abstract boolean operateDecimalFloat(XSDecimalPointable decp1, FloatPointable floatp2)
            throws SystemException, IOException;

    public abstract boolean operateDecimalInteger(XSDecimalPointable decp1, LongPointable longp2)
            throws SystemException, IOException;

    public abstract boolean operateDoubleDecimal(DoublePointable doublep1, XSDecimalPointable decp2)
            throws SystemException, IOException;

    public abstract boolean operateDoubleDouble(DoublePointable doublep1, DoublePointable doublep2)
            throws SystemException, IOException;

    public abstract boolean operateDoubleFloat(DoublePointable doublep1, FloatPointable floatp2)
            throws SystemException, IOException;

    public abstract boolean operateDoubleInteger(DoublePointable doublep1, LongPointable longp2)
            throws SystemException, IOException;

    public abstract boolean operateDTDurationDTDuration(LongPointable longp1, LongPointable longp2)
            throws SystemException, IOException;

    public abstract boolean operateDTDurationDuration(LongPointable longp1, XSDurationPointable durationp2)
            throws SystemException, IOException;

    public abstract boolean operateDurationDTDuration(XSDurationPointable durationp1, LongPointable longp2)
            throws SystemException, IOException;

    public abstract boolean operateDurationDuration(XSDurationPointable durationp1, XSDurationPointable durationp2)
            throws SystemException, IOException;

    public abstract boolean operateDurationYMDuration(XSDurationPointable durationp1, IntegerPointable intp2)
            throws SystemException, IOException;

    public abstract boolean operateFloatDecimal(FloatPointable floatp1, XSDecimalPointable decp2)
            throws SystemException, IOException;

    public abstract boolean operateFloatDouble(FloatPointable floatp1, DoublePointable doublep2)
            throws SystemException, IOException;

    public abstract boolean operateFloatFloat(FloatPointable floatp1, FloatPointable floatp2) throws SystemException,
            IOException;

    public abstract boolean operateFloatInteger(FloatPointable floatp1, LongPointable longp2) throws SystemException,
            IOException;

    public abstract boolean operateGDayGDay(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx)
            throws SystemException, IOException;

    public abstract boolean operateGMonthDayGMonthDay(XSDatePointable datep1, XSDatePointable datep2,
            DynamicContext dCtx) throws SystemException, IOException;

    public abstract boolean operateGMonthGMonth(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx)
            throws SystemException, IOException;

    public abstract boolean operateGYearGYear(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx)
            throws SystemException, IOException;

    public abstract boolean operateGYearMonthGYearMonth(XSDatePointable datep1, XSDatePointable datep2,
            DynamicContext dCtx) throws SystemException, IOException;

    public abstract boolean operateHexBinaryHexBinary(XSBinaryPointable binaryp1, XSBinaryPointable binaryp2)
            throws SystemException, IOException;

    public abstract boolean operateIntegerDecimal(LongPointable longp1, XSDecimalPointable decp2)
            throws SystemException, IOException;

    public abstract boolean operateIntegerDouble(LongPointable longp1, DoublePointable doublep2)
            throws SystemException, IOException;

    public abstract boolean operateIntegerFloat(LongPointable longp1, FloatPointable floatp2) throws SystemException,
            IOException;

    public abstract boolean operateIntegerInteger(LongPointable longp1, LongPointable longp2) throws SystemException,
            IOException;

    public abstract boolean operateNotationNotation(UTF8StringPointable stringp1, UTF8StringPointable stringp2)
            throws SystemException, IOException;

    public abstract boolean operateQNameQName(XSQNamePointable qnamep1, XSQNamePointable qnamep2)
            throws SystemException, IOException;

    public abstract boolean operateStringString(UTF8StringPointable stringp1, UTF8StringPointable stringp2)
            throws SystemException, IOException;

    public abstract boolean operateTimeTime(XSTimePointable timep1, XSTimePointable timep2, DynamicContext dCtx)
            throws SystemException, IOException;

    public abstract boolean operateYMDurationDuration(IntegerPointable intp1, XSDurationPointable durationp2)
            throws SystemException, IOException;

    public abstract boolean operateYMDurationYMDuration(IntegerPointable intp1, IntegerPointable intp2)
            throws SystemException, IOException;
}