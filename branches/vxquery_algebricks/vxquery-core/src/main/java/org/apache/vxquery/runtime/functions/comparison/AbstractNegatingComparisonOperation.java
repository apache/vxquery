package org.apache.vxquery.runtime.functions.comparison;

import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.datamodel.accessors.atomic.XSBinaryPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDatePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDateTimePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDurationPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSQNamePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSTimePointable;

import edu.uci.ics.hyracks.data.std.primitive.BooleanPointable;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

public abstract class AbstractNegatingComparisonOperation extends AbstractValueComparisonOperation {
    final AbstractValueComparisonOperation aOp = createBaseComparisonOperation();

    protected abstract AbstractValueComparisonOperation createBaseComparisonOperation();

    @Override
    public boolean operateAnyURIAnyURI(UTF8StringPointable stringp1, UTF8StringPointable stringp2) throws Exception {
        return !aOp.operateAnyURIAnyURI(stringp1, stringp2);
    }

    @Override
    public boolean operateBase64BinaryBase64Binary(XSBinaryPointable binaryp1, XSBinaryPointable binaryp2)
            throws Exception {
        return !aOp.operateBase64BinaryBase64Binary(binaryp1, binaryp2);
    }

    @Override
    public boolean operateBooleanBoolean(BooleanPointable boolp1, BooleanPointable boolp2) throws Exception {
        return !aOp.operateBooleanBoolean(boolp1, boolp2);
    }

    @Override
    public boolean operateDateDate(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx) throws Exception {
        return !aOp.operateDateDate(datep1, datep2, dCtx);
    }

    @Override
    public boolean operateDatetimeDatetime(XSDateTimePointable datetimep1, XSDateTimePointable datetimep2, DynamicContext dCtx)
            throws Exception {
        return !aOp.operateDatetimeDatetime(datetimep1, datetimep2, dCtx);
    }

    @Override
    public boolean operateDecimalDecimal(XSDecimalPointable decp1, XSDecimalPointable decp2) throws Exception {
        return !aOp.operateDecimalDecimal(decp1, decp2);
    }

    @Override
    public boolean operateDecimalDouble(XSDecimalPointable decp1, DoublePointable doublep2) throws Exception {
        return !aOp.operateDecimalDouble(decp1, doublep2);
    }

    @Override
    public boolean operateDecimalFloat(XSDecimalPointable decp1, FloatPointable floatp2) throws Exception {
        return !aOp.operateDecimalFloat(decp1, floatp2);
    }

    @Override
    public boolean operateDecimalInteger(XSDecimalPointable decp1, LongPointable longp2) throws Exception {
        return !aOp.operateDecimalInteger(decp1, longp2);
    }

    @Override
    public boolean operateDoubleDecimal(DoublePointable doublep1, XSDecimalPointable decp2) throws Exception {
        return !aOp.operateDoubleDecimal(doublep1, decp2);
    }

    @Override
    public boolean operateDoubleDouble(DoublePointable doublep1, DoublePointable doublep2) throws Exception {
        return !aOp.operateDoubleDouble(doublep1, doublep2);
    }

    @Override
    public boolean operateDoubleFloat(DoublePointable doublep1, FloatPointable floatp2) throws Exception {
        return !aOp.operateDoubleFloat(doublep1, floatp2);
    }

    @Override
    public boolean operateDoubleInteger(DoublePointable doublep1, LongPointable longp2) throws Exception {
        return !aOp.operateDoubleInteger(doublep1, longp2);
    }

    @Override
    public boolean operateDTDurationDTDuration(IntegerPointable intp1, IntegerPointable intp2) throws Exception {
        return !aOp.operateDTDurationDTDuration(intp1, intp2);
    }

    @Override
    public boolean operateDurationDuration(XSDurationPointable durationp1, XSDurationPointable durationp2)
            throws Exception {
        return !aOp.operateDurationDuration(durationp1, durationp2);
    }

    @Override
    public boolean operateFloatDecimal(FloatPointable floatp1, XSDecimalPointable decp2) throws Exception {
        return !aOp.operateFloatDecimal(floatp1, decp2);
    }

    @Override
    public boolean operateFloatDouble(FloatPointable floatp1, DoublePointable doublep2) throws Exception {
        return !aOp.operateFloatDouble(floatp1, doublep2);
    }

    @Override
    public boolean operateFloatFloat(FloatPointable floatp1, FloatPointable floatp2) throws Exception {
        return !aOp.operateFloatFloat(floatp1, floatp2);
    }

    @Override
    public boolean operateFloatInteger(FloatPointable floatp1, LongPointable longp2) throws Exception {
        return !aOp.operateFloatInteger(floatp1, longp2);
    }

    @Override
    public boolean operateGDayGDay(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx) throws Exception {
        return !aOp.operateGDayGDay(datep1, datep2, dCtx);
    }

    @Override
    public boolean operateGMonthDayGMonthDay(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx) throws Exception {
        return !aOp.operateGMonthDayGMonthDay(datep1, datep2, dCtx);
    }

    @Override
    public boolean operateGMonthGMonth(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx) throws Exception {
        return !aOp.operateGMonthGMonth(datep1, datep2, dCtx);
    }

    @Override
    public boolean operateGYearGYear(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx) throws Exception {
        return !aOp.operateGYearGYear(datep1, datep2, dCtx);
    }

    @Override
    public boolean operateGYearMonthGYearMonth(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx) throws Exception {
        return !aOp.operateGYearMonthGYearMonth(datep1, datep2, dCtx);
    }

    @Override
    public boolean operateHexBinaryHexBinary(XSBinaryPointable binaryp1, XSBinaryPointable binaryp2) throws Exception {
        return !aOp.operateHexBinaryHexBinary(binaryp1, binaryp2);
    }

    @Override
    public boolean operateIntegerDecimal(LongPointable longp1, XSDecimalPointable decp2) throws Exception {
        return !aOp.operateIntegerDecimal(longp1, decp2);
    }

    @Override
    public boolean operateIntegerDouble(LongPointable longp1, DoublePointable doublep2) throws Exception {
        return !aOp.operateIntegerDouble(longp1, doublep2);
    }

    @Override
    public boolean operateIntegerFloat(LongPointable longp1, FloatPointable floatp2) throws Exception {
        return !aOp.operateIntegerFloat(longp1, floatp2);
    }

    @Override
    public boolean operateIntegerInteger(LongPointable longp1, LongPointable longp2) throws Exception {
        return !aOp.operateIntegerInteger(longp1, longp2);
    }

    @Override
    public boolean operateNotationNotation(UTF8StringPointable stringp1, UTF8StringPointable stringp2) throws Exception {
        return !aOp.operateNotationNotation(stringp1, stringp2);
    }

    @Override
    public boolean operateQNameQName(XSQNamePointable qnamep1, XSQNamePointable qnamep2) throws Exception {
        return !aOp.operateQNameQName(qnamep1, qnamep2);
    }

    @Override
    public boolean operateStringString(UTF8StringPointable stringp1, UTF8StringPointable stringp2) throws Exception {
        return !aOp.operateStringString(stringp1, stringp2);
    }

    @Override
    public boolean operateTimeTime(XSTimePointable timep1, XSTimePointable timep2, DynamicContext dCtx) throws Exception {
        return !aOp.operateTimeTime(timep1, timep2, dCtx);
    }

    @Override
    public boolean operateYMDurationYMDuration(IntegerPointable intp1, IntegerPointable intp2) throws Exception {
        return !aOp.operateYMDurationYMDuration(intp1, intp2);
    }

}