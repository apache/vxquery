package org.apache.vxquery.runtime.functions.comparison;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.datamodel.accessors.atomic.XSBinaryPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDatePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDateTimePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDurationPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSQNamePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSTimePointable;
import org.apache.vxquery.datamodel.util.DateTime;
import org.apache.vxquery.exceptions.SystemException;

import edu.uci.ics.hyracks.data.std.primitive.BooleanPointable;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class ValueLtComparisonOperation extends AbstractValueComparisonOperation {
    protected final ArrayBackedValueStorage abvsInner = new ArrayBackedValueStorage();
    protected final DataOutput dOutInner = abvsInner.getDataOutput();

    @Override
    public boolean operateAnyURIAnyURI(UTF8StringPointable stringp1, UTF8StringPointable stringp2)
            throws SystemException, IOException {
        return (stringp1.compareTo(stringp2) == -1);
    }

    @Override
    public boolean operateBase64BinaryBase64Binary(XSBinaryPointable binaryp1, XSBinaryPointable binaryp2)
            throws SystemException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean operateBooleanBoolean(BooleanPointable boolp1, BooleanPointable boolp2) throws SystemException,
            IOException {
        return (boolp1.compareTo(boolp2) == -1);
    }

    @Override
    public boolean operateDateDate(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx)
            throws SystemException, IOException {
        abvsInner.reset();
        DateTime.getTimezoneDateTime(datep1, dCtx, dOutInner);
        DateTime.getTimezoneDateTime(datep2, dCtx, dOutInner);
        int startOffset1 = abvsInner.getStartOffset() + 1;
        int startOffset2 = startOffset1 + 1 + XSDateTimePointable.TYPE_TRAITS.getFixedLength();
        if (XSDateTimePointable.getYearMonth(abvsInner.getByteArray(), startOffset1) < XSDateTimePointable
                .getYearMonth(abvsInner.getByteArray(), startOffset2)
                && XSDateTimePointable.getDayTime(abvsInner.getByteArray(), startOffset1) < XSDateTimePointable
                        .getDayTime(abvsInner.getByteArray(), startOffset2)) {
            return true;
        }
        return false;
    }

    @Override
    public boolean operateDatetimeDatetime(XSDateTimePointable datetimep1, XSDateTimePointable datetimep2,
            DynamicContext dCtx) throws SystemException, IOException {
        abvsInner.reset();
        DateTime.getTimezoneDateTime(datetimep1, dCtx, dOutInner);
        DateTime.getTimezoneDateTime(datetimep2, dCtx, dOutInner);
        int startOffset1 = abvsInner.getStartOffset() + 1;
        int startOffset2 = startOffset1 + 1 + XSDateTimePointable.TYPE_TRAITS.getFixedLength();
        if (XSDateTimePointable.getYearMonth(abvsInner.getByteArray(), startOffset1) < XSDateTimePointable
                .getYearMonth(abvsInner.getByteArray(), startOffset2)
                && XSDateTimePointable.getDayTime(abvsInner.getByteArray(), startOffset1) < XSDateTimePointable
                        .getDayTime(abvsInner.getByteArray(), startOffset2)) {
            return true;
        }
        return false;
    }

    @Override
    public boolean operateDecimalDecimal(XSDecimalPointable decp1, XSDecimalPointable decp2) throws SystemException,
            IOException {
        return (decp1.compareTo(decp2) == -1);
    }

    @Override
    public boolean operateDecimalDouble(XSDecimalPointable decp1, DoublePointable doublep2) throws SystemException,
            IOException {
        double double1 = decp1.doubleValue();
        double double2 = doublep2.doubleValue();
        return (double1 < double2);
    }

    @Override
    public boolean operateDecimalFloat(XSDecimalPointable decp1, FloatPointable floatp2) throws SystemException,
            IOException {
        float float1 = decp1.floatValue();
        float float2 = floatp2.floatValue();
        return (float1 < float2);
    }

    @Override
    public boolean operateDecimalInteger(XSDecimalPointable decp1, LongPointable longp2) throws SystemException,
            IOException {
        XSDecimalPointable decp2 = (XSDecimalPointable) XSDecimalPointable.FACTORY.createPointable();
        decp2.setDecimal(longp2.getLong(), (byte) 0);
        return (decp1.compareTo(decp2) == -1);
    }

    @Override
    public boolean operateDoubleDecimal(DoublePointable doublep1, XSDecimalPointable decp2) throws SystemException,
            IOException {
        double double1 = doublep1.doubleValue();
        double double2 = decp2.doubleValue();
        return (double1 < double2);
    }

    @Override
    public boolean operateDoubleDouble(DoublePointable doublep1, DoublePointable doublep2) throws SystemException,
            IOException {
        return (doublep1.compareTo(doublep2) == -1);
    }

    @Override
    public boolean operateDoubleFloat(DoublePointable doublep1, FloatPointable floatp2) throws SystemException,
            IOException {
        double double1 = doublep1.doubleValue();
        double double2 = floatp2.doubleValue();
        return (double1 < double2);
    }

    @Override
    public boolean operateDoubleInteger(DoublePointable doublep1, LongPointable longp2) throws SystemException,
            IOException {
        double double1 = doublep1.doubleValue();
        double double2 = longp2.doubleValue();
        return (double1 < double2);
    }

    @Override
    public boolean operateDTDurationDTDuration(LongPointable longp1, LongPointable longp2) throws SystemException,
            IOException {
        return (longp1.compareTo(longp2) == -1);
    }

    @Override
    public boolean operateDurationDuration(XSDurationPointable durationp1, XSDurationPointable durationp2)
            throws SystemException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean operateFloatDecimal(FloatPointable floatp1, XSDecimalPointable decp2) throws SystemException,
            IOException {
        float float1 = floatp1.floatValue();
        float float2 = decp2.floatValue();
        return (float1 < float2);
    }

    @Override
    public boolean operateFloatDouble(FloatPointable floatp1, DoublePointable doublep2) throws SystemException,
            IOException {
        double double1 = floatp1.doubleValue();
        double double2 = doublep2.doubleValue();
        return (double1 < double2);
    }

    @Override
    public boolean operateFloatFloat(FloatPointable floatp1, FloatPointable floatp2) throws SystemException,
            IOException {
        return (floatp1.compareTo(floatp2) == -1);
    }

    @Override
    public boolean operateFloatInteger(FloatPointable floatp1, LongPointable longp2) throws SystemException,
            IOException {
        float float1 = floatp1.floatValue();
        float float2 = longp2.floatValue();
        return (float1 < float2);
    }

    @Override
    public boolean operateGDayGDay(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx)
            throws SystemException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean operateGMonthDayGMonthDay(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx)
            throws SystemException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean operateGMonthGMonth(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx)
            throws SystemException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean operateGYearGYear(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx)
            throws SystemException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean operateGYearMonthGYearMonth(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx)
            throws SystemException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean operateHexBinaryHexBinary(XSBinaryPointable binaryp1, XSBinaryPointable binaryp2)
            throws SystemException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean operateIntegerDecimal(LongPointable longp1, XSDecimalPointable decp2) throws SystemException,
            IOException {
        XSDecimalPointable decp1 = (XSDecimalPointable) XSDecimalPointable.FACTORY.createPointable();
        decp1.setDecimal(longp1.getLong(), (byte) 0);
        return (decp1.compareTo(decp2) == -1);
    }

    @Override
    public boolean operateIntegerDouble(LongPointable longp1, DoublePointable doublep2) throws SystemException,
            IOException {
        double double1 = longp1.doubleValue();
        double double2 = doublep2.doubleValue();
        return (double1 < double2);
    }

    @Override
    public boolean operateIntegerFloat(LongPointable longp1, FloatPointable floatp2) throws SystemException,
            IOException {
        float float1 = longp1.floatValue();
        float float2 = floatp2.floatValue();
        return (float1 < float2);
    }

    @Override
    public boolean operateIntegerInteger(LongPointable longp1, LongPointable longp2) throws SystemException,
            IOException {
        return (longp1.compareTo(longp2) == -1);
    }

    @Override
    public boolean operateNotationNotation(UTF8StringPointable stringp1, UTF8StringPointable stringp2)
            throws SystemException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean operateQNameQName(XSQNamePointable qnamep1, XSQNamePointable qnamep2) throws SystemException,
            IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean operateStringString(UTF8StringPointable stringp1, UTF8StringPointable stringp2)
            throws SystemException, IOException {
        return (stringp1.compareTo(stringp2) == -1);
    }

    @Override
    public boolean operateTimeTime(XSTimePointable timep1, XSTimePointable timep2, DynamicContext dCtx)
            throws SystemException, IOException {
        abvsInner.reset();
        DateTime.getTimezoneDateTime(timep1, dCtx, dOutInner);
        DateTime.getTimezoneDateTime(timep2, dCtx, dOutInner);
        int startOffset1 = abvsInner.getStartOffset() + 1;
        int startOffset2 = startOffset1 + 1 + XSDateTimePointable.TYPE_TRAITS.getFixedLength();
        if (XSDateTimePointable.getYearMonth(abvsInner.getByteArray(), startOffset1) < XSDateTimePointable.getYearMonth(
                abvsInner.getByteArray(), startOffset2)
                && XSDateTimePointable.getDayTime(abvsInner.getByteArray(), startOffset1) < XSDateTimePointable
                        .getDayTime(abvsInner.getByteArray(), startOffset2)) {
            return true;
        }
        return false;
    }

    @Override
    public boolean operateYMDurationYMDuration(IntegerPointable intp1, IntegerPointable intp2) throws SystemException,
            IOException {
        return (intp1.compareTo(intp2) == -1);
    }

}