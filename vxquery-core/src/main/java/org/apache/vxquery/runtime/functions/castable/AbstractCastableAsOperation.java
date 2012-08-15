package org.apache.vxquery.runtime.functions.castable;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.atomic.XSBinaryPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDatePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDateTimePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDurationPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSQNamePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSTimePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.SystemException;

import edu.uci.ics.hyracks.data.std.primitive.BooleanPointable;
import edu.uci.ics.hyracks.data.std.primitive.BytePointable;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.primitive.ShortPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

public abstract class AbstractCastableAsOperation {
    public void convertAnyURI(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertBase64Binary(XSBinaryPointable binaryp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertBoolean(BooleanPointable boolp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertDate(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertDatetime(XSDateTimePointable datetimep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertDecimal(XSDecimalPointable decp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertDouble(DoublePointable doublep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertDTDuration(IntegerPointable intp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertDuration(XSDurationPointable durationp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertFloat(FloatPointable floatp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertGDay(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertGMonth(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertGMonthDay(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertGYear(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertGYearMonth(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertHexBinary(XSBinaryPointable binaryp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertNotation(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertQName(XSQNamePointable qnamep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertString(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertTime(XSTimePointable timep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertUntypedAtomic(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertYMDuration(IntegerPointable intp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    /**
     * Derived Datatypes
     */
    public void convertByte(BytePointable bytep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertInt(IntegerPointable intp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertLong(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertNegativeInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertNonNegativeInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertNonPositiveInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertPositiveInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertShort(ShortPointable shortp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertUnsignedByte(BytePointable bytep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertUnsignedInt(IntegerPointable intp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertUnsignedLong(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

    public void convertUnsignedShort(ShortPointable shortp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 0);
    }

}
