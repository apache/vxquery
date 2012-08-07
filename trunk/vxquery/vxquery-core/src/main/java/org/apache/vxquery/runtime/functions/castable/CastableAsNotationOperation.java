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
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;

import edu.uci.ics.hyracks.data.std.primitive.BooleanPointable;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

public class CastableAsNotationOperation extends AbstractCastableAsOperation {
    public void convertAnyURI(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    public void convertBase64Binary(XSBinaryPointable binaryp, DataOutput dOut) throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    public void convertBoolean(BooleanPointable boolp, DataOutput dOut) throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    public void convertDate(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    public void convertDatetime(XSDateTimePointable datetimep, DataOutput dOut) throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    public void convertDecimal(XSDecimalPointable decp, DataOutput dOut) throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    public void convertDouble(DoublePointable doublep, DataOutput dOut) throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    public void convertDTDuration(IntegerPointable intp, DataOutput dOut) throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    public void convertDuration(XSDurationPointable durationp, DataOutput dOut) throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    public void convertFloat(FloatPointable floatp, DataOutput dOut) throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    public void convertGDay(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    public void convertGMonth(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    public void convertGMonthDay(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    public void convertGYear(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    public void convertGYearMonth(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    public void convertHexBinary(XSBinaryPointable binaryp, DataOutput dOut) throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    public void convertInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    public void convertNotation(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    public void convertQName(XSQNamePointable qnamep, DataOutput dOut) throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    public void convertString(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    public void convertTime(XSTimePointable timep, DataOutput dOut) throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    public void convertUntypedAtomic(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    public void convertYMDuration(IntegerPointable intp, DataOutput dOut) throws SystemException, IOException {
        throw new SystemException(ErrorCode.XPTY0004);
    }
}