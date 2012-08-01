package org.apache.vxquery.runtime.functions.cast;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.strings.ICharacterIterator;
import org.apache.vxquery.runtime.functions.strings.UTF8StringCharacterIterator;

import edu.uci.ics.hyracks.data.std.primitive.BooleanPointable;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

public class CastToIntegerOperation extends AbstractCastToOperation {

    @Override
    public void convertBoolean(BooleanPointable boolp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_INTEGER_TAG);
        dOut.writeLong((long) (boolp.getBoolean() ? 1 : 0));
    }

    @Override
    public void convertDecimal(XSDecimalPointable decp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_INTEGER_TAG);
        dOut.writeLong(decp.longValue());
    }

    @Override
    public void convertDouble(DoublePointable doublep, DataOutput dOut) throws SystemException, IOException {
        if (doublep.getDouble() > Long.MAX_VALUE || doublep.getDouble() < Long.MIN_VALUE) {
            throw new SystemException(ErrorCode.FOCA0003);
        }
        dOut.write(ValueTag.XS_INTEGER_TAG);
        dOut.writeLong(doublep.longValue());
    }

    @Override
    public void convertFloat(FloatPointable floatp, DataOutput dOut) throws SystemException, IOException {
        if (floatp.getFloat() > Long.MAX_VALUE || floatp.getFloat() < Long.MIN_VALUE) {
            throw new SystemException(ErrorCode.FOCA0003);
        }
        dOut.write(ValueTag.XS_INTEGER_TAG);
        dOut.writeLong(floatp.longValue());
    }

    @Override
    public void convertInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_INTEGER_TAG);
        dOut.write(longp.getByteArray(), longp.getStartOffset(), longp.getLength());
    }

    @Override
    public void convertString(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        ICharacterIterator charIterator = new UTF8StringCharacterIterator(stringp);
        charIterator.reset();
        long value = 0;
        int c = 0;
        boolean negative = false;
        while ((c = charIterator.next()) != ICharacterIterator.EOS_CHAR) {
            if (Character.isDigit(c)) {
                value = value * 10 + Character.getNumericValue(c);
            } else if (c == Character.valueOf('-')) {
                negative = true;
            } else {
                throw new SystemException(ErrorCode.FORG0001);
            }
        }
        if (negative) {
            value *= -1;
        }
        System.err.println("   int value = " + value);

        dOut.write(ValueTag.XS_INTEGER_TAG);
        dOut.writeLong(value);
    }

    @Override
    public void convertUntypedAtomic(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        convertString(stringp, dOut);
    }

}