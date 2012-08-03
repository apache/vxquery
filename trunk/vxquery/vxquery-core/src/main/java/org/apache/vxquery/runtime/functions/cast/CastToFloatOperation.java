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

public class CastToFloatOperation extends AbstractCastToOperation {

    @Override
    public void convertBoolean(BooleanPointable boolp, DataOutput dOut) throws SystemException, IOException {
        float value = (boolp.getBoolean() ? 1 : 0);
        dOut.write(ValueTag.XS_FLOAT_TAG);
        dOut.writeFloat(value);
    }

    @Override
    public void convertDecimal(XSDecimalPointable decp, DataOutput dOut) throws SystemException, IOException {
        float value = decp.floatValue();
        dOut.write(ValueTag.XS_FLOAT_TAG);
        dOut.writeFloat(value);
    }

    @Override
    public void convertDouble(DoublePointable doublep, DataOutput dOut) throws SystemException, IOException {
        float value = doublep.floatValue();
        dOut.write(ValueTag.XS_FLOAT_TAG);
        dOut.writeFloat(value);
    }

    @Override
    public void convertFloat(FloatPointable floatp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_FLOAT_TAG);
        dOut.write(floatp.getByteArray(), floatp.getStartOffset(), floatp.getLength());
    }

    @Override
    public void convertInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        float value = longp.floatValue();
        dOut.write(ValueTag.XS_FLOAT_TAG);
        dOut.writeFloat(value);
    }

    @Override
    public void convertString(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        ICharacterIterator charIterator = new UTF8StringCharacterIterator(stringp);
        charIterator.reset();
        byte decimalPlace = 0;
        long value = 0;
        boolean pastDecimal = false, negativeValue = false;
        int c = ICharacterIterator.EOS_CHAR;
        int c2 = ICharacterIterator.EOS_CHAR;
        int c3 = ICharacterIterator.EOS_CHAR;

        while ((c = charIterator.next()) != ICharacterIterator.EOS_CHAR) {
            if (Character.isDigit(c)) {
                value = value * 10 + Character.getNumericValue(c);
                if (pastDecimal) {
                    decimalPlace--;
                }
            } else if (c == Character.valueOf('-')) {
                negativeValue = true;
            } else if (c == Character.valueOf('E') || c == Character.valueOf('e')) {
                break;
            } else if (c == Character.valueOf('.')) {
                pastDecimal = true;
            } else if (c == Character.valueOf('I') || c == Character.valueOf('N') && value == 0) {
                break;
            } else {
                throw new SystemException(ErrorCode.FORG0001);
            }
        }
        if (c == Character.valueOf('E') || c == Character.valueOf('e')) {
            int moveOffset = 0;
            boolean offsetNegative = false;
            while ((c = charIterator.next()) != ICharacterIterator.EOS_CHAR) {
                if (Character.isDigit(c)) {
                    moveOffset = moveOffset * 10 + Character.getNumericValue(c);
                } else if (c == Character.valueOf('-')) {
                    offsetNegative = true;
                } else {
                    throw new SystemException(ErrorCode.FORG0001);
                }
            }
            if (offsetNegative) {
                moveOffset *= -1;
            }
            decimalPlace += moveOffset;
        }
        float valueFloat;
        if (c == Character.valueOf('I') || c == Character.valueOf('N')) {
            c2 = charIterator.next();
            c3 = charIterator.next();
            if (charIterator.next() != ICharacterIterator.EOS_CHAR) {
                throw new SystemException(ErrorCode.FORG0001);
            } else if (c == Character.valueOf('I') && c2 == Character.valueOf('N') && c3 == Character.valueOf('F')) {
                if (negativeValue) {
                    valueFloat = Float.NEGATIVE_INFINITY;
                } else {
                    valueFloat = Float.POSITIVE_INFINITY;
                }
            } else if (c == Character.valueOf('N') && c2 == Character.valueOf('a') && c3 == Character.valueOf('N')) {
                valueFloat = Float.NaN;
            } else {
                throw new SystemException(ErrorCode.FORG0001);
            }
        } else {
            if (negativeValue) {
                value *= -1;
            }
            valueFloat = value;
            while (decimalPlace != 0) {
                if (decimalPlace > 0) {
                    --decimalPlace;
                    valueFloat *= 10;
                } else {
                    ++decimalPlace;
                    valueFloat *= 0.1;
                }
            }
        }

        dOut.write(ValueTag.XS_FLOAT_TAG);
        dOut.writeFloat(valueFloat);
    }

    @Override
    public void convertUntypedAtomic(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        convertString(stringp, dOut);
    }

}