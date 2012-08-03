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

public class CastToDecimalOperation extends AbstractCastToOperation {

    @Override
    public void convertBoolean(BooleanPointable boolp, DataOutput dOut) throws SystemException, IOException {
        long value = (boolp.getBoolean() ? 1 : 0);
        dOut.write(ValueTag.XS_DECIMAL_TAG);
        dOut.writeByte(0);
        dOut.writeLong(value);
    }

    @Override
    public void convertDecimal(XSDecimalPointable decp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_DECIMAL_TAG);
        dOut.write(decp.getByteArray(), decp.getStartOffset(), decp.getLength());
    }

    @Override
    public void convertDouble(DoublePointable doublep, DataOutput dOut) throws SystemException, IOException {
        double doubleValue = doublep.getDouble();
        byte decimalPlace = 0;
        // Move the decimal
        while (doubleValue % 1 != 0 && (doubleValue != 0 || doubleValue != -0)) {
            if (decimalPlace + 1 > XSDecimalPointable.PRECISION) {
                throw new SystemException(ErrorCode.FOCA0001);
            }
            decimalPlace++;
            doubleValue *= 10;
        }
        // Remove extra zeros
        while (doubleValue % 10 == 0 && (doubleValue != 0 || doubleValue != -0)) {
            doubleValue /= 10;
            --decimalPlace;
        }
        dOut.write(ValueTag.XS_DECIMAL_TAG);
        dOut.write(decimalPlace);
        dOut.writeLong((long) doubleValue);
    }

    @Override
    public void convertFloat(FloatPointable floatp, DataOutput dOut) throws SystemException, IOException {
        float floatValue = floatp.getFloat();
        byte decimalPlace = 0;
        
        // Move the decimal
        while (floatValue % 1 != 0 && (floatValue != 0 || floatValue != -0)) {
            if (decimalPlace + 1 > XSDecimalPointable.PRECISION) {
                throw new SystemException(ErrorCode.FOCA0001);
            }
            decimalPlace++;
            floatValue *= 10;
        }
        // Remove extra zeros
        while (floatValue % 10 == 0 && (floatValue != 0 || floatValue != -0)) {
            floatValue /= 10;
            --decimalPlace;
        }
        dOut.write(ValueTag.XS_DECIMAL_TAG);
        dOut.write(decimalPlace);
        dOut.writeLong((long) floatValue);
    }

    @Override
    public void convertInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_DECIMAL_TAG);
        dOut.write((byte) 0);
        dOut.writeLong(longp.getLong());
    }

    @Override
    public void convertString(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        ICharacterIterator charIterator = new UTF8StringCharacterIterator(stringp);
        charIterator.reset();
        byte decimalPlace = 0;
        long value = 0;
        boolean pastDecimal = false, negativeValue = false;
        int count = 0;
        int c = 0;

        while ((c = charIterator.next()) != ICharacterIterator.EOS_CHAR) {
            if (count + 1 > XSDecimalPointable.PRECISION) {
                throw new SystemException(ErrorCode.FOCA0006);
            } else if (Character.isDigit(c)) {
                value = value * 10 + Character.getNumericValue(c);
                if (pastDecimal) {
                    decimalPlace++;
                }
                count++;
            } else if (c == Character.valueOf('-')) {
                negativeValue = true;
            } else if (c == Character.valueOf('.')) {
                pastDecimal = true;
            } else {
                throw new SystemException(ErrorCode.FORG0001);
            }
        }
        if (negativeValue) {
            value *= -1;
        }

        // Normalize the value and take off trailing zeros.
        while (value != 0 && value % 10 == 0) {
            value -= 10;
            --decimalPlace;
        }
        dOut.write(ValueTag.XS_DECIMAL_TAG);
        dOut.write(decimalPlace);
        dOut.writeLong(value);
    }

    @Override
    public void convertUntypedAtomic(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        convertString(stringp, dOut);
    }

}