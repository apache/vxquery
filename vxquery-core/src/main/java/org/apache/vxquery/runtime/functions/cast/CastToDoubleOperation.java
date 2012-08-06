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

public class CastToDoubleOperation extends AbstractCastToOperation {

    @Override
    public void convertBoolean(BooleanPointable boolp, DataOutput dOut) throws SystemException, IOException {
        double value = (boolp.getBoolean() ? 1 : 0);
        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.writeDouble(value);
    }

    @Override
    public void convertDecimal(XSDecimalPointable decp, DataOutput dOut) throws SystemException, IOException {
        double value = decp.doubleValue();
        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.writeDouble(value);
    }

    @Override
    public void convertDouble(DoublePointable doublep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.write(doublep.getByteArray(), doublep.getStartOffset(), doublep.getLength());
    }

    @Override
    public void convertFloat(FloatPointable floatp, DataOutput dOut) throws SystemException, IOException {
        double value = floatp.doubleValue();
        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.writeDouble(value);
    }

    @Override
    public void convertInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        double value = longp.doubleValue();
        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.writeDouble(value);
    }

    @Override
    public void convertString(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        ICharacterIterator charIterator = new UTF8StringCharacterIterator(stringp);
        charIterator.reset();
        byte decimalPlace = 0;
        long value = 0;
        double valueDouble;
        boolean pastDecimal = false, negativeValue = false;
        int c = ICharacterIterator.EOS_CHAR;
        int c2 = ICharacterIterator.EOS_CHAR;
        int c3 = ICharacterIterator.EOS_CHAR;

        // Check sign.
        c = charIterator.next();
        if (c == Character.valueOf('-')) {
            negativeValue = true;
            c = charIterator.next();
        }
        // Check the special cases.
        if (c == Character.valueOf('I') || c == Character.valueOf('N')) {
            c2 = charIterator.next();
            c3 = charIterator.next();
            if (charIterator.next() != ICharacterIterator.EOS_CHAR) {
                throw new SystemException(ErrorCode.FORG0001);
            } else if (c == Character.valueOf('I') && c2 == Character.valueOf('N') && c3 == Character.valueOf('F')) {
                valueDouble = (negativeValue ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY);
            } else if (c == Character.valueOf('N') && c2 == Character.valueOf('a') && c3 == Character.valueOf('N')) {
                valueDouble = Double.NaN;
            } else {
                throw new SystemException(ErrorCode.FORG0001);
            }
        } else {
            // Read in the number.
            do {
                if (Character.isDigit(c)) {
                    if (value > Long.MAX_VALUE / 10) {
                        throw new SystemException(ErrorCode.FOCA0006);
                    }
                    value = value * 10 + Character.getNumericValue(c);
                    if (pastDecimal) {
                        decimalPlace--;
                    }
                } else if (c == Character.valueOf('.') && pastDecimal == false) {
                    pastDecimal = true;
                } else if (c == Character.valueOf('E') || c == Character.valueOf('e')) {
                    break;
                } else {
                    throw new SystemException(ErrorCode.FORG0001);
                }
            } while ((c = charIterator.next()) != ICharacterIterator.EOS_CHAR);

            // Parse the exponent.
            if (c == Character.valueOf('E') || c == Character.valueOf('e')) {
                int moveOffset = 0;
                boolean negativeOffset = false;
                // Check for the negative sign.
                c = charIterator.next();
                if (c == Character.valueOf('-')) {
                    negativeOffset = true;
                    c = charIterator.next();
                }
                // Process the numeric value.
                do {
                    if (Character.isDigit(c)) {
                        moveOffset = moveOffset * 10 + Character.getNumericValue(c);
                    } else {
                        throw new SystemException(ErrorCode.FORG0001);
                    }
                } while ((c = charIterator.next()) != ICharacterIterator.EOS_CHAR);
                decimalPlace += (negativeOffset ? -moveOffset : moveOffset);
                if (decimalPlace > 324 || decimalPlace < -324) {
                    throw new SystemException(ErrorCode.FOCA0006);
                }
            }

            // TODO Verify the long value and exponent are combined to give the correct double.
            valueDouble = (double) value;
            while (decimalPlace != 0 && valueDouble != 0) {
                if (decimalPlace > 0) {
                    --decimalPlace;
                    valueDouble *= 10;
                } else {
                    ++decimalPlace;
                    valueDouble /= 10;
                }
            }

        }
        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.writeDouble((negativeValue ? -valueDouble : valueDouble));
    }

    @Override
    public void convertUntypedAtomic(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        convertString(stringp, dOut);
    }

}