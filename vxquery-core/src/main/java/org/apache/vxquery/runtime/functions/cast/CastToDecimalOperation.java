package org.apache.vxquery.runtime.functions.cast;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.strings.ICharacterIterator;
import org.apache.vxquery.runtime.functions.strings.UTF8StringCharacterIterator;

import edu.uci.ics.hyracks.data.std.api.INumeric;
import edu.uci.ics.hyracks.data.std.primitive.BooleanPointable;
import edu.uci.ics.hyracks.data.std.primitive.BytePointable;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.primitive.ShortPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class CastToDecimalOperation extends AbstractCastToOperation {
    private ArrayBackedValueStorage abvsInner = new ArrayBackedValueStorage();
    private DataOutput dOutInner = abvsInner.getDataOutput();

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
        abvsInner.reset();
        CastToStringOperation castTo = new CastToStringOperation();
        castTo.convertDoubleCanonical(doublep, dOutInner);

        UTF8StringPointable stringp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        stringp.set(abvsInner.getByteArray(), abvsInner.getStartOffset() + 1, abvsInner.getLength() - 1);
        convertStringExtra(stringp, dOut, true);
    }

    @Override
    public void convertFloat(FloatPointable floatp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        CastToStringOperation castTo = new CastToStringOperation();
        castTo.convertFloatCanonical(floatp, dOutInner);

        UTF8StringPointable stringp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        stringp.set(abvsInner.getByteArray(), abvsInner.getStartOffset() + 1, abvsInner.getLength() - 1);
        convertStringExtra(stringp, dOut, true);
    }

    @Override
    public void convertInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_DECIMAL_TAG);
        dOut.write((byte) 0);
        dOut.writeLong(longp.getLong());
    }

    @Override
    public void convertString(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        convertStringExtra(stringp, dOut, false);
    }

    private void convertStringExtra(UTF8StringPointable stringp, DataOutput dOut, boolean connoicalForm)
            throws SystemException, IOException {
        ICharacterIterator charIterator = new UTF8StringCharacterIterator(stringp);
        charIterator.reset();
        byte decimalPlace = 0;
        long value = 0;
        boolean pastDecimal = false, negativeValue = false;
        int count = 0;
        int c = 0;

        // Check sign.
        c = charIterator.next();
        if (c == Character.valueOf('-')) {
            negativeValue = true;
            c = charIterator.next();
        }

        // Read in the number.
        do {
            if (count + 1 > XSDecimalPointable.PRECISION) {
                throw new SystemException(ErrorCode.FOCA0006);
            } else if (Character.isDigit(c)) {
                value = value * 10 + Character.getNumericValue(c);
                if (pastDecimal) {
                    decimalPlace++;
                }
                count++;
            } else if (c == Character.valueOf('.') && pastDecimal == false) {
                pastDecimal = true;
            } else if (c == Character.valueOf('E') || c == Character.valueOf('e') && connoicalForm) {
                break;
            } else {
                throw new SystemException(ErrorCode.FORG0001);
            }
        } while ((c = charIterator.next()) != ICharacterIterator.EOS_CHAR);

        // Parse the exponent.
        if (c == Character.valueOf('E') || c == Character.valueOf('e') && connoicalForm) {
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
            decimalPlace -= (negativeOffset ? -moveOffset : moveOffset);
        }

        // Normalize the value and take off trailing zeros.
        while (value != 0 && value % 10 == 0) {
            value /= 10;
            --decimalPlace;
        }
        if (decimalPlace > XSDecimalPointable.PRECISION) {
            throw new SystemException(ErrorCode.FOCA0006);
        }

        dOut.write(ValueTag.XS_DECIMAL_TAG);
        dOut.write(decimalPlace);
        dOut.writeLong((negativeValue ? -value : value));
    }

    @Override
    public void convertUntypedAtomic(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        convertString(stringp, dOut);
    }

    /**
     * Derived Datatypes
     */
    public void convertByte(BytePointable bytep, DataOutput dOut) throws SystemException, IOException {
        writeDecimalValue(bytep, dOut);
    }

    public void convertInt(IntegerPointable intp, DataOutput dOut) throws SystemException, IOException {
        writeDecimalValue(intp, dOut);
    }

    public void convertLong(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        writeDecimalValue(longp, dOut);
    }

    public void convertNegativeInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        writeDecimalValue(longp, dOut);
    }

    public void convertNonNegativeInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        writeDecimalValue(longp, dOut);
    }

    public void convertNonPositiveInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        writeDecimalValue(longp, dOut);
    }

    public void convertPositiveInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        writeDecimalValue(longp, dOut);
    }

    public void convertShort(ShortPointable shortp, DataOutput dOut) throws SystemException, IOException {
        writeDecimalValue(shortp, dOut);
    }

    public void convertUnsignedByte(BytePointable bytep, DataOutput dOut) throws SystemException, IOException {
        writeDecimalValue(bytep, dOut);
    }

    public void convertUnsignedInt(IntegerPointable intp, DataOutput dOut) throws SystemException, IOException {
        writeDecimalValue(intp, dOut);
    }

    public void convertUnsignedLong(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        writeDecimalValue(longp, dOut);
    }

    public void convertUnsignedShort(ShortPointable shortp, DataOutput dOut) throws SystemException, IOException {
        writeDecimalValue(shortp, dOut);
    }

    private void writeDecimalValue(INumeric numericp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_DECIMAL_TAG);
        dOut.write((byte) 0);
        dOut.writeLong(numericp.longValue());
    }

}