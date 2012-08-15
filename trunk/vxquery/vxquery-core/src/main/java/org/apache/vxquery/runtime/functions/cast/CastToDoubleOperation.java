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

public class CastToDoubleOperation extends AbstractCastToOperation {
    /*
     * All the positive powers of 10 that can be represented exactly in float.
     */
    private static final double powersOf10[] = { 1.0e0, 1.0e1, 1.0e2, 1.0e3, 1.0e4, 1.0e5, 1.0e6, 1.0e7, 1.0e8, 1.0e9,
            1.0e10, 1.0e11, 1.0e12, 1.0e13, 1.0e14, 1.0e15, 1.0e16, 1.0e17, 1.0e18, 1.0e19, 1.0e20, 1.0e21, 1.0e22 };
    private static final double powersOf2[] = { 1.0e16d, 1.0e32f, 1.0e64, 1.0e128, 1.0e256 };

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
        short decimalPlace = 0;
        long value = 0;
        double valueDouble;
        boolean pastDecimal = false, negativeValue = false;
        int c = ICharacterIterator.EOS_CHAR;
        int c2 = ICharacterIterator.EOS_CHAR;
        int c3 = ICharacterIterator.EOS_CHAR;
        long limit = -Long.MAX_VALUE;

        // Check sign.
        c = charIterator.next();
        if (c == Character.valueOf('-')) {
            negativeValue = true;
            c = charIterator.next();
            limit = Long.MIN_VALUE;
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
                    if (value < limit / 10 + Character.getNumericValue(c)) {
                        throw new SystemException(ErrorCode.FOCA0006);
                    }
                    value = value * 10 - Character.getNumericValue(c);
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
                if (moveOffset > 324 || moveOffset < -324) {
                    throw new SystemException(ErrorCode.FOCA0006);
                }
                decimalPlace += (negativeOffset ? -moveOffset : moveOffset);
            }

            /*
             * The following conditions to create the floating point value is using known valid float values.
             * In addition, each one only needs one or two operations to get the float value, further minimizing
             * possible errors. (Not perfect, but pretty good.)
             */
            valueDouble = (double) value;
            if (decimalPlace == 0 || valueDouble == 0.0f) {
                // No modification required to float value.
            } else if (decimalPlace >= 0) {
                if (decimalPlace <= 16) {
                    valueDouble *= powersOf10[decimalPlace];
                } else {
                    // Multiply the value based on the exponent binary.
                    if ((decimalPlace & 15) != 0) {
                        valueDouble *= powersOf10[decimalPlace & 15];
                    }
                    if ((decimalPlace >>= 4) != 0) {
                        int j;
                        for (j = 0; decimalPlace > 1; j++, decimalPlace >>= 1) {
                            if ((decimalPlace & 1) != 0)
                                valueDouble *= powersOf2[j];
                        }
                        // Handle the last cast for infinity and max value.
                        double t = valueDouble * powersOf2[j];
                        if (Double.isInfinite(t)) {
                            // Overflow
                            t = valueDouble / 2.0;
                            t *= powersOf2[j];
                            if (Double.isInfinite(t)) {
                                valueDouble = Double.POSITIVE_INFINITY;
                            }
                            t = -Double.MAX_VALUE;
                        }
                        valueDouble = t;
                    }
                }
            } else {
                if (decimalPlace >= -16) {
                    valueDouble /= powersOf10[-decimalPlace];
                } else {
                    if ((decimalPlace & 15) != 0) {
                        valueDouble /= powersOf10[decimalPlace & 15];
                    }
                    if ((decimalPlace >>= 4) != 0) {
                        int j;
                        for (j = 0; decimalPlace > 1; j++, decimalPlace >>= 1) {
                            if ((decimalPlace & 1) != 0)
                                valueDouble /= powersOf2[j];
                        }
                        // Handle the last cast for zero and min value.
                        double t = valueDouble / powersOf2[j];
                        if (t == 0.0) {
                            // Underflow.
                            t = valueDouble * 2.0;
                            t /= powersOf2[j];
                            if (t == 0.0) {
                                valueDouble = 0.0;
                            }
                            t = Double.MIN_VALUE;
                        }
                        valueDouble = t;
                    }
                }
            }

        }

        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.writeDouble((negativeValue ? valueDouble : -valueDouble));
    }

    @Override
    public void convertUntypedAtomic(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        convertString(stringp, dOut);
    }

    /**
     * Derived Datatypes
     */
    public void convertByte(BytePointable bytep, DataOutput dOut) throws SystemException, IOException {
        writeDoubleValue(bytep, dOut);
    }

    public void convertInt(IntegerPointable intp, DataOutput dOut) throws SystemException, IOException {
        writeDoubleValue(intp, dOut);
    }

    public void convertLong(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        writeDoubleValue(longp, dOut);
    }

    public void convertNegativeInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        writeDoubleValue(longp, dOut);
    }

    public void convertNonNegativeInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        writeDoubleValue(longp, dOut);
    }

    public void convertNonPositiveInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        writeDoubleValue(longp, dOut);
    }

    public void convertPositiveInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        writeDoubleValue(longp, dOut);
    }

    public void convertShort(ShortPointable shortp, DataOutput dOut) throws SystemException, IOException {
        writeDoubleValue(shortp, dOut);
    }

    public void convertUnsignedByte(BytePointable bytep, DataOutput dOut) throws SystemException, IOException {
        writeDoubleValue(bytep, dOut);
    }

    public void convertUnsignedInt(IntegerPointable intp, DataOutput dOut) throws SystemException, IOException {
        writeDoubleValue(intp, dOut);
    }

    public void convertUnsignedLong(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        writeDoubleValue(longp, dOut);
    }

    public void convertUnsignedShort(ShortPointable shortp, DataOutput dOut) throws SystemException, IOException {
        writeDoubleValue(shortp, dOut);
    }

    private void writeDoubleValue(INumeric numericp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.writeDouble(numericp.doubleValue());
    }

}