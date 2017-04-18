/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.vxquery.runtime.functions.cast;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.data.std.api.INumeric;
import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.BytePointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.util.string.UTF8StringWriter;
import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.strings.ICharacterIterator;
import org.apache.vxquery.runtime.functions.strings.UTF8StringCharacterIterator;

public class CastToDecimalOperation extends AbstractCastToOperation {
    private ArrayBackedValueStorage abvsInner = new ArrayBackedValueStorage();
    private DataOutput dOutInner = abvsInner.getDataOutput();
    private CastToStringOperation castToString = new CastToStringOperation();
    private UTF8StringPointable stringp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
    private UTF8StringWriter sw = new UTF8StringWriter();

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
        if (doublep.getDouble() == 0) {
            long bits = Double.doubleToLongBits(doublep.getDouble());
            boolean negative = ((bits >> 63) == 0) ? false : true;
            if (negative) {
                throw new SystemException(ErrorCode.FORG0001);
            }
        }
        if (Double.isNaN(doublep.getDouble()) || Double.isInfinite(doublep.getDouble())) {
            throw new SystemException(ErrorCode.FORG0001);
        }
        abvsInner.reset();
        sw.writeUTF8(Double.toString(doublep.getDouble()), dOutInner);
        stringp.set(abvsInner.getByteArray(), abvsInner.getStartOffset(), abvsInner.getLength());
//        dOutInner.write(ValueTag.XS_STRING_TAG);
//        sw.writeUTF8(Double.toString(doublep.getDouble()), dOutInner);
//        stringp.set(abvsInner.getByteArray(), abvsInner.getStartOffset() + 1, abvsInner.getLength() - 1);
        convertStringExtra(stringp, dOut, true);
    }

    @Override
    public void convertFloat(FloatPointable floatp, DataOutput dOut) throws SystemException, IOException {
        if (floatp.getFloat() == 0) {
            long bits = Float.floatToIntBits(floatp.getFloat());
            boolean negative = ((bits >> 31) == 0) ? false : true;
            if (negative) {
                throw new SystemException(ErrorCode.FORG0001);
            }
        }
        abvsInner.reset();
        castToString.convertFloatCanonical(floatp, dOutInner);
        stringp.set(abvsInner.getByteArray(), abvsInner.getStartOffset() + 1, abvsInner.getLength() - 1);
        convertStringExtra(stringp, dOut, true);
    }

    @Override
    public void convertInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        writeIntegerAsDecimal(longp, dOut);
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
        dOut.writeLong(negativeValue ? -value : value);
    }

    @Override
    public void convertUntypedAtomic(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        convertString(stringp, dOut);
    }

    /**
     * Derived Datatypes
     */
    public void convertByte(BytePointable bytep, DataOutput dOut) throws SystemException, IOException {
        writeIntegerAsDecimal(bytep, dOut);
    }

    public void convertInt(IntegerPointable intp, DataOutput dOut) throws SystemException, IOException {
        writeIntegerAsDecimal(intp, dOut);
    }

    public void convertLong(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        writeIntegerAsDecimal(longp, dOut);
    }

    public void convertNegativeInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        writeIntegerAsDecimal(longp, dOut);
    }

    public void convertNonNegativeInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        writeIntegerAsDecimal(longp, dOut);
    }

    public void convertNonPositiveInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        writeIntegerAsDecimal(longp, dOut);
    }

    public void convertPositiveInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        writeIntegerAsDecimal(longp, dOut);
    }

    public void convertShort(ShortPointable shortp, DataOutput dOut) throws SystemException, IOException {
        writeIntegerAsDecimal(shortp, dOut);
    }

    public void convertUnsignedByte(ShortPointable shortp, DataOutput dOut) throws SystemException, IOException {
        writeIntegerAsDecimal(shortp, dOut);
    }

    public void convertUnsignedInt(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        writeIntegerAsDecimal(longp, dOut);
    }

    public void convertUnsignedLong(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        writeIntegerAsDecimal(longp, dOut);
    }

    public void convertUnsignedShort(IntegerPointable intp, DataOutput dOut) throws SystemException, IOException {
        writeIntegerAsDecimal(intp, dOut);
    }

    private void writeIntegerAsDecimal(INumeric numericp, DataOutput dOut) throws SystemException, IOException {
        byte decimalPlace = 0;
        long value = numericp.longValue();

        // Normalize the value and take off trailing zeros.
        while (value != 0 && value % 10 == 0) {
            value /= 10;
            --decimalPlace;
        }

        dOut.write(ValueTag.XS_DECIMAL_TAG);
        dOut.write(decimalPlace);
        dOut.writeLong(value);
    }

}
