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
        writeIntegerAsDouble(longp, dOut);
    }

    @Override
    public void convertString(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        ICharacterIterator charIterator = new UTF8StringCharacterIterator(stringp);
        charIterator.reset();
        double valueDouble;
        boolean negativeValue = false;
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
                valueDouble = Double.POSITIVE_INFINITY;
            } else if (c == Character.valueOf('N') && c2 == Character.valueOf('a') && c3 == Character.valueOf('N')) {
                valueDouble = Double.NaN;
            } else {
                throw new SystemException(ErrorCode.FORG0001);
            }
            dOut.write(ValueTag.XS_DOUBLE_TAG);
            dOut.writeDouble((negativeValue ? -valueDouble : valueDouble));
        } else {
            // We create an object to keep the conversion algorithm simple and improve precision.
            // While a better solution may be available this will hold us over until then.
            StringBuilder sb = new StringBuilder();
            stringp.toString(sb);
            try {
                valueDouble = Double.parseDouble(sb.toString());
            } catch (NumberFormatException e) {
                throw new SystemException(ErrorCode.FORG0001);
            }
            dOut.write(ValueTag.XS_DOUBLE_TAG);
            dOut.writeDouble(valueDouble);
        }
    }

    @Override
    public void convertUntypedAtomic(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        convertString(stringp, dOut);
    }

    /**
     * Derived Datatypes
     */
    public void convertByte(BytePointable bytep, DataOutput dOut) throws SystemException, IOException {
        writeIntegerAsDouble(bytep, dOut);
    }

    public void convertInt(IntegerPointable intp, DataOutput dOut) throws SystemException, IOException {
        writeIntegerAsDouble(intp, dOut);
    }

    public void convertLong(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        writeIntegerAsDouble(longp, dOut);
    }

    public void convertNegativeInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        writeIntegerAsDouble(longp, dOut);
    }

    public void convertNonNegativeInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        writeIntegerAsDouble(longp, dOut);
    }

    public void convertNonPositiveInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        writeIntegerAsDouble(longp, dOut);
    }

    public void convertPositiveInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        writeIntegerAsDouble(longp, dOut);
    }

    public void convertShort(ShortPointable shortp, DataOutput dOut) throws SystemException, IOException {
        writeIntegerAsDouble(shortp, dOut);
    }

    public void convertUnsignedByte(ShortPointable shortp, DataOutput dOut) throws SystemException, IOException {
        writeIntegerAsDouble(shortp, dOut);
    }

    public void convertUnsignedInt(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        writeIntegerAsDouble(longp, dOut);
    }

    public void convertUnsignedLong(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        writeIntegerAsDouble(longp, dOut);
    }

    public void convertUnsignedShort(IntegerPointable intp, DataOutput dOut) throws SystemException, IOException {
        writeIntegerAsDouble(intp, dOut);
    }

    private void writeIntegerAsDouble(INumeric numericp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.writeDouble(numericp.doubleValue());
    }

}