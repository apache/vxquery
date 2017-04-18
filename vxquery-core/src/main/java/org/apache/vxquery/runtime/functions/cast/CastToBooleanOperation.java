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

import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.strings.ICharacterIterator;
import org.apache.vxquery.runtime.functions.strings.LowerCaseCharacterIterator;
import org.apache.vxquery.runtime.functions.strings.UTF8StringCharacterIterator;

public class CastToBooleanOperation extends AbstractCastToOperation {

    @Override
    public void convertBoolean(BooleanPointable boolp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write(boolp.getByteArray(), boolp.getStartOffset(), boolp.getLength());
    }

    @Override
    public void convertDecimal(XSDecimalPointable decp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        if (decp.getDecimalValue() == 0 && decp.getBeforeDecimalPlace() == 0) {
            dOut.write(0);
        } else {
            dOut.write(1);
        }
    }

    @Override
    public void convertDouble(DoublePointable doublep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        if (Double.isNaN(doublep.getDouble()) || doublep.getDouble() == 0) {
            dOut.write(0);
        } else {
            dOut.write(1);
        }
    }

    @Override
    public void convertFloat(FloatPointable floatp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        if (Float.isNaN(floatp.getFloat()) || floatp.getFloat() == 0) {
            dOut.write(0);
        } else {
            dOut.write(1);
        }
    }

    @Override
    public void convertInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        if (longp.getLong() == 0) {
            dOut.write(0);
        } else {
            dOut.write(1);
        }
    }

    @Override
    public void convertString(UTF8StringPointable stringp, DataOutput dOut) throws IOException {
        byte result = 2;
        ICharacterIterator charIterator = new LowerCaseCharacterIterator(new UTF8StringCharacterIterator(stringp));
        charIterator.reset();

        int c1 = charIterator.next();
        int c2 = charIterator.next();
        int c3 = charIterator.next();
        int c4 = charIterator.next();
        int c5 = charIterator.next();
        int c6 = charIterator.next();

        if (c1 == Character.valueOf('1') && c2 == ICharacterIterator.EOS_CHAR) {
            result = 1;
        } else if (c1 == Character.valueOf('0') && c2 == ICharacterIterator.EOS_CHAR) {
            result = 0;
        } else if (c1 == Character.valueOf('t') && c2 == Character.valueOf('r') && c3 == Character.valueOf('u')
                && c4 == Character.valueOf('e') && c5 == ICharacterIterator.EOS_CHAR) {
            result = 1;
        } else if (c1 == Character.valueOf('f') && c2 == Character.valueOf('a') && c3 == Character.valueOf('l')
                && c4 == Character.valueOf('s') && c5 == Character.valueOf('e') && c6 == ICharacterIterator.EOS_CHAR) {
            result = 0;
        }
        if (result == 2) {
            throw new SystemException(ErrorCode.FORG0001);
        }
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write(result);
    }

    @Override
    public void convertUntypedAtomic(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        convertString(stringp, dOut);
    }
}
