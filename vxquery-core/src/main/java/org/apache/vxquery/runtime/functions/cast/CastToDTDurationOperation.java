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

import org.apache.vxquery.datamodel.accessors.atomic.XSDurationPointable;
import org.apache.vxquery.datamodel.util.DateTime;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.strings.ICharacterIterator;
import org.apache.vxquery.runtime.functions.strings.UTF8StringCharacterIterator;

import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;

public class CastToDTDurationOperation extends AbstractCastToOperation {

    @Override
    public void convertDTDuration(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.write(longp.getByteArray(), longp.getStartOffset(), longp.getLength());
    }

    @Override
    public void convertDuration(XSDurationPointable durationp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeLong(durationp.getDayTime());
    }

    @Override
    public void convertString(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        ICharacterIterator charIterator = new UTF8StringCharacterIterator(stringp);
        charIterator.reset();
        boolean pastDecimal = false, timeSection = false;
        byte decimalPlace = 3;

        int value = 0;
        long day = 0, hour = 0, minute = 0, millisecond = 0;
        long negativeResult = 1;

        // First character 
        int c = charIterator.next();
        if (c == Character.valueOf('-')) {
            negativeResult = -1;
            c = charIterator.next();
        }
        if (c != Character.valueOf('P')) {
            // Invalid duration format.
            throw new SystemException(ErrorCode.FORG0001);
        }

        while ((c = charIterator.next()) != ICharacterIterator.EOS_CHAR) {
            if (Character.isDigit(c)) {
                value = value * 10 + Character.getNumericValue(c);
                if (pastDecimal) {
                    --decimalPlace;
                }
            } else if (c == Character.valueOf('T')) {
                timeSection = true;
            } else if (c == Character.valueOf('.')) {
                pastDecimal = true;
            } else if (c == Character.valueOf('D') && !timeSection) {
                day = value;
                value = 0;
                pastDecimal = false;
            } else if (c == Character.valueOf('H') && timeSection) {
                hour = value;
                value = 0;
                pastDecimal = false;
            } else if (c == Character.valueOf('M') && timeSection) {
                minute = value;
                value = 0;
                pastDecimal = false;
            } else if (c == Character.valueOf('S') && timeSection) {
                millisecond = (long) (value * Math.pow(10, decimalPlace));
                value = 0;
                pastDecimal = false;
            } else {
                // Invalid duration format.
                throw new SystemException(ErrorCode.FORG0001);
            }
        }

        long dayTime = day * DateTime.CHRONON_OF_DAY + hour * DateTime.CHRONON_OF_HOUR + minute
                * DateTime.CHRONON_OF_MINUTE + millisecond;
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeLong(negativeResult * dayTime);
    }

    @Override
    public void convertYMDuration(IntegerPointable intp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeLong(0);
    }

    @Override
    public void convertUntypedAtomic(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        convertString(stringp, dOut);
    }

}
