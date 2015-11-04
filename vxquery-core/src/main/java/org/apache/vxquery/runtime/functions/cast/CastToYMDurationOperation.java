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
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.strings.ICharacterIterator;
import org.apache.vxquery.runtime.functions.strings.UTF8StringCharacterIterator;

import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

public class CastToYMDurationOperation extends AbstractCastToOperation {

    @Override
    public void convertDTDuration(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_YEAR_MONTH_DURATION_TAG);
        dOut.writeInt(0);
    }

    @Override
    public void convertDuration(XSDurationPointable durationp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_YEAR_MONTH_DURATION_TAG);
        dOut.writeInt(durationp.getYearMonth());
    }

    @Override
    public void convertString(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        ICharacterIterator charIterator = new UTF8StringCharacterIterator(stringp);
        charIterator.reset();

        int value = 0;
        long year = 0, month = 0;
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
            } else if (c == Character.valueOf('Y')) {
                year = value;
                value = 0;
            } else if (c == Character.valueOf('M')) {
                month = value;
                value = 0;
            } else {
                // Invalid duration format.
                throw new SystemException(ErrorCode.FORG0001);
            }
        }

        long yearMonth = year * 12 + month;
        dOut.write(ValueTag.XS_YEAR_MONTH_DURATION_TAG);
        dOut.writeInt((int) (negativeResult * yearMonth));
    }

    @Override
    public void convertUntypedAtomic(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        convertString(stringp, dOut);
    }

    @Override
    public void convertYMDuration(IntegerPointable intp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_YEAR_MONTH_DURATION_TAG);
        dOut.write(intp.getByteArray(), intp.getStartOffset(), intp.getLength());
    }

}