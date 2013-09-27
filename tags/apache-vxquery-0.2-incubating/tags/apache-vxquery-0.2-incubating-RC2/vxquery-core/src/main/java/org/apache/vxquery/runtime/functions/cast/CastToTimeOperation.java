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

import org.apache.vxquery.datamodel.accessors.atomic.XSDateTimePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSTimePointable;
import org.apache.vxquery.datamodel.util.DateTime;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.strings.ICharacterIterator;
import org.apache.vxquery.runtime.functions.strings.UTF8StringCharacterIterator;

import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

public class CastToTimeOperation extends AbstractCastToOperation {

    @Override
    public void convertDatetime(XSDateTimePointable datetimep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_TIME_TAG);
        dOut.write((byte) datetimep.getHour());
        dOut.write((byte) datetimep.getMinute());
        dOut.writeInt((int) datetimep.getMilliSecond());
        dOut.write((byte) datetimep.getTimezoneHour());
        dOut.write((byte) datetimep.getTimezoneMinute());
    }

    @Override
    public void convertString(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        ICharacterIterator charIterator = new UTF8StringCharacterIterator(stringp);
        charIterator.reset();
        int c;
        int index = 0;
        long[] date = new long[5];
        boolean positiveTimezone = false;
        boolean pastDecimal = false;
        byte decimalPlace = 3;

        // Set defaults
        date[3] = DateTime.TIMEZONE_HOUR_NULL;
        date[4] = DateTime.TIMEZONE_MINUTE_NULL;

        while ((c = charIterator.next()) != ICharacterIterator.EOS_CHAR) {
            if (Character.isDigit(c)) {
                // Add the digit to the current numbered index.
                date[index] = date[index] * 10 + Character.getNumericValue(c);
                if (pastDecimal) {
                    --decimalPlace;
                }
            } else if (c == Character.valueOf('-') || c == Character.valueOf(':')) {
                // The basic case for going to the next number in the series.
                ++index;
                pastDecimal = false;
                date[index] = 0;
            } else if (c == Character.valueOf('+')) {
                // Moving to the next number and logging this is now a positive timezone offset.
                ++index;
                pastDecimal = false;
                date[index] = 0;
                positiveTimezone = true;
            } else if (c == Character.valueOf('.')) {
                // Only used by the seconds attribute.
                pastDecimal = true;
            } else if (c == Character.valueOf('Z')) {
                // Set the timezone to UTC.
                date[3] = 0;
                date[4] = 0;
            } else {
                // Invalid date format.
                throw new SystemException(ErrorCode.FORG0001);
            }
        }
        // Final touches on seconds and timezone.
        date[2] = (long) (date[2] * Math.pow(10, decimalPlace));
        if (!positiveTimezone && date[3] != DateTime.TIMEZONE_HOUR_NULL) {
            date[3] *= -1;
        }
        if (!positiveTimezone && date[4] != DateTime.TIMEZONE_MINUTE_NULL) {
            date[4] *= -1;
        }
        if (date[0] == 24) {
            date[0] = 0;
        }

        // Double check for a valid time
        if (!DateTime.valid(1972, 12, 31, date[0], date[1], date[2], date[3], date[4])) {
            throw new SystemException(ErrorCode.FORG0001);
        }

        dOut.write(ValueTag.XS_TIME_TAG);
        dOut.write((byte) date[0]);
        dOut.write((byte) date[1]);
        dOut.writeInt((int) date[2]);
        dOut.write((byte) date[3]);
        dOut.write((byte) date[4]);
    }

    @Override
    public void convertTime(XSTimePointable timep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_TIME_TAG);
        dOut.write(timep.getByteArray(), timep.getStartOffset(), timep.getLength());
    }

    @Override
    public void convertUntypedAtomic(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        convertString(stringp, dOut);
    }

}