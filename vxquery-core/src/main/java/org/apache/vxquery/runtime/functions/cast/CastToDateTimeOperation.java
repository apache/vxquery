package org.apache.vxquery.runtime.functions.cast;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.atomic.XSDatePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDateTimePointable;
import org.apache.vxquery.datamodel.util.DateTime;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.strings.ICharacterIterator;
import org.apache.vxquery.runtime.functions.strings.UTF8StringCharacterIterator;

import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

public class CastToDateTimeOperation extends AbstractCastToOperation {

    @Override
    public void convertDate(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_DATETIME_TAG);
        dOut.writeShort((short) datep.getYear());
        dOut.write((byte) datep.getMonth());
        dOut.write((byte) datep.getDay());
        dOut.write((byte) 0); // Hour
        dOut.write((byte) 0); // Minute
        dOut.writeInt(0); // Millisecond
        dOut.write((byte) datep.getTimezoneHour());
        dOut.write((byte) datep.getTimezoneMinute());
    }

    @Override
    public void convertDatetime(XSDateTimePointable datetimep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_DATETIME_TAG);
        dOut.write(datetimep.getByteArray(), datetimep.getStartOffset(), datetimep.getLength());
    }

    @Override
    public void convertString(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        ICharacterIterator charIterator = new UTF8StringCharacterIterator(stringp);
        charIterator.reset();
        int c;
        int index = 0;
        long[] date = new long[8];
        boolean positiveTimezone = false;
        boolean pastDecimal = false;
        boolean negativeYear = false;
        byte decimalPlace = 3;

        // Set defaults
        date[6] = DateTime.TIMEZONE_HOUR_NULL;
        date[7] = DateTime.TIMEZONE_MINUTE_NULL;

        while ((c = charIterator.next()) != ICharacterIterator.EOS_CHAR) {
            if (Character.isDigit(c)) {
                // Add the digit to the current numbered index.
                date[index] = date[index] * 10 + Character.getNumericValue(c);
                if (pastDecimal) {
                    --decimalPlace;
                }
            } else if (c == Character.valueOf('-') && index == 0 && date[index] == 0) {
                // If the first dash does not have a number in front, its a negative year.
                negativeYear = true;
            } else if (c == Character.valueOf('-') || c == Character.valueOf(':') || c == Character.valueOf('T')) {
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
                date[6] = 0;
                date[7] = 0;
            } else {
                // Invalid date format.
                throw new SystemException(ErrorCode.FORG0001);
            }
        }

        // Final touches on year, seconds and timezone.
        date[5] = (long) (date[5] * Math.pow(10, decimalPlace));
        if (negativeYear) {
            date[0] *= -1;
        }
        if (!positiveTimezone && date[6] != DateTime.TIMEZONE_HOUR_NULL) {
            date[6] *= -1;
        }
        if (!positiveTimezone && date[7] != DateTime.TIMEZONE_MINUTE_NULL) {
            date[7] *= -1;
        }
        if (date[3] == 24) {
            date[3] = 0;
        }

        // Double check for a valid datetime
        if (!DateTime.valid(date[0], date[1], date[2], date[3], date[4], date[5], date[6], date[7])) {
            throw new SystemException(ErrorCode.FORG0001);
        }

        dOut.write(ValueTag.XS_DATETIME_TAG);
        dOut.writeShort((short) date[0]);
        dOut.write((byte) date[1]);
        dOut.write((byte) date[2]);
        dOut.write((byte) date[3]);
        dOut.write((byte) date[4]);
        dOut.writeInt((int) date[5]);
        dOut.write((byte) date[6]);
        dOut.write((byte) date[7]);
    }

    @Override
    public void convertUntypedAtomic(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        convertString(stringp, dOut);
    }

}