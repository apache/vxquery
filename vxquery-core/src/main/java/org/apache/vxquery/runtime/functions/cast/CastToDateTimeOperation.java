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
        byte decimalPlace = 3;

        // Set defaults
        date[6] = DateTime.TIMEZONE_HOUR_NULL;
        date[7] = DateTime.TIMEZONE_MINUTE_NULL;

        while ((c = charIterator.next()) != ICharacterIterator.EOS_CHAR) {
            if (Character.isDigit(c)) {
                date[index] = date[index] * 10 + Character.getNumericValue(c);
                if (pastDecimal) {
                    --decimalPlace;
                }
            } else if (c == Character.valueOf('-') || c == Character.valueOf(':') || c == Character.valueOf('T')) {
                ++index;
                pastDecimal = false;
            } else if (c == Character.valueOf('+')) {
                pastDecimal = false;
                positiveTimezone = true;
                ++index;
            } else if (c == Character.valueOf('.')) {
                pastDecimal = true;
            } else {
                // Invalid date format.
                throw new SystemException(ErrorCode.FORG0001);
            }
        }
        // Final touches on seconds and timezone.
        date[5] = (long) (date[5] * Math.pow(10, decimalPlace));
        if (!positiveTimezone && date[6] != DateTime.TIMEZONE_HOUR_NULL) {
            date[6] *= -1;
        }
        if (!positiveTimezone && date[7] != DateTime.TIMEZONE_MINUTE_NULL) {
            date[7] *= -1;
        }
        // Double check for a valid datetime
        if (!DateTime.valid(date[0], date[1], date[2], date[3], date[4], date[5], date[6], date[7])) {
            throw new SystemException(ErrorCode.FODT0001);
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