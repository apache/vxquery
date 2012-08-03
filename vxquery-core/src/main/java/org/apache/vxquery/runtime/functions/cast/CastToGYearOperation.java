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

public class CastToGYearOperation extends AbstractCastToOperation {

    @Override
    public void convertDate(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_G_YEAR_TAG);
        dOut.writeShort((short) datep.getYear());
        dOut.write((byte) 1);
        dOut.write((byte) 1);
        dOut.write((byte) datep.getTimezoneHour());
        dOut.write((byte) datep.getTimezoneMinute());
    }

    @Override
    public void convertDatetime(XSDateTimePointable datetimep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_G_YEAR_TAG);
        dOut.writeShort((short) datetimep.getYear());
        dOut.write((byte) 1);
        dOut.write((byte) 1);
        dOut.write((byte) datetimep.getTimezoneHour());
        dOut.write((byte) datetimep.getTimezoneMinute());
    }

    @Override
    public void convertGYear(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_G_YEAR_TAG);
        dOut.write(datep.getByteArray(), datep.getStartOffset(), datep.getLength());
    }

    @Override
    public void convertString(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        ICharacterIterator charIterator = new UTF8StringCharacterIterator(stringp);
        charIterator.reset();
        int c;
        int index = 0;
        long[] date = new long[3];
        boolean positiveTimezone = false;
        boolean negativeYear = false;

        // Set defaults
        date[1] = DateTime.TIMEZONE_HOUR_NULL;
        date[2] = DateTime.TIMEZONE_MINUTE_NULL;

        while ((c = charIterator.next()) != ICharacterIterator.EOS_CHAR) {
            if (Character.isDigit(c)) {
                // Add the digit to the current numbered index.
                date[index] = date[index] * 10 + Character.getNumericValue(c);
            } else if (c == Character.valueOf('-') && index == 0 && date[index] == 0) {
                // If the first dash does not have a number in front, its a negative year.
                negativeYear = true;
            } else if (c == Character.valueOf('-') || c == Character.valueOf(':')) {
                // The basic case for going to the next number in the series.
                ++index;
                date[index] = 0;
            } else if (c == Character.valueOf('+')) {
                // Moving to the next number and logging this is now a positive timezone offset.
                ++index;
                date[index] = 0;
                positiveTimezone = true;
            } else if (c == Character.valueOf('Z')) {
                // Set the timezone to UTC.
                date[1] = 0;
                date[2] = 0;
            } else {
                // Invalid date format.
                throw new SystemException(ErrorCode.FORG0001);
            }
        }
        // Final touches on year and timezone.
        if (negativeYear) {
            date[0] *= -1;
        }
        if (!positiveTimezone && date[1] != DateTime.TIMEZONE_HOUR_NULL) {
            date[1] *= -1;
        }
        if (!positiveTimezone && date[2] != DateTime.TIMEZONE_MINUTE_NULL) {
            date[2] *= -1;
        }
        if (!DateTime.valid(date[0], 1, 1, 0, 0, 0, date[1], date[2])) {
            throw new SystemException(ErrorCode.FODT0001);
        }

        dOut.write(ValueTag.XS_G_YEAR_TAG);
        dOut.writeShort((short) date[0]);
        dOut.write((byte) 1);
        dOut.write((byte) 1);
        dOut.write((byte) date[1]);
        dOut.write((byte) date[2]);
    }

    @Override
    public void convertUntypedAtomic(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        convertString(stringp, dOut);
    }

}