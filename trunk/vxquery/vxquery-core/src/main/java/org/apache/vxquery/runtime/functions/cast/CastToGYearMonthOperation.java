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

public class CastToGYearMonthOperation extends AbstractCastToOperation {

    @Override
    public void convertDate(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_G_YEAR_MONTH_TAG);
        dOut.writeShort((short) datep.getYear());
        dOut.write((byte) datep.getMonth());
        if (DateTime.isLeapYear(datep.getYear())) {
            dOut.write((byte) DateTime.DAYS_OF_MONTH_ORDI[(int) (datep.getMonth() - 1)]);
        } else {
            dOut.write((byte) DateTime.DAYS_OF_MONTH_LEAP[(int) (datep.getMonth() - 1)]);
        }
        dOut.write((byte) DateTime.TIMEZONE_HOUR_NULL);
        dOut.write((byte) DateTime.TIMEZONE_MINUTE_NULL);
    }

    @Override
    public void convertDatetime(XSDateTimePointable datetimep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_G_YEAR_MONTH_TAG);
        dOut.writeShort((short) datetimep.getYear());
        dOut.write((byte) datetimep.getMonth());
        if (DateTime.isLeapYear(datetimep.getYear())) {
            dOut.write((byte) DateTime.DAYS_OF_MONTH_ORDI[(int) (datetimep.getMonth() - 1)]);
        } else {
            dOut.write((byte) DateTime.DAYS_OF_MONTH_LEAP[(int) (datetimep.getMonth() - 1)]);
        }
        dOut.write((byte) 0);
        dOut.write((byte) 0);
        dOut.writeInt((int) 0);
        dOut.write((byte) DateTime.TIMEZONE_HOUR_NULL);
        dOut.write((byte) DateTime.TIMEZONE_MINUTE_NULL);
    }

    @Override
    public void convertGYearMonth(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_G_YEAR_MONTH_TAG);
        dOut.write(datep.getByteArray(), datep.getStartOffset(), datep.getLength());
    }

    @Override
    public void convertString(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        ICharacterIterator charIterator = new UTF8StringCharacterIterator(stringp);
        charIterator.reset();
        int c;
        int index = 0;
        long[] date = new long[4];
        boolean positiveTimezone = false;

        // Set defaults
        date[2] = DateTime.TIMEZONE_HOUR_NULL;
        date[3] = DateTime.TIMEZONE_MINUTE_NULL;

        while ((c = charIterator.next()) != ICharacterIterator.EOS_CHAR) {
            if (Character.isDigit(c)) {
                date[index] = date[index] * 10 + Character.getNumericValue(c);
            } else if (c == Character.valueOf('-') || c == Character.valueOf(':')) {
                ++index;
            } else if (c == Character.valueOf('+')) {
                positiveTimezone = true;
                ++index;
            } else {
                // Invalid date format.
                throw new SystemException(ErrorCode.FORG0001);
            }
        }
        long day;
        if (DateTime.isLeapYear(date[0])) {
            day = DateTime.DAYS_OF_MONTH_ORDI[(int) (date[1] - 1)];
        } else {
            day = DateTime.DAYS_OF_MONTH_LEAP[(int) (date[1] - 1)];
        }
        if (!DateTime.valid(date[0], date[1], day, 0, 0, 0, date[2], date[3])) {
            throw new SystemException(ErrorCode.FODT0001);
        }

        dOut.write(ValueTag.XS_G_YEAR_MONTH_TAG);
        dOut.writeShort((short) date[0]);
        dOut.write((byte) date[1]);
        dOut.write((byte) day);
        dOut.write((byte) ((positiveTimezone ? 1 : -1) * date[2]));
        dOut.write((byte) ((positiveTimezone ? 1 : -1) * date[3]));
    }

    @Override
    public void convertUntypedAtomic(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        convertString(stringp, dOut);
    }

}