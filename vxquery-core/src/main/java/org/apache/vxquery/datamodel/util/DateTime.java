package org.apache.vxquery.datamodel.util;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.datamodel.accessors.atomic.XSDatePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDateTimePointable;
import org.apache.vxquery.datamodel.api.ITimezone;
import org.apache.vxquery.datamodel.values.ValueTag;

public class DateTime {
    public static final long[] DAYS_OF_MONTH_ORDI = { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    public static final long[] DAYS_OF_MONTH_LEAP = { 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };

    public static final long CHRONON_OF_SECOND = 1000;
    public static final long CHRONON_OF_MINUTE = 60 * CHRONON_OF_SECOND;
    public static final long CHRONON_OF_HOUR = 60 * CHRONON_OF_MINUTE;
    public static final long CHRONON_OF_DAY = 24 * CHRONON_OF_HOUR;

    /**
     * Minimum feasible value of each field
     */
    public static final int[] FIELD_MINS = { Short.MIN_VALUE, // year
            1, // month
            1, // day
            0, // hour
            0, // minute
            0 // millisecond
    };

    public static final int[] FIELD_MAXS = { Short.MAX_VALUE, // year
            12, // month
            31, // day
            23, // hour
            59, // minute
            59999 // millisecond
    };

    public static final int TIMEZONE_HOUR_MIN = -12, TIMEZONE_HOUR_MAX = 14, TIMEZONE_MIN_MIN = -60,
            TIMEZONE_MIN_MAX = 60;
    // Used to store the timezone value when one does not exist.
    public static final byte TIMEZONE_HOUR_NULL = 127, TIMEZONE_MIN_NULL = 127;

    public static final int YEAR_FIELD_INDEX = 0, MONTH_FIELD_INDEX = 1, DAY_FIELD_INDEX = 2, HOUR_FIELD_INDEX = 3,
            MINUTE_FIELD_INDEX = 4, MILLISECOND_FIELD_INDEX = 5;

    /**
     * Check whether a given year is a leap year.
     * 
     * @param year
     * @return
     */
    public static boolean isLeapYear(long year) {
        return ((year & 3) == 0) && ((year % 100) != 0 || (year % 400) == 0);
    }

    /**
     * Return a normalized time.
     */
    public static void normalizeDateTime(long yearMonth, long dayTime, DataOutput dOut) throws Exception {
        long[] monthDayLimits;

        long day = dayTime / CHRONON_OF_DAY;
        dayTime %= CHRONON_OF_DAY;
        long hour = dayTime / CHRONON_OF_HOUR;
        dayTime %= CHRONON_OF_HOUR;
        long minute = dayTime / CHRONON_OF_MINUTE;
        dayTime %= CHRONON_OF_MINUTE;
        long millisecond = dayTime;
        long month = yearMonth % 12;
        long year = yearMonth / 12;

        monthDayLimits = (isLeapYear(year) ? DateTime.DAYS_OF_MONTH_LEAP : DateTime.DAYS_OF_MONTH_ORDI);
        while (day < DateTime.FIELD_MINS[DateTime.DAY_FIELD_INDEX] || day > monthDayLimits[(int) month - 1]
                || month < DateTime.FIELD_MINS[DateTime.MONTH_FIELD_INDEX]
                || month > DateTime.FIELD_MAXS[DateTime.MONTH_FIELD_INDEX]) {
            if (day < DateTime.FIELD_MINS[DateTime.DAY_FIELD_INDEX]) {
                // Too small
                --month;
                day += monthDayLimits[(int) month - 1];
            }
            if (day > monthDayLimits[(int) month - 1]) {
                // Too large
                day -= monthDayLimits[(int) month - 1];
                ++month;
            }
            if (month < DateTime.FIELD_MINS[DateTime.MONTH_FIELD_INDEX]) {
                // Too small
                month = DateTime.FIELD_MAXS[DateTime.MONTH_FIELD_INDEX];
                --year;
            } else if (month > DateTime.FIELD_MAXS[DateTime.MONTH_FIELD_INDEX]) {
                // Too large
                month = DateTime.FIELD_MINS[DateTime.MONTH_FIELD_INDEX];
                ++year;
            }
            monthDayLimits = (isLeapYear(year) ? DateTime.DAYS_OF_MONTH_LEAP : DateTime.DAYS_OF_MONTH_ORDI);
        }
        dOut.write(ValueTag.XS_DATETIME_TAG);
        dOut.writeShort((short) year);
        dOut.writeByte((byte) month);
        dOut.writeByte((byte) day);
        dOut.writeByte((byte) hour);
        dOut.writeByte((byte) minute);
        dOut.writeInt((int) millisecond);
        dOut.writeByte((byte) DateTime.TIMEZONE_HOUR_NULL);
        dOut.writeByte((byte) DateTime.TIMEZONE_MIN_NULL);
    }

    public static void getTimezoneDateTime(ITimezone timezonep, DynamicContext dCtx, DataOutput dOut)
            throws Exception {
        long timezoneHour;
        long timezoneMinute;
        // Consider time zones.
        if (timezonep.getTimezoneHour() == DateTime.TIMEZONE_HOUR_NULL
                || timezonep.getTimezoneMinute() == DateTime.TIMEZONE_MIN_NULL) {
            XSDateTimePointable defaultTimezone = new XSDateTimePointable();
            dCtx.getCurrentDateTime(defaultTimezone);
            timezoneHour = defaultTimezone.getTimezoneHour();
            timezoneMinute = defaultTimezone.getTimezoneMinute();
        } else {
            timezoneHour = timezonep.getTimezoneHour();
            timezoneMinute = timezonep.getTimezoneMinute();
        }
        long dayTime = timezonep.getDayTime() + timezoneHour * DateTime.CHRONON_OF_HOUR + timezoneMinute
                * DateTime.CHRONON_OF_HOUR;
        DateTime.normalizeDateTime(timezonep.getYearMonth(), dayTime, dOut);
    }

}
