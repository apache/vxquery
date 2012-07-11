package org.apache.vxquery.datamodel.util;

import org.apache.vxquery.datamodel.accessors.atomic.XSDateTimePointable;

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
     * 
     * @param year
     * @return
     */
    public static void normalizeDateTime(byte[] bytes, int start, long yearMonth, long dayTime) {
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
        XSDateTimePointable.setDateTime(bytes, 0, year, month, day, hour, minute, millisecond, 0, 0);
    }

}
