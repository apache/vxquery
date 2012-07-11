package org.apache.vxquery.datamodel.accessors.atomic;

import org.apache.vxquery.datamodel.api.IDate;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.api.AbstractPointable;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.api.IPointableFactory;
import edu.uci.ics.hyracks.data.std.primitive.BytePointable;
import edu.uci.ics.hyracks.data.std.primitive.ShortPointable;

/**
 * The date is split up into five sections. Due to leap year, we have decided to keep the
 * storage simple by saving each date section separately. For calculations you can access
 * YearMonth (months) and DayTime (milliseconds) values.
 * 
 * @author prestoncarman
 */
public class XSDatePointable extends AbstractPointable implements IDate {
    public final static int YEAR_OFFSET = 0;
    public final static int MONTH_OFFSET = 2;
    public final static int DAY_OFFSET = 3;
    public final static int TIMEZONE_HOUR_OFFSET = 4;
    public final static int TIMEZONE_MINUTE_OFFSET = 5;
    // CHRONON_OF_DAY is used to convert days into milliseconds.
    private final static long CHRONON_OF_DAY = 24 * 60 * 60 * 1000;

    public static final ITypeTraits TYPE_TRAITS = new ITypeTraits() {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isFixedLength() {
            return true;
        }

        @Override
        public int getFixedLength() {
            return 6;
        }
    };

    public static final IPointableFactory FACTORY = new IPointableFactory() {
        private static final long serialVersionUID = 1L;

        @Override
        public IPointable createPointable() {
            return new XSDatePointable();
        }

        @Override
        public ITypeTraits getTypeTraits() {
            return TYPE_TRAITS;
        }
    };

    public void setDate(long year, long month, long day, long timezoneHour, long timezoneMinute) {
        setDate(bytes, start, year, month, day, timezoneHour, timezoneMinute);
    }

    public static void setDate(byte[] bytes, int start, long year, long month, long day, long timezoneHour, long timezoneMinute) {
        System.out.println("  day1 = " + day);
        System.out.println("  month1 = " + month);
        System.out.println("  year1 = " + year);
        System.out.println("  timezoneHour = " + timezoneHour);
        System.out.println("  timezoneMinute = " + timezoneMinute);
        ShortPointable.setShort(bytes, start + YEAR_OFFSET, (short) year);
        BytePointable.setByte(bytes, start + MONTH_OFFSET, (byte) month);
        BytePointable.setByte(bytes, start + DAY_OFFSET, (byte) day);
        BytePointable.setByte(bytes, start + TIMEZONE_HOUR_OFFSET, (byte) timezoneHour);
        BytePointable.setByte(bytes, start + TIMEZONE_MINUTE_OFFSET, (byte) timezoneMinute);
    }

    @Override
    public long getYear() {
        return getYear(bytes, start);
    }

    public static long getYear(byte[] bytes, int start) {
        return (long) ShortPointable.getShort(bytes, start + YEAR_OFFSET);
    }

    @Override
    public long getMonth() {
        return getMonth(bytes, start);
    }

    public static long getMonth(byte[] bytes, int start) {
        return (long) BytePointable.getByte(bytes, start + MONTH_OFFSET);
    }

    @Override
    public long getDay() {
        return getDay(bytes, start);
    }

    public static long getDay(byte[] bytes, int start) {
        return (long) BytePointable.getByte(bytes, start + DAY_OFFSET);
    }

    @Override
    public long getTimezoneHour() {
        return getTimezoneHour(bytes, start);
    }

    public static long getTimezoneHour(byte[] bytes, int start) {
        return (long) BytePointable.getByte(bytes, start + TIMEZONE_HOUR_OFFSET);
    }

    @Override
    public long getTimezoneMinute() {
        return getTimezoneMinute(bytes, start);
    }

    public static long getTimezoneMinute(byte[] bytes, int start) {
        return (long) BytePointable.getByte(bytes, start + TIMEZONE_MINUTE_OFFSET);
    }

    @Override
    public long getTimezone() {
        return getTimezone(bytes, start);
    }

    public static long getTimezone(byte[] bytes, int start) {
        return (getTimezoneHour(bytes, start) * 60 + getTimezoneMinute(bytes, start));
    }

    @Override
    public long getYearMonth() {
        return getYearMonth(bytes, start);
    }

    public static long getYearMonth(byte[] bytes, int start) {
        return (getYear(bytes, start) * 12 + getMonth(bytes, start));
    }

    @Override
    public long getDayTime() {
        return getDayTime(bytes, start);
    }

    public static long getDayTime(byte[] bytes, int start) {
        return getDay(bytes, start) * CHRONON_OF_DAY;
    }

}