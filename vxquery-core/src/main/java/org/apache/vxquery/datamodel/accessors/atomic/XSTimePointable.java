package org.apache.vxquery.datamodel.accessors.atomic;

import org.apache.vxquery.datamodel.api.ITime;
import org.apache.vxquery.datamodel.api.ITimezone;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.api.AbstractPointable;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.api.IPointableFactory;
import edu.uci.ics.hyracks.data.std.primitive.BytePointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;

/**
 * The time is split up into five sections. Due to leap year, we have decided to keep the
 * storage simple by saving each date section separately. For calculations you can access
 * DayTime (milliseconds) values.
 * 
 * @author prestoncarman
 */
public class XSTimePointable extends AbstractPointable implements ITime, ITimezone {
    public final static int HOUR_OFFSET = 0;
    public final static int MINUTE_OFFSET = 1;
    public final static int MILLISECOND_OFFSET = 2;
    public final static int TIMEZONE_HOUR_OFFSET = 6;
    public final static int TIMEZONE_MINUTE_OFFSET = 7;

    public static final ITypeTraits TYPE_TRAITS = new ITypeTraits() {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isFixedLength() {
            return true;
        }

        @Override
        public int getFixedLength() {
            return 4;
        }
    };

    public static final IPointableFactory FACTORY = new IPointableFactory() {
        private static final long serialVersionUID = 1L;

        @Override
        public IPointable createPointable() {
            return new XSTimePointable();
        }

        @Override
        public ITypeTraits getTypeTraits() {
            return TYPE_TRAITS;
        }
    };

    public void setTime(long hour, long minute, long second, long timezoneHour, long timezoneMinute) {
        setTime(bytes, start, hour, minute, second, timezoneHour, timezoneMinute);
    }

    public static void setTime(byte[] bytes, int start, long hour, long minute, long second, long timezoneHour, long timezoneMinute) {
        BytePointable.setByte(bytes, start + HOUR_OFFSET, (byte) hour);
        BytePointable.setByte(bytes, start + MINUTE_OFFSET, (byte) minute);
        IntegerPointable.setInteger(bytes, start + MILLISECOND_OFFSET, (byte) second);
        BytePointable.setByte(bytes, start + TIMEZONE_HOUR_OFFSET, (byte) timezoneHour);
        BytePointable.setByte(bytes, start + TIMEZONE_MINUTE_OFFSET, (byte) timezoneMinute);
    }

    @Override
    public long getHour() {
        return getHour(bytes, start);
    }

    public static long getHour(byte[] bytes, int start) {
        return (long) BytePointable.getByte(bytes, start + HOUR_OFFSET);
    }

    @Override
    public long getMinute() {
        return getMinute(bytes, start);
    }

    public static long getMinute(byte[] bytes, int start) {
        return (long) BytePointable.getByte(bytes, start + MINUTE_OFFSET);
    }

    @Override
    public long getMilliSecond() {
        return getMilliSecond(bytes, start);
    }

    public static long getMilliSecond(byte[] bytes, int start) {
        return (long) IntegerPointable.getInteger(bytes, start + MILLISECOND_OFFSET);
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
    public long getDayTime() {
        return getDayTime(bytes, start);
    }

    public static long getDayTime(byte[] bytes, int start) {
        return (((getHour(bytes, start)) * 60 + getMinute(bytes, start)) * 60 * 1000 + getMilliSecond(bytes, start));
    }

    @Override
    public long getYearMonth() {
        return getYearMonth(bytes, start);
    }

    public static long getYearMonth(byte[] bytes, int start) {
        return 0;
    }

}