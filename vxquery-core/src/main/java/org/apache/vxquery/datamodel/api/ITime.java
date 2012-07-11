package org.apache.vxquery.datamodel.api;

public interface ITime {
    public long getHour();

    public long getMinute();

    public long getMilliSecond();

    public long getTimezoneHour();

    public long getTimezoneMinute();

    public long getTimezone();

    public long getDayTime();

}
