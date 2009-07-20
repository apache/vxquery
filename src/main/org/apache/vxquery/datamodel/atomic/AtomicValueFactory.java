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
package org.apache.vxquery.datamodel.atomic;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.GregorianCalendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeConstants;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.Duration;
import javax.xml.datatype.XMLGregorianCalendar;
import javax.xml.namespace.QName;

import org.apache.vxquery.datamodel.NameCache;
import org.apache.vxquery.datamodel.XDMValue;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.types.AtomicType;
import org.apache.vxquery.types.BuiltinTypeRegistry;

public final class AtomicValueFactory {
    /*
     * GYEARMONTH_PATTERN
     * 
     * 1 - minus
     * 
     * 2 - year
     * 
     * 3 - month
     * 
     * 5 - Z
     * 
     * 7 - TZ sign
     * 
     * 8 - TZ hours
     * 
     * 9 - TZ minutes
     */
    private static final Pattern GYEARMONTH_PATTERN = Pattern
            .compile("(-)?(\\d{4,})-(\\d{2})((Z)|((\\+|-)(\\d{2}):(\\d{2})))?");

    /*
     * GMONTHDAY_PATTERN
     * 
     * 1 - month
     * 
     * 2 - day
     * 
     * 4 - Z
     * 
     * 6 - TZ sign
     * 
     * 7 - TZ hours
     * 
     * 8 - TZ minutes
     */
    private static final Pattern GMONTHDAY_PATTERN = Pattern
            .compile("(\\d{2})-(\\d{2})((Z)|((\\+|-)(\\d{2}):(\\d{2})))?");

    /*
     * GYEAR_PATTERN
     * 
     * 1 - minus
     * 
     * 2 - year
     * 
     * 4 - Z
     * 
     * 6 - TZ sign
     * 
     * 7 - TZ hours
     * 
     * 8 - TZ minutes
     */
    private static final Pattern GYEAR_PATTERN = Pattern.compile("(-)?(\\d{4,})((Z)|((\\+|-)(\\d{2}):(\\d{2})))?");

    /*
     * GMONTH_PATTERN
     * 
     * 1 - month
     * 
     * 3 - Z
     * 
     * 5 - TZ sign
     * 
     * 6 - TZ hours
     * 
     * 7 - TZ minutes
     */
    private static final Pattern GMONTH_PATTERN = Pattern.compile("(\\d{2})((Z)|((\\+|-)(\\d{2}):(\\d{2})))?");

    /*
     * GDAY_PATTERN
     * 
     * 1 - day
     * 
     * 3 - Z
     * 
     * 5 - TZ sign
     * 
     * 6 - TZ hours
     * 
     * 7 - TZ minutes
     */
    private static final Pattern GDAY_PATTERN = Pattern.compile("(\\d{2})((Z)|((\\+|-)(\\d{2}):(\\d{2})))?");

    /*
     * DURATION_PATTERN
     * 
     * 1 - minus
     * 
     * 3 - year
     * 
     * 5 - month
     * 
     * 7 - day
     * 
     * 10 - hour
     * 
     * 12 - minute
     * 
     * 14 - seconds
     */
    private static final Pattern DURATION_PATTERN = Pattern
            .compile("(-)?P((\\d+)Y)?((\\d+)M)?((\\d+)D)?(T((\\d+)H)?((\\d+)M)?((\\d+(\\.\\d+)?)S)?)?");

    private static final DatatypeFactory DT_FACTORY;
    private NameCache nameCache;

    static {
        try {
            DT_FACTORY = DatatypeFactory.newInstance();
        } catch (DatatypeConfigurationException e) {
            e.printStackTrace();
            throw new IllegalStateException(e);
        }
    }

    public AtomicValueFactory(NameCache nameCache) {
        this.nameCache = nameCache;
    }

    public AnyUriValue createAnyUri(CharSequence value) {
        return new AnyUriValue(value);
    }

    public AnyUriValue createAnyUri(CharSequence value, AtomicType type) {
        return new AnyUriValue(value, type);
    }

    public BooleanValue createBoolean(boolean value) {
        return value ? BooleanValue.TRUE : BooleanValue.FALSE;
    }

    public DateTimeValue createDateTime(BigInteger year, int month, int day, int hour, int minute, int second,
            BigDecimal fSecond, int timezone) {
        XMLGregorianCalendar c = DT_FACTORY.newXMLGregorianCalendar(year, month, day, hour, minute, second, fSecond,
                timezone);
        return new DateTimeValue(c);
    }

    public DateTimeValue createDateTime(BigInteger year, int month, int day, int hour, int minute, int second,
            BigDecimal fSecond, int timezone, AtomicType type) {
        XMLGregorianCalendar c = DT_FACTORY.newXMLGregorianCalendar(year, month, day, hour, minute, second, fSecond,
                timezone);
        return new DateTimeValue(c, type);
    }

    public DateTimeValue createDateTime(BigInteger year, int month, int day, int hour, int minute, int second,
            BigDecimal fSecond) {
        XMLGregorianCalendar c = DT_FACTORY.newXMLGregorianCalendar(year, month, day, hour, minute, second, fSecond,
                DatatypeConstants.FIELD_UNDEFINED);
        return new DateTimeValue(c);
    }

    public DateTimeValue createDateTime(BigInteger year, int month, int day, int hour, int minute, int second,
            BigDecimal fSecond, AtomicType type) {
        XMLGregorianCalendar c = DT_FACTORY.newXMLGregorianCalendar(year, month, day, hour, minute, second, fSecond,
                DatatypeConstants.FIELD_UNDEFINED);
        return new DateTimeValue(c, type);
    }

    public XDMValue createDateTime(CharSequence value) {
        XMLGregorianCalendar c = DT_FACTORY.newXMLGregorianCalendar(value.toString());
        return new DateTimeValue(c);
    }

    public DateValue createDate(BigInteger year, int month, int day, int timezone) {
        XMLGregorianCalendar c = DT_FACTORY.newXMLGregorianCalendar(year, month, day,
                DatatypeConstants.FIELD_UNDEFINED, DatatypeConstants.FIELD_UNDEFINED,
                DatatypeConstants.FIELD_UNDEFINED, null, timezone);
        return new DateValue(c);
    }

    public DateValue createDate(BigInteger year, int month, int day, int timezone, AtomicType type) {
        XMLGregorianCalendar c = DT_FACTORY.newXMLGregorianCalendar(year, month, day,
                DatatypeConstants.FIELD_UNDEFINED, DatatypeConstants.FIELD_UNDEFINED,
                DatatypeConstants.FIELD_UNDEFINED, null, timezone);
        return new DateValue(c, type);
    }

    public DateValue createDate(BigInteger year, int month, int day) {
        XMLGregorianCalendar c = DT_FACTORY.newXMLGregorianCalendar(year, month, day,
                DatatypeConstants.FIELD_UNDEFINED, DatatypeConstants.FIELD_UNDEFINED,
                DatatypeConstants.FIELD_UNDEFINED, null, DatatypeConstants.FIELD_UNDEFINED);
        return new DateValue(c);
    }

    public DateValue createDate(BigInteger year, int month, int day, AtomicType type) {
        XMLGregorianCalendar c = DT_FACTORY.newXMLGregorianCalendar(year, month, day,
                DatatypeConstants.FIELD_UNDEFINED, DatatypeConstants.FIELD_UNDEFINED,
                DatatypeConstants.FIELD_UNDEFINED, null, DatatypeConstants.FIELD_UNDEFINED);
        return new DateValue(c, type);
    }

    public XDMValue createDate(CharSequence value) {
        XMLGregorianCalendar c = DT_FACTORY.newXMLGregorianCalendar(value.toString());
        return new DateTimeValue(c);
    }

    public TimeValue createTime(int hour, int minute, int second, BigDecimal fSecond, int timezone) {
        XMLGregorianCalendar c = DT_FACTORY.newXMLGregorianCalendar(null, DatatypeConstants.FIELD_UNDEFINED,
                DatatypeConstants.FIELD_UNDEFINED, hour, minute, second, fSecond, timezone);
        return new TimeValue(c);
    }

    public TimeValue createTime(int hour, int minute, int second, BigDecimal fSecond, int timezone, AtomicType type) {
        XMLGregorianCalendar c = DT_FACTORY.newXMLGregorianCalendar(null, DatatypeConstants.FIELD_UNDEFINED,
                DatatypeConstants.FIELD_UNDEFINED, hour, minute, second, fSecond, timezone);
        return new TimeValue(c, type);
    }

    public TimeValue createTime(int hour, int minute, int second, BigDecimal fSecond) {
        XMLGregorianCalendar c = DT_FACTORY.newXMLGregorianCalendar(null, DatatypeConstants.FIELD_UNDEFINED,
                DatatypeConstants.FIELD_UNDEFINED, hour, minute, second, fSecond, DatatypeConstants.FIELD_UNDEFINED);
        return new TimeValue(c);
    }

    public TimeValue createTime(int hour, int minute, int second, BigDecimal fSecond, AtomicType type) {
        XMLGregorianCalendar c = DT_FACTORY.newXMLGregorianCalendar(null, DatatypeConstants.FIELD_UNDEFINED,
                DatatypeConstants.FIELD_UNDEFINED, hour, minute, second, fSecond, DatatypeConstants.FIELD_UNDEFINED);
        return new TimeValue(c, type);
    }

    public XDMValue createTime(CharSequence value) {
        XMLGregorianCalendar c = DT_FACTORY.newXMLGregorianCalendar(value.toString());
        return new DateTimeValue(c);
    }

    public DecimalValue createDecimal(BigDecimal value) {
        return new DecimalValue(value);
    }

    public IntegerValue createInteger(BigInteger value) {
        return new IntegerValue(value);
    }

    public IntValue createInt(long value) {
        return new IntValue(value);
    }

    public DoubleValue createDouble(double value) {
        return new DoubleValue(value);
    }

    public FloatValue createFloat(float value) {
        return new FloatValue(value);
    }

    public QNameValue createQName(QName value) {
        return new QNameValue(nameCache, nameCache.intern(value.getPrefix(), value.getNamespaceURI(), value
                .getLocalPart()));
    }

    public QNameValue createQName(CharSequence prefix, CharSequence nsUri, CharSequence local) {
        return new QNameValue(nameCache, nameCache.intern(prefix.toString(), nsUri.toString(), local.toString()));
    }

    public QNameValue createQName(NameCache nameCache, int code) {
        return new QNameValue(nameCache, code);
    }

    public StringValue createString(CharSequence value) {
        return new StringValue(value);
    }

    public StringValue createString(CharSequence value, AtomicType type) {
        return new StringValue(value, type);
    }

    public UntypedAtomicValue createUntypedAtomic(CharSequence value) {
        return new UntypedAtomicValue(value);
    }

    public DurationValue createDuration(Duration duration) {
        AtomicType type = BuiltinTypeRegistry.XS_DURATION;
        if (duration.isSet(DatatypeConstants.YEARS)) {
        }
        QName typeName = duration.getXMLSchemaType();
        if (DatatypeConstants.DURATION_DAYTIME.equals(typeName)) {
            type = BuiltinTypeRegistry.XS_DAY_TIME_DURATION;
        } else if (DatatypeConstants.DURATION_YEARMONTH.equals(typeName)) {
            type = BuiltinTypeRegistry.XS_YEAR_MONTH_DURATION;
        }
        return new DurationValue(duration, type);
    }

    public DurationValue createDuration(long millis) {
        Duration d = DT_FACTORY.newDuration(millis);
        return createDuration(d);
    }

    public DurationValue createDuration(CharSequence value) throws SystemException {
        Matcher m = DURATION_PATTERN.matcher(value);
        if (!m.matches()) {
            throw new SystemException(ErrorCode.FORG0001);
        }
        String negStr = m.group(1);
        String yearStr = m.group(3);
        String monthStr = m.group(5);
        String dayStr = m.group(7);
        String hourStr = m.group(10);
        String minStr = m.group(12);
        String secStr = m.group(14);

        if (m.group(8) != null && hourStr == null && minStr == null && secStr == null) {
            throw new SystemException(ErrorCode.FORG0001);
        }

        boolean neg = negStr == null;
        BigInteger years = yearStr == null ? null : new BigInteger(yearStr);
        BigInteger months = monthStr == null ? null : new BigInteger(monthStr);
        BigInteger days = dayStr == null ? null : new BigInteger(dayStr);
        BigInteger hours = hourStr == null ? null : new BigInteger(hourStr);
        BigInteger minutes = minStr == null ? null : new BigInteger(minStr);
        BigDecimal seconds = secStr == null ? null : new BigDecimal(secStr);

        if (years != null || months != null) {
            if (years == null) {
                years = BigInteger.ZERO;
            }
            if (months == null) {
                months = BigInteger.ZERO;
            }
        }
        if (days != null || hours != null || minutes != null || seconds != null) {
            if (days == null) {
                days = BigInteger.ZERO;
            }
            if (hours == null) {
                hours = BigInteger.ZERO;
            }
            if (minutes == null) {
                minutes = BigInteger.ZERO;
            }
            if (seconds == null) {
                seconds = BigDecimal.ZERO;
            }
        }

        Duration d = DT_FACTORY.newDuration(neg, years, months, days, hours, minutes, seconds);
        return createDuration(d);
    }

    public DateValue createDate(XMLGregorianCalendar calendar) {
        return new DateValue(calendar);
    }

    public TimeValue createTime(XMLGregorianCalendar calendar) {
        return new TimeValue(calendar);
    }

    public DateTimeValue createDateTime(XMLGregorianCalendar calendar) {
        return new DateTimeValue(calendar);
    }

    public DateTimeValue createDateTime(GregorianCalendar calendar) {
        return createDateTime(DT_FACTORY.newXMLGregorianCalendar(calendar));
    }

    public StringValue createNCName(CharSequence value) {
        return new StringValue(value, BuiltinTypeRegistry.XS_NCNAME);
    }

    public XDMValue createDouble(CharSequence value) {
        return createDouble(value, BuiltinTypeRegistry.XS_DOUBLE);
    }

    public XDMValue createDouble(CharSequence value, AtomicType type) {
        double dv;
        String str = value.toString();
        try {
            dv = Double.parseDouble(str);
        } catch (NumberFormatException e) {
            if (str.equalsIgnoreCase("-inf")) {
                dv = Double.NEGATIVE_INFINITY;
            } else if (str.equalsIgnoreCase("inf")) {
                dv = Double.POSITIVE_INFINITY;
            } else {
                dv = Double.NaN;
            }
        }
        return new DoubleValue(dv, type);
    }

    public XDMValue createInteger(CharSequence value) {
        return new IntegerValue(new BigInteger(value.toString()));
    }

    public XDMValue createInteger(CharSequence value, AtomicType type) {
        return new IntegerValue(new BigInteger(value.toString()), type);
    }

    public XDMValue createDecimal(CharSequence value) {
        return new DecimalValue(new BigDecimal(value.toString()));
    }

    public XDMValue createDecimal(CharSequence value, AtomicType type) {
        return new DecimalValue(new BigDecimal(value.toString()), type);
    }

    public XDMValue createInt(CharSequence value) {
        return new IntValue(Long.parseLong(value.toString()));
    }

    public XDMValue createInt(CharSequence value, AtomicType type) {
        return new IntValue(Long.parseLong(value.toString()), type);
    }

    public XDMValue createFloat(CharSequence value) {
        return createFloat(value, BuiltinTypeRegistry.XS_FLOAT);
    }

    public XDMValue createFloat(CharSequence value, AtomicType type) {
        float fv;
        String str = value.toString();
        try {
            fv = Float.parseFloat(str);
        } catch (NumberFormatException e) {
            if (str.equalsIgnoreCase("-inf")) {
                fv = Float.NEGATIVE_INFINITY;
            } else if (str.equalsIgnoreCase("inf")) {
                fv = Float.POSITIVE_INFINITY;
            } else if (str.equalsIgnoreCase("nan")) {
                fv = Float.NaN;
            } else {
                throw e;
            }
        }
        return new FloatValue(fv, type);
    }

    public XDMValue createBoolean(CharSequence value) throws SystemException {
        String str = value.toString();
        if ("true".equalsIgnoreCase(str) || "1".equalsIgnoreCase(str)) {
            return BooleanValue.TRUE;
        } else if ("false".equalsIgnoreCase(str) || "0".equalsIgnoreCase(str)) {
            return BooleanValue.FALSE;
        }
        throw new SystemException(ErrorCode.FORG0001);
    }

    public XDMValue createGYearMonth(CharSequence value) throws SystemException {
        Matcher m = GYEARMONTH_PATTERN.matcher(value);
        if (!m.matches()) {
            throw new SystemException(ErrorCode.FORG0001);
        }
        String nStr = m.group(1);
        String yStr = m.group(2);
        String mStr = m.group(3);
        String zStr = m.group(5);
        String tzSgnStr = m.group(7);
        String tzHrStr = m.group(8);
        String tzMinStr = m.group(9);

        byte sign = (byte) (nStr == null || nStr.length() == 0 ? 1 : -1);
        int year = Integer.parseInt(yStr) * sign;
        int month = Integer.parseInt(mStr);
        Integer tz = zStr == null ? (tzSgnStr == null ? null : ((Integer.parseInt(tzHrStr) * 60 + Integer
                .parseInt(tzMinStr)) * ("-".equals(tzSgnStr) ? -1 : 1))) : 0;
        return new GYearMonthValue(year, month, tz);
    }

    public XDMValue createGMonthDay(CharSequence value) throws SystemException {
        Matcher m = GMONTHDAY_PATTERN.matcher(value);
        if (!m.matches()) {
            throw new SystemException(ErrorCode.FORG0001);
        }
        String mStr = m.group(1);
        String dStr = m.group(2);
        String zStr = m.group(4);
        String tzSgnStr = m.group(6);
        String tzHrStr = m.group(7);
        String tzMinStr = m.group(8);

        int month = Integer.parseInt(mStr);
        int day = Integer.parseInt(dStr);
        Integer tz = zStr == null ? (tzSgnStr == null ? null : ((Integer.parseInt(tzHrStr) * 60 + Integer
                .parseInt(tzMinStr)) * ("-".equals(tzSgnStr) ? -1 : 1))) : 0;
        return new GMonthDayValue(month, day, tz);
    }

    public XDMValue createGYear(CharSequence value) throws SystemException {
        Matcher m = GYEAR_PATTERN.matcher(value);
        if (!m.matches()) {
            throw new SystemException(ErrorCode.FORG0001);
        }
        String nStr = m.group(1);
        String yStr = m.group(2);
        String zStr = m.group(4);
        String tzSgnStr = m.group(6);
        String tzHrStr = m.group(7);
        String tzMinStr = m.group(8);

        byte sign = (byte) (nStr == null || nStr.length() == 0 ? 1 : -1);
        int year = Integer.parseInt(yStr) * sign;
        Integer tz = zStr == null ? (tzSgnStr == null ? null : ((Integer.parseInt(tzHrStr) * 60 + Integer
                .parseInt(tzMinStr)) * ("-".equals(tzSgnStr) ? -1 : 1))) : 0;
        return new GYearValue(year, tz);
    }

    public XDMValue createGMonth(CharSequence value) throws SystemException {
        Matcher m = GMONTH_PATTERN.matcher(value);
        if (!m.matches()) {
            throw new SystemException(ErrorCode.FORG0001);
        }
        String mStr = m.group(1);
        String zStr = m.group(3);
        String tzSgnStr = m.group(5);
        String tzHrStr = m.group(6);
        String tzMinStr = m.group(7);

        int month = Integer.parseInt(mStr);
        Integer tz = zStr == null ? (tzSgnStr == null ? null : ((Integer.parseInt(tzHrStr) * 60 + Integer
                .parseInt(tzMinStr)) * ("-".equals(tzSgnStr) ? -1 : 1))) : 0;
        return new GMonthValue(month, tz);
    }

    public XDMValue createGDay(CharSequence value) throws SystemException {
        Matcher m = GDAY_PATTERN.matcher(value);
        if (!m.matches()) {
            throw new SystemException(ErrorCode.FORG0001);
        }
        String dStr = m.group(1);
        String zStr = m.group(3);
        String tzSgnStr = m.group(5);
        String tzHrStr = m.group(6);
        String tzMinStr = m.group(7);

        int day = Integer.parseInt(dStr);
        Integer tz = zStr == null ? (tzSgnStr == null ? null : ((Integer.parseInt(tzHrStr) * 60 + Integer
                .parseInt(tzMinStr)) * ("-".equals(tzSgnStr) ? -1 : 1))) : 0;
        return new GDayValue(day, tz);
    }

    public XDMValue createBase64Binary(CharSequence str) {
        return new Base64BinaryValue(str);
    }

    public XDMValue createBase64Binary(byte[] bytes) {
        return new Base64BinaryValue(bytes);
    }

    public XDMValue createHexBinary(byte[] bytes) {
        return new HexBinaryValue(bytes);
    }

    public XDMValue createHexBinary(CharSequence str) throws SystemException {
        return new HexBinaryValue(str);
    }
}