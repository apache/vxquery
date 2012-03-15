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
package org.apache.vxquery.v0datamodel.atomic.compare;

import org.apache.vxquery.collations.Collation;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.v0datamodel.atomic.Base64BinaryValue;
import org.apache.vxquery.v0datamodel.atomic.BooleanValue;
import org.apache.vxquery.v0datamodel.atomic.DateTimeValue;
import org.apache.vxquery.v0datamodel.atomic.DateValue;
import org.apache.vxquery.v0datamodel.atomic.DurationValue;
import org.apache.vxquery.v0datamodel.atomic.GDayValue;
import org.apache.vxquery.v0datamodel.atomic.GMonthDayValue;
import org.apache.vxquery.v0datamodel.atomic.GMonthValue;
import org.apache.vxquery.v0datamodel.atomic.GYearMonthValue;
import org.apache.vxquery.v0datamodel.atomic.GYearValue;
import org.apache.vxquery.v0datamodel.atomic.HexBinaryValue;
import org.apache.vxquery.v0datamodel.atomic.NumericValue;
import org.apache.vxquery.v0datamodel.atomic.QNameValue;
import org.apache.vxquery.v0datamodel.atomic.StringValue;
import org.apache.vxquery.v0datamodel.atomic.TimeValue;

public final class ValueLeComparator implements ValueComparator {
    public static final ValueComparator INSTANCE = new ValueLeComparator();

    private ValueLeComparator() {
    }

    @Override
    public boolean compareBoolean(BooleanValue v1, BooleanValue v2) throws SystemException {
        return !ValueGtComparator.INSTANCE.compareBoolean(v1, v2);
    }

    @Override
    public boolean compareDate(DateValue v1, DateValue v2, int implicitTZ) throws SystemException {
        return !ValueGtComparator.INSTANCE.compareDate(v1, v2, implicitTZ);
    }

    @Override
    public boolean compareDateTime(DateTimeValue v1, DateTimeValue v2, int implicitTZ) throws SystemException {
        return !ValueGtComparator.INSTANCE.compareDateTime(v1, v2, implicitTZ);
    }

    @Override
    public boolean compareDecimal(NumericValue v1, NumericValue v2) throws SystemException {
        return !ValueGtComparator.INSTANCE.compareDecimal(v1, v2);
    }

    @Override
    public boolean compareDouble(NumericValue v1, NumericValue v2) throws SystemException {
        return !ValueGtComparator.INSTANCE.compareDouble(v1, v2);
    }

    @Override
    public boolean compareDuration(DurationValue v1, DurationValue v2) throws SystemException {
        return !ValueGtComparator.INSTANCE.compareDuration(v1, v2);
    }

    @Override
    public boolean compareFloat(NumericValue v1, NumericValue v2) throws SystemException {
        return !ValueGtComparator.INSTANCE.compareFloat(v1, v2);
    }

    @Override
    public boolean compareInteger(NumericValue v1, NumericValue v2) throws SystemException {
        return !ValueGtComparator.INSTANCE.compareInteger(v1, v2);
    }

    @Override
    public boolean compareQName(QNameValue v1, QNameValue v2) throws SystemException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public boolean compareString(StringValue v1, StringValue v2, Collation collation) throws SystemException {
        return !ValueGtComparator.INSTANCE.compareString(v1, v2, collation);
    }

    @Override
    public boolean compareTime(TimeValue v1, TimeValue v2, int implicitTZ) throws SystemException {
        return v1.compare(v2, implicitTZ) > 0;
    }

    @Override
    public boolean compareBase64Binary(Base64BinaryValue v1, Base64BinaryValue v2) throws SystemException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean compareGDay(GDayValue v1, GDayValue v2, int implicitTZ) throws SystemException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean compareGMonth(GMonthValue v1, GMonthValue v2, int implicitTZ) throws SystemException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean compareGMonthDay(GMonthDayValue v1, GMonthDayValue v2, int implicitTZ) throws SystemException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean compareGYear(GYearValue v1, GYearValue v2, int implicitTZ) throws SystemException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean compareGYearMonth(GYearMonthValue v1, GYearMonthValue v2, int implicitTZ) throws SystemException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean compareHexBinary(HexBinaryValue v1, HexBinaryValue v2) throws SystemException {
        throw new UnsupportedOperationException();
    }
}