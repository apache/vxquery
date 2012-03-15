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
package org.apache.vxquery.v0datamodel.atomic.arithmetics;

import java.math.BigDecimal;

import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.v0datamodel.XDMItem;
import org.apache.vxquery.v0datamodel.atomic.AtomicValueFactory;
import org.apache.vxquery.v0datamodel.atomic.DateTimeValue;
import org.apache.vxquery.v0datamodel.atomic.DateValue;
import org.apache.vxquery.v0datamodel.atomic.DurationValue;
import org.apache.vxquery.v0datamodel.atomic.NumericValue;
import org.apache.vxquery.v0datamodel.atomic.TimeValue;

public class MultiplyArithmeticOperation implements ArithmeticOperation {
    public static final ArithmeticOperation INSTANCE = new MultiplyArithmeticOperation();

    private MultiplyArithmeticOperation() {
    }

    @Override
    public XDMItem operateDate(AtomicValueFactory avf, DateValue v1, DateValue v2) throws SystemException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public XDMItem operateDateDayTimeDuration(AtomicValueFactory avf, DateValue v1, DurationValue v2)
            throws SystemException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public XDMItem operateDateTime(AtomicValueFactory avf, DateTimeValue v1, DateTimeValue v2) throws SystemException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public XDMItem operateDateTimeDayTimeDuration(AtomicValueFactory avf, DateTimeValue v1, DurationValue v2)
            throws SystemException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public XDMItem operateDateTimeYearMonthDuration(AtomicValueFactory avf, DateTimeValue v1, DurationValue v2)
            throws SystemException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public XDMItem operateDateYearMonthDuration(AtomicValueFactory avf, DateValue v1, DurationValue v2)
            throws SystemException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public XDMItem operateDayTimeDuration(AtomicValueFactory avf, DurationValue v1, DurationValue v2)
            throws SystemException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public XDMItem operateDayTimeDurationDate(AtomicValueFactory avf, DurationValue v1, DateValue v2)
            throws SystemException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public XDMItem operateDayTimeDurationDateTime(AtomicValueFactory avf, DurationValue v1, DateTimeValue v2)
            throws SystemException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public XDMItem operateDayTimeDurationTime(AtomicValueFactory avf, DurationValue v1, TimeValue v2)
            throws SystemException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public XDMItem operateDecimal(AtomicValueFactory avf, NumericValue v1, NumericValue v2) throws SystemException {
        return avf.createDecimal(v1.getDecimalValue().multiply(v2.getDecimalValue()));
    }

    @Override
    public XDMItem operateDouble(AtomicValueFactory avf, NumericValue v1, NumericValue v2) throws SystemException {
        return avf.createDouble(v1.getDoubleValue() * v2.getDoubleValue());
    }

    @Override
    public XDMItem operateFloat(AtomicValueFactory avf, NumericValue v1, NumericValue v2) throws SystemException {
        return avf.createFloat(v1.getFloatValue() * v2.getFloatValue());
    }

    @Override
    public XDMItem operateInteger(AtomicValueFactory avf, NumericValue v1, NumericValue v2) throws SystemException {
        return avf.createInteger(v1.getIntegerValue().multiply(v2.getIntegerValue()));
    }

    @Override
    public XDMItem operateTime(AtomicValueFactory avf, TimeValue v1, TimeValue v2) throws SystemException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public XDMItem operateTimeDayTimeDuration(AtomicValueFactory avf, TimeValue v1, DurationValue v2)
            throws SystemException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public XDMItem operateTimeYearMonthDuration(AtomicValueFactory avf, TimeValue v1, DurationValue v2)
            throws SystemException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public XDMItem operateYearMonthDuration(AtomicValueFactory avf, DurationValue v1, DurationValue v2)
            throws SystemException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public XDMItem operateYearMonthDurationDate(AtomicValueFactory avf, DurationValue v1, DateValue v2)
            throws SystemException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public XDMItem operateYearMonthDurationDateTime(AtomicValueFactory avf, DurationValue v1, DateTimeValue v2)
            throws SystemException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public XDMItem operateYearMonthDurationTime(AtomicValueFactory avf, DurationValue v1, TimeValue v2)
            throws SystemException {
        throw new SystemException(ErrorCode.XPTY0004);
    }

    @Override
    public XDMItem operateDayTimeDurationNumeric(AtomicValueFactory avf, DurationValue v1, NumericValue v2)
            throws SystemException {
        return avf.createDuration(v1.getInternalDuration().multiply(BigDecimal.valueOf(v2.getDoubleValue())));
    }

    @Override
    public XDMItem operateNumericDayTimeDuration(AtomicValueFactory avf, NumericValue v1, DurationValue v2)
            throws SystemException {
        return operateDayTimeDurationNumeric(avf, v2, v1);
    }

    @Override
    public XDMItem operateNumericYearMonthDuration(AtomicValueFactory avf, NumericValue v1, DurationValue v2)
            throws SystemException {
        return operateYearMonthDurationNumeric(avf, v2, v1);
    }

    @Override
    public XDMItem operateYearMonthDurationNumeric(AtomicValueFactory avf, DurationValue v1, NumericValue v2)
            throws SystemException {
        return avf.createDuration(v1.getInternalDuration().multiply(BigDecimal.valueOf(v2.getDoubleValue())));
    }
}