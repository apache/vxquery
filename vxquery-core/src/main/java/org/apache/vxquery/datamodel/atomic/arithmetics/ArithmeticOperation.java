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
package org.apache.vxquery.datamodel.atomic.arithmetics;

import org.apache.vxquery.datamodel.XDMItem;
import org.apache.vxquery.datamodel.atomic.AtomicValueFactory;
import org.apache.vxquery.datamodel.atomic.DateTimeValue;
import org.apache.vxquery.datamodel.atomic.DateValue;
import org.apache.vxquery.datamodel.atomic.DurationValue;
import org.apache.vxquery.datamodel.atomic.NumericValue;
import org.apache.vxquery.datamodel.atomic.TimeValue;
import org.apache.vxquery.exceptions.SystemException;

public interface ArithmeticOperation {
    XDMItem operateDecimal(AtomicValueFactory avf, NumericValue v1, NumericValue v2) throws SystemException;

    XDMItem operateFloat(AtomicValueFactory avf, NumericValue v1, NumericValue v2) throws SystemException;

    XDMItem operateDouble(AtomicValueFactory avf, NumericValue v1, NumericValue v2) throws SystemException;

    XDMItem operateInteger(AtomicValueFactory avf, NumericValue v1, NumericValue v2) throws SystemException;

    XDMItem operateDateDayTimeDuration(AtomicValueFactory avf, DateValue v1, DurationValue v2) throws SystemException;

    XDMItem operateDateYearMonthDuration(AtomicValueFactory avf, DateValue v1, DurationValue v2)
            throws SystemException;

    XDMItem operateDateTimeDayTimeDuration(AtomicValueFactory avf, DateTimeValue v1, DurationValue v2)
            throws SystemException;

    XDMItem operateDateTimeYearMonthDuration(AtomicValueFactory avf, DateTimeValue v1, DurationValue v2)
            throws SystemException;

    XDMItem operateTimeDayTimeDuration(AtomicValueFactory avf, TimeValue v1, DurationValue v2) throws SystemException;

    XDMItem operateTimeYearMonthDuration(AtomicValueFactory avf, TimeValue v1, DurationValue v2)
            throws SystemException;

    XDMItem operateDayTimeDurationDate(AtomicValueFactory avf, DurationValue v1, DateValue v2) throws SystemException;

    XDMItem operateDayTimeDurationTime(AtomicValueFactory avf, DurationValue v1, TimeValue v2) throws SystemException;

    XDMItem operateDayTimeDurationDateTime(AtomicValueFactory avf, DurationValue v1, DateTimeValue v2)
            throws SystemException;

    XDMItem operateDayTimeDuration(AtomicValueFactory avf, DurationValue v1, DurationValue v2) throws SystemException;

    XDMItem operateYearMonthDurationDate(AtomicValueFactory avf, DurationValue v1, DateValue v2)
            throws SystemException;

    XDMItem operateYearMonthDurationTime(AtomicValueFactory avf, DurationValue v1, TimeValue v2)
            throws SystemException;

    XDMItem operateYearMonthDurationDateTime(AtomicValueFactory avf, DurationValue v1, DateTimeValue v2)
            throws SystemException;

    XDMItem operateYearMonthDuration(AtomicValueFactory avf, DurationValue v1, DurationValue v2)
            throws SystemException;

    XDMItem operateDate(AtomicValueFactory avf, DateValue v1, DateValue v2) throws SystemException;

    XDMItem operateTime(AtomicValueFactory avf, TimeValue v1, TimeValue v2) throws SystemException;

    XDMItem operateDateTime(AtomicValueFactory avf, DateTimeValue v1, DateTimeValue v2) throws SystemException;

    XDMItem operateDayTimeDurationNumeric(AtomicValueFactory avf, DurationValue v1, NumericValue v2)
            throws SystemException;

    XDMItem operateNumericDayTimeDuration(AtomicValueFactory avf, NumericValue v1, DurationValue v2)
            throws SystemException;

    XDMItem operateYearMonthDurationNumeric(AtomicValueFactory avf, DurationValue v1, NumericValue v2)
            throws SystemException;

    XDMItem operateNumericYearMonthDuration(AtomicValueFactory avf, NumericValue v1, DurationValue v2)
            throws SystemException;
}