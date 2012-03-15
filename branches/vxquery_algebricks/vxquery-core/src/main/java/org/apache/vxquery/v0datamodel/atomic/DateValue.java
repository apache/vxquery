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
package org.apache.vxquery.v0datamodel.atomic;

import java.math.BigInteger;

import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.vxquery.types.AtomicType;
import org.apache.vxquery.types.BuiltinTypeRegistry;

public class DateValue extends AbstractDateTimeValue {
    DateValue(XMLGregorianCalendar calendar) {
        this(calendar, BuiltinTypeRegistry.XS_DATE);
    }

    DateValue(XMLGregorianCalendar calendar, AtomicType type) {
        super(type, calendar);
    }

    public BigInteger getYearValue() {
        return calendar.getEonAndYear();
    }

    public int getMonthValue() {
        return calendar.getMonth();
    }

    public int getDayValue() {
        return calendar.getDay();
    }

    public int getTimezoneValue() {
        return calendar.getTimezone();
    }

    @Override
    public String toString() {
        return "[DATE_VALUE " + calendar + "]";
    }
}