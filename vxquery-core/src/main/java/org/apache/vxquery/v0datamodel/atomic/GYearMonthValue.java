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

import org.apache.vxquery.types.AtomicType;
import org.apache.vxquery.types.BuiltinTypeRegistry;

public class GYearMonthValue extends AtomicValue {
    private static final int[] DAYS_IN_MONTH = { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };

    private int yearMonth;
    private Integer tz;

    public GYearMonthValue(int year, int month, Integer tz) {
        this(year, month, tz, BuiltinTypeRegistry.XS_G_YEAR_MONTH);
    }

    public GYearMonthValue(int year, int month, Integer tz, AtomicType type) {
        super(type);
        yearMonth = year < 0 ? (year * 12 - month) : (year * 12 + month);
        this.tz = tz;
    }

    @Override
    public CharSequence getStringValue() {
        return String.valueOf(getYear() + "-" + getMonth());
    }

    public int getYear() {
        return yearMonth / 12;
    }

    public int getMonth() {
        return Math.abs(yearMonth) % 12;
    }

    public Integer getTimezone() {
        return tz;
    }

    public boolean equals(GYearMonthValue v2, int implicitTZ) {
        int s1 = yearMonth < 0 ? -1 : 1;
        int s2 = v2.yearMonth < 0 ? -1 : 1;
        int ym1 = yearMonth * s1;
        int ym2 = v2.yearMonth * s2;
        int d1 = getLastDayInMonth(getYear(), getMonth());
        int d2 = getLastDayInMonth(v2.getYear(), v2.getMonth());
        int tz1 = tz != null ? tz : implicitTZ;
        int tz2 = v2.tz != null ? v2.tz : implicitTZ;
        return (ym1 * 32 + d1) * 24 * 60 * s1 - tz1 == (ym2 * 32 + d2) * 24 * 60 * s2 - tz2;
    }

    private static int getLastDayInMonth(int y, int m) {
        return (m == 2 && y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 29 : DAYS_IN_MONTH[m];
    }
}