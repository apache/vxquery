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

import org.apache.vxquery.types.AtomicType;
import org.apache.vxquery.types.BuiltinTypeRegistry;

public class GMonthValue extends AtomicValue {
    private static final int[] DAYS_IN_MONTH = { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    private int month;
    private Integer tz;

    public GMonthValue(int month, Integer tz) {
        this(month, tz, BuiltinTypeRegistry.XS_G_MONTH);
    }

    public GMonthValue(int month, Integer tz, AtomicType type) {
        super(type);
        this.month = month;
        this.tz = tz;
    }

    @Override
    public CharSequence getStringValue() {
        return String.valueOf(month);
    }

    public int getMonth() {
        return month;
    }

    public Integer getTimezone() {
        return tz;
    }

    public boolean equals(GMonthValue v2, int implicitTZ) {
        int m1 = month;
        int m2 = v2.month;
        int tz1 = tz != null ? tz : implicitTZ;
        int tz2 = v2.tz != null ? v2.tz : implicitTZ;
        return ((m1 - 1) * 32 + DAYS_IN_MONTH[m1 - 1]) * 24 * 60 - tz1 == ((m2 - 1) * 32 + DAYS_IN_MONTH[m2 - 1]) * 24
                * 60 - tz2;
    }
}