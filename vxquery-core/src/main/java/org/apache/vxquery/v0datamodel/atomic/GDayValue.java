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

public class GDayValue extends AtomicValue {
    private int day;
    private Integer tz;

    public GDayValue(int day, Integer tz) {
        this(day, tz, BuiltinTypeRegistry.XS_G_DAY);
    }

    public GDayValue(int day, Integer tz, AtomicType type) {
        super(type);
        this.day = day;
        this.tz = tz;
    }

    @Override
    public CharSequence getStringValue() {
        return String.valueOf(day);
    }

    public int getDay() {
        return day;
    }

    public Integer getTimezone() {
        return tz;
    }

    public boolean equals(GDayValue v2, int implicitTZ) {
        int d1 = day;
        int d2 = v2.day;
        int tz1 = tz != null ? tz : implicitTZ;
        int tz2 = v2.tz != null ? v2.tz : implicitTZ;
        return d1 * 24 * 60 - tz1 == d2 * 24 * 60 - tz2;
    }
}