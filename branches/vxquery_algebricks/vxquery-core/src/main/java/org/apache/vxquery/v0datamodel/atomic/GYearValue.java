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

public class GYearValue extends AtomicValue {
    private int year;
    private Integer tz;

    public GYearValue(int year, Integer tz) {
        this(year, tz, BuiltinTypeRegistry.XS_G_YEAR);
    }

    public GYearValue(int year, Integer tz, AtomicType type) {
        super(type);
        this.year = year;
        this.tz = tz;
    }

    @Override
    public CharSequence getStringValue() {
        return String.valueOf(year);
    }

    public int getYear() {
        return year;
    }

    public Integer getTimezone() {
        return tz;
    }

    public boolean equals(GYearValue v2, int implicitTZ) {
        int s1 = year < 0 ? -1 : 1;
        int s2 = v2.year < 0 ? -1 : 1;
        int y1 = year * s1;
        int y2 = v2.year * s2;
        int tz1 = tz != null ? tz : implicitTZ;
        int tz2 = v2.tz != null ? v2.tz : implicitTZ;
        return ((y1 * 12 + 1) * 32 + 1) * 24 * 60 * s1 - tz1 == ((y2 * 12 + 1) * 32 + 1) * 24 * 60 * s2 - tz2;
    }
}