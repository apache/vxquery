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

import javax.xml.datatype.Duration;

import org.apache.vxquery.types.AtomicType;
import org.apache.vxquery.types.BuiltinTypeRegistry;

public class DurationValue extends AtomicValue {
    private Duration duration;

    DurationValue(Duration duration) {
        this(duration, BuiltinTypeRegistry.XS_DURATION);
    }

    DurationValue(Duration duration, AtomicType type) {
        super(type);
        this.duration = duration;
    }

    @Override
    public CharSequence getStringValue() {
        return duration.toString();
    }

    public int getSign() {
        return duration.getSign();
    }

    public int getYearValue() {
        return duration.getYears();
    }

    public int getMonthValue() {
        return duration.getMonths();
    }

    public int getDayValue() {
        return duration.getDays();
    }

    public int getHourValue() {
        return duration.getHours();
    }

    public int getMinuteValue() {
        return duration.getMinutes();
    }

    public int getSecondValue() {
        return duration.getSeconds();
    }

    public int compare(DurationValue otherDuration) {
        return duration.compare(otherDuration.duration);
    }

    public Duration getInternalDuration() {
        return duration;
    }

    @Override
    public String toString() {
        return "[DURATION_VALUE " + duration + "]";
    }
}