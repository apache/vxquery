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

import javax.xml.datatype.DatatypeConstants;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.vxquery.types.AtomicType;

public abstract class AbstractDateTimeValue extends AtomicValue {
    protected final XMLGregorianCalendar calendar;

    public AbstractDateTimeValue(AtomicType type, XMLGregorianCalendar calendar) {
        super(type);
        this.calendar = calendar;
    }

    @Override
    public final CharSequence getStringValue() {
        return calendar.toXMLFormat();
    }

    public final boolean hasTimezone() {
        return calendar.getTimezone() != DatatypeConstants.FIELD_UNDEFINED;
    }

    public final int compare(AbstractDateTimeValue otherDateTime, int implicitTZ) {
        if (hasTimezone()) {
            if (otherDateTime.hasTimezone()) {
                return calendar.compare(otherDateTime.calendar);
            } else {
                XMLGregorianCalendar oc = (XMLGregorianCalendar) otherDateTime.calendar.clone();
                oc.setTimezone(implicitTZ);
                return calendar.compare(oc);
            }
        } else {
            if (otherDateTime.hasTimezone()) {
                XMLGregorianCalendar c = (XMLGregorianCalendar) calendar.clone();
                c.setTimezone(implicitTZ);
                return c.compare(otherDateTime.calendar);
            } else {
                return calendar.compare(otherDateTime.calendar);
            }
        }
    }

    public final XMLGregorianCalendar getInternalCalendar() {
        return calendar;
    }
}