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
package org.apache.vxquery.runtime.functions.castable;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.atomic.XSDatePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDateTimePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.cast.CastToGMonthDayOperation;

import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class CastableAsGMonthDayOperation extends AbstractCastableAsOperation {
    private ArrayBackedValueStorage abvsInner = new ArrayBackedValueStorage();
    private DataOutput dOutInner = abvsInner.getDataOutput();

    @Override
    public void convertDate(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 1);
    }

    @Override
    public void convertDatetime(XSDateTimePointable datetimep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 1);
    }

    @Override
    public void convertGMonthDay(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 1);
    }

    @Override
    public void convertString(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        boolean castable = true;
        try {
            abvsInner.reset();
            CastToGMonthDayOperation castTo = new CastToGMonthDayOperation();
            castTo.convertString(stringp, dOutInner);
        } catch (Exception e) {
            castable = false;
        }
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) (castable ? 1 : 0));
    }

    @Override
    public void convertUntypedAtomic(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        convertString(stringp, dOut);
    }

}
