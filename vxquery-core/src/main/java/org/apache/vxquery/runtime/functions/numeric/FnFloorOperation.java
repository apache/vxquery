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
package org.apache.vxquery.runtime.functions.numeric;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.SystemException;

import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;

public class FnFloorOperation extends AbstractNumericOperation {

    @Override
    public void operateDecimal(XSDecimalPointable decp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_DECIMAL_TAG);
        dOut.write(0);
        if (decp.getDecimalValue() < 0 && decp.getDecimalPlace() > 0) {
            dOut.writeLong((long) (decp.getBeforeDecimalPlace()) - 1);
        } else {
            dOut.writeLong((long) (decp.getBeforeDecimalPlace()));
        }
    }

    @Override
    public void operateDouble(DoublePointable doublep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.writeDouble(Math.floor(doublep.getDouble()));
    }

    @Override
    public void operateFloat(FloatPointable floatp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_FLOAT_TAG);
        dOut.writeFloat((float) Math.floor(floatp.getFloat()));
    }

    @Override
    public void operateInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_INTEGER_TAG);
        dOut.writeLong(longp.getLong());
    }
}
