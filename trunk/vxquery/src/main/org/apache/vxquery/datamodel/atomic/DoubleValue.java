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

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.vxquery.types.AtomicType;
import org.apache.vxquery.types.BuiltinTypeRegistry;

public class DoubleValue extends NumericValue {
    DoubleValue(double value) {
        super(value, BuiltinTypeRegistry.XS_DOUBLE);
    }

    public DoubleValue(double value, AtomicType type) {
        super(value, type);
    }

    @Override
    public int sign() {
        double dVal = ((Double) value).doubleValue();
        return dVal < 0 ? -1 : (dVal > 0 ? 1 : 0);
    }

    @Override
    public NumericValue abs() {
        double dVal = ((Double) value).doubleValue();
        if (Double.isInfinite(dVal)) {
            return Double.NEGATIVE_INFINITY == dVal ? new DoubleValue(Double.POSITIVE_INFINITY) : this;
        }
        return super.abs();
    }

    @Override
    public NumericValue negate() {
        return new DoubleValue(-((Double) value).doubleValue());
    }

    @Override
    public NumericValue ceiling() {
        double dVal = ((Double) value).doubleValue();
        double cdVal = Math.ceil(dVal);
        return dVal == cdVal ? this : new DoubleValue(cdVal);
    }

    @Override
    public NumericValue floor() {
        double dVal = ((Double) value).doubleValue();
        double cdVal = Math.floor(dVal);
        return dVal == cdVal ? this : new DoubleValue(cdVal);
    }

    @Override
    public NumericValue round() {
        double dVal = ((Double) value).doubleValue();
        double cdVal = Math.round(dVal);
        return dVal == cdVal ? this : new DoubleValue(cdVal);
    }

    @Override
    public NumericValue roundHalfToEven() {
        double dVal = ((Double) value).doubleValue();
        double cdVal = Math.round(dVal);
        return dVal == cdVal ? this : new DoubleValue(cdVal);
    }

    @Override
    public BigInteger getIntegerValue() {
        return getDecimalValue().toBigInteger();
    }

    @Override
    public BigDecimal getDecimalValue() {
        return BigDecimal.valueOf((Double) value);
    }

    @Override
    public String toString() {
        return "[DOUBLE_VALUE " + value + "]";
    }
}