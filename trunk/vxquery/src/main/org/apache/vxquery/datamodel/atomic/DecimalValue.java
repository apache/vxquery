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
import java.math.RoundingMode;

import org.apache.vxquery.types.AtomicType;
import org.apache.vxquery.types.BuiltinTypeRegistry;

public class DecimalValue extends NumericValue {
    DecimalValue(BigDecimal value) {
        super(value, BuiltinTypeRegistry.XS_INTEGER);
    }

    public DecimalValue(BigDecimal value, AtomicType type) {
        super(value, type);
    }

    @Override
    public int sign() {
        return ((BigDecimal) value).signum();
    }

    @Override
    public NumericValue negate() {
        return new DecimalValue(((BigDecimal) value).negate());
    }

    @Override
    public NumericValue ceiling() {
        BigDecimal dVal = (BigDecimal) value;
        return dVal.scale() == 0 ? this : new DecimalValue(dVal.setScale(0, RoundingMode.CEILING));
    }

    @Override
    public NumericValue floor() {
        BigDecimal dVal = (BigDecimal) value;
        return dVal.scale() == 0 ? this : new DecimalValue(dVal.setScale(0, RoundingMode.FLOOR));
    }

    @Override
    public NumericValue round() {
        BigDecimal dVal = (BigDecimal) value;
        return dVal.scale() == 0 ? this : new DecimalValue(dVal.setScale(0, RoundingMode.HALF_UP));
    }

    @Override
    public NumericValue roundHalfToEven() {
        BigDecimal dVal = (BigDecimal) value;
        return dVal.scale() == 0 ? this : new DecimalValue(dVal.setScale(0, RoundingMode.HALF_EVEN));
    }

    @Override
    public BigInteger getIntegerValue() {
        return ((BigDecimal) value).toBigInteger();
    }

    @Override
    public BigDecimal getDecimalValue() {
        return (BigDecimal) value;
    }

    @Override
    public String toString() {
        return "[DECIMAL_VALUE " + value + "]";
    }
}