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

public class IntValue extends NumericValue {
    public static final IntValue ZERO = new IntValue(0);

    IntValue(long value) {
        super(value, BuiltinTypeRegistry.XS_INT);
    }

    public IntValue(long value, AtomicType type) {
        super(value, type);
    }

    @Override
    public int sign() {
        long lVal = ((Long) value).longValue();
        return lVal < 0 ? -1 : (lVal > 0 ? 1 : 0);
    }

    @Override
    public NumericValue negate() {
        return new IntValue(-((Long) value).longValue());
    }

    @Override
    public NumericValue ceiling() {
        return this;
    }

    @Override
    public NumericValue floor() {
        return this;
    }

    @Override
    public NumericValue round() {
        return this;
    }

    @Override
    public NumericValue roundHalfToEven() {
        return this;
    }

    @Override
    public BigInteger getIntegerValue() {
        return new BigInteger(value.toString());
    }

    @Override
    public BigDecimal getDecimalValue() {
        return BigDecimal.valueOf((Long) value);
    }

    @Override
    public String toString() {
        return "[INT_VALUE " + value + "]";
    }
}