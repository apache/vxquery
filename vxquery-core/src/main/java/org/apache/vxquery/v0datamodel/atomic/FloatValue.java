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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

import org.apache.vxquery.types.AtomicType;
import org.apache.vxquery.types.BuiltinTypeRegistry;

public class FloatValue extends NumericValue {
    
    private static final float LOWER = 0.000001f;
    private static final float UPPER = 1000000f;
    
    FloatValue(float value) {
        super(value, BuiltinTypeRegistry.XS_FLOAT);
    }

    public FloatValue(float value, AtomicType type) {
        super(value, type);
    }

    @Override
    public CharSequence getStringValue() {
        float f = (Float)value;
        float af = Math.abs(f);
        if (LOWER <= af && af <= UPPER) {
            // TODO is there a more efficient way?
            return DecimalValue.stringValue(BigDecimal.valueOf(f));
        }
        if (af == 0.0) {
            return f == 0.0 ? "0" : "-0";
        }
        if (f == Float.NEGATIVE_INFINITY) {
            return "-INF";
        }
        if (f == Float.POSITIVE_INFINITY) {
            return "INF";
        }
        return String.valueOf(value);
    }

    @Override
    public int sign() {
        float fVal = ((Float) value).floatValue();
        return fVal < 0 ? -1 : (fVal > 0 ? 1 : 0);
    }

    @Override
    public NumericValue abs() {
        float fVal = ((Float) value).floatValue();
        if (Float.isInfinite(fVal)) {
            return Float.NEGATIVE_INFINITY == fVal ? new FloatValue(Float.POSITIVE_INFINITY) : this;
        }
        return super.abs();
    }

    @Override
    public NumericValue negate() {
        return new FloatValue(-((Float) value).floatValue());
    }

    @Override
    public NumericValue ceiling() {
        float fVal = ((Float) value).floatValue();
        float cfVal = (float) Math.ceil(fVal);
        return fVal == cfVal ? this : new FloatValue(cfVal);
    }

    @Override
    public NumericValue floor() {
        float fVal = ((Float) value).floatValue();
        float cfVal = (float) Math.floor(fVal);
        return fVal == cfVal ? this : new FloatValue(cfVal);
    }

    @Override
    public NumericValue round() {
        float fVal = ((Float) value).floatValue();
        float cfVal = Math.round(fVal);
        return fVal == cfVal ? this : new FloatValue(cfVal);
    }

    @Override
    public NumericValue roundHalfToEven(IntegerValue precision) {
        float fVal = ((Float) value).floatValue();
        BigDecimal decVal = BigDecimal.valueOf(fVal);
        int scale = (int) precision.getIntValue();
        return decVal.scale() <= scale ? this 
                : new FloatValue(decVal.setScale(scale, RoundingMode.HALF_EVEN).floatValue());
    }

    @Override
    public BigInteger getIntegerValue() {
        return getDecimalValue().toBigInteger();
    }

    @Override
    public BigDecimal getDecimalValue() {
        return BigDecimal.valueOf((Float) value);
    }

    @Override
    public String toString() {
        return "[FLOAT_VALUE " + value + "]";
    }
}