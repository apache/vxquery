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
package org.apache.vxquery.runtime.functions.comparison;

import java.io.IOException;

import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSBinaryPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDatePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDateTimePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDurationPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSQNamePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSTimePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.SystemException;

import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;

public abstract class AbstractNegatingComparisonOperation extends AbstractValueComparisonOperation {
    final AbstractValueComparisonOperation aOp = createBaseComparisonOperation();

    protected abstract AbstractValueComparisonOperation createBaseComparisonOperation();

    @Override
    public boolean operateAnyURIAnyURI(UTF8StringPointable stringp1, UTF8StringPointable stringp2)
            throws SystemException, IOException {
        return !aOp.operateAnyURIAnyURI(stringp1, stringp2);
    }

    @Override
    public boolean operateBase64BinaryBase64Binary(XSBinaryPointable binaryp1, XSBinaryPointable binaryp2)
            throws SystemException, IOException {
        return !aOp.operateBase64BinaryBase64Binary(binaryp1, binaryp2);
    }

    @Override
    public boolean operateBooleanBoolean(BooleanPointable boolp1, BooleanPointable boolp2)
            throws SystemException, IOException {
        return !aOp.operateBooleanBoolean(boolp1, boolp2);
    }

    @Override
    public boolean operateDateDate(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx)
            throws SystemException, IOException {
        return !aOp.operateDateDate(datep1, datep2, dCtx);
    }

    @Override
    public boolean operateDatetimeDatetime(XSDateTimePointable datetimep1, XSDateTimePointable datetimep2,
            DynamicContext dCtx) throws SystemException, IOException {
        return !aOp.operateDatetimeDatetime(datetimep1, datetimep2, dCtx);
    }

    @Override
    public boolean operateDecimalDecimal(XSDecimalPointable decp1, XSDecimalPointable decp2)
            throws SystemException, IOException {
        return !aOp.operateDecimalDecimal(decp1, decp2);
    }

    @Override
    public boolean operateDecimalDouble(XSDecimalPointable decp1, DoublePointable doublep2)
            throws SystemException, IOException {
        if (Double.isNaN(doublep2.getDouble())) {
            return false;
        }
        return !aOp.operateDecimalDouble(decp1, doublep2);
    }

    @Override
    public boolean operateDecimalFloat(XSDecimalPointable decp1, FloatPointable floatp2)
            throws SystemException, IOException {
        if (Float.isNaN(floatp2.getFloat())) {
            return false;
        }
        return !aOp.operateDecimalFloat(decp1, floatp2);
    }

    @Override
    public boolean operateDecimalInteger(XSDecimalPointable decp1, LongPointable longp2)
            throws SystemException, IOException {
        return !aOp.operateDecimalInteger(decp1, longp2);
    }

    @Override
    public boolean operateDoubleDecimal(DoublePointable doublep1, XSDecimalPointable decp2)
            throws SystemException, IOException {
        if (Double.isNaN(doublep1.getDouble())) {
            return false;
        }
        return !aOp.operateDoubleDecimal(doublep1, decp2);
    }

    @Override
    public boolean operateDoubleDouble(DoublePointable doublep1, DoublePointable doublep2)
            throws SystemException, IOException {
        if (Double.isNaN(doublep1.getDouble()) || Double.isNaN(doublep2.getDouble())) {
            return false;
        }
        return !aOp.operateDoubleDouble(doublep1, doublep2);
    }

    @Override
    public boolean operateDoubleFloat(DoublePointable doublep1, FloatPointable floatp2)
            throws SystemException, IOException {
        if (Double.isNaN(doublep1.getDouble()) || Float.isNaN(floatp2.getFloat())) {
            return false;
        }
        return !aOp.operateDoubleFloat(doublep1, floatp2);
    }

    @Override
    public boolean operateDoubleInteger(DoublePointable doublep1, LongPointable longp2)
            throws SystemException, IOException {
        if (Double.isNaN(doublep1.getDouble())) {
            return false;
        }
        return !aOp.operateDoubleInteger(doublep1, longp2);
    }

    @Override
    public boolean operateDTDurationDTDuration(LongPointable longp1, LongPointable longp2)
            throws SystemException, IOException {
        return !aOp.operateDTDurationDTDuration(longp1, longp2);
    }

    @Override
    public boolean operateDTDurationDuration(LongPointable longp1, XSDurationPointable durationp2)
            throws SystemException, IOException {
        return !aOp.operateDTDurationDuration(longp1, durationp2);
    }

    @Override
    public boolean operateDTDurationYMDuration(LongPointable longp1, IntegerPointable intp2)
            throws SystemException, IOException {
        return !aOp.operateDTDurationYMDuration(longp1, intp2);
    }

    @Override
    public boolean operateDurationDTDuration(XSDurationPointable durationp1, LongPointable longp2)
            throws SystemException, IOException {
        return !aOp.operateDurationDTDuration(durationp1, longp2);
    }

    @Override
    public boolean operateDurationDuration(XSDurationPointable durationp1, XSDurationPointable durationp2)
            throws SystemException, IOException {
        return !aOp.operateDurationDuration(durationp1, durationp2);
    }

    @Override
    public boolean operateDurationYMDuration(XSDurationPointable durationp1, IntegerPointable intp2)
            throws SystemException, IOException {
        return !aOp.operateDurationYMDuration(durationp1, intp2);
    }

    @Override
    public boolean operateFloatDecimal(FloatPointable floatp1, XSDecimalPointable decp2)
            throws SystemException, IOException {
        if (Float.isNaN(floatp1.getFloat())) {
            return false;
        }
        return !aOp.operateFloatDecimal(floatp1, decp2);
    }

    @Override
    public boolean operateFloatDouble(FloatPointable floatp1, DoublePointable doublep2)
            throws SystemException, IOException {
        if (Float.isNaN(floatp1.getFloat()) || Double.isNaN(doublep2.doubleValue())) {
            return false;
        }
        return !aOp.operateFloatDouble(floatp1, doublep2);
    }

    @Override
    public boolean operateFloatFloat(FloatPointable floatp1, FloatPointable floatp2)
            throws SystemException, IOException {
        if (Float.isNaN(floatp1.getFloat()) || Float.isNaN(floatp2.getFloat())) {
            return false;
        }
        return !aOp.operateFloatFloat(floatp1, floatp2);
    }

    @Override
    public boolean operateFloatInteger(FloatPointable floatp1, LongPointable longp2)
            throws SystemException, IOException {
        if (Float.isNaN(floatp1.getFloat())) {
            return false;
        }
        return !aOp.operateFloatInteger(floatp1, longp2);
    }

    @Override
    public boolean operateGDayGDay(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx)
            throws SystemException, IOException {
        return !aOp.operateGDayGDay(datep1, datep2, dCtx);
    }

    @Override
    public boolean operateGMonthDayGMonthDay(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx)
            throws SystemException, IOException {
        return !aOp.operateGMonthDayGMonthDay(datep1, datep2, dCtx);
    }

    @Override
    public boolean operateGMonthGMonth(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx)
            throws SystemException, IOException {
        return !aOp.operateGMonthGMonth(datep1, datep2, dCtx);
    }

    @Override
    public boolean operateGYearGYear(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx)
            throws SystemException, IOException {
        return !aOp.operateGYearGYear(datep1, datep2, dCtx);
    }

    @Override
    public boolean operateGYearMonthGYearMonth(XSDatePointable datep1, XSDatePointable datep2, DynamicContext dCtx)
            throws SystemException, IOException {
        return !aOp.operateGYearMonthGYearMonth(datep1, datep2, dCtx);
    }

    @Override
    public boolean operateHexBinaryHexBinary(XSBinaryPointable binaryp1, XSBinaryPointable binaryp2)
            throws SystemException, IOException {
        return !aOp.operateHexBinaryHexBinary(binaryp1, binaryp2);
    }

    @Override
    public boolean operateIntegerDecimal(LongPointable longp1, XSDecimalPointable decp2)
            throws SystemException, IOException {
        return !aOp.operateIntegerDecimal(longp1, decp2);
    }

    @Override
    public boolean operateIntegerDouble(LongPointable longp1, DoublePointable doublep2)
            throws SystemException, IOException {
        if (Double.isNaN(doublep2.doubleValue())) {
            return false;
        }
        return !aOp.operateIntegerDouble(longp1, doublep2);
    }

    @Override
    public boolean operateIntegerFloat(LongPointable longp1, FloatPointable floatp2)
            throws SystemException, IOException {
        if (Float.isNaN(floatp2.floatValue())) {
            return false;
        }
        return !aOp.operateIntegerFloat(longp1, floatp2);
    }

    @Override
    public boolean operateIntegerInteger(LongPointable longp1, LongPointable longp2)
            throws SystemException, IOException {
        return !aOp.operateIntegerInteger(longp1, longp2);
    }

    @Override
    public boolean operateNotationNotation(UTF8StringPointable stringp1, UTF8StringPointable stringp2)
            throws SystemException, IOException {
        return !aOp.operateNotationNotation(stringp1, stringp2);
    }

    @Override
    public boolean operateQNameQName(XSQNamePointable qnamep1, XSQNamePointable qnamep2)
            throws SystemException, IOException {
        return !aOp.operateQNameQName(qnamep1, qnamep2);
    }

    @Override
    public boolean operateStringString(UTF8StringPointable stringp1, UTF8StringPointable stringp2)
            throws SystemException, IOException {
        return !aOp.operateStringString(stringp1, stringp2);
    }

    @Override
    public boolean operateTimeTime(XSTimePointable timep1, XSTimePointable timep2, DynamicContext dCtx)
            throws SystemException, IOException {
        return !aOp.operateTimeTime(timep1, timep2, dCtx);
    }

    @Override
    public boolean operateYMDurationDTDuration(IntegerPointable intp1, LongPointable longp2)
            throws SystemException, IOException {
        return !aOp.operateYMDurationDTDuration(intp1, longp2);
    }

    @Override
    public boolean operateYMDurationDuration(IntegerPointable intp1, XSDurationPointable durationp2)
            throws SystemException, IOException {
        return !aOp.operateYMDurationDuration(intp1, durationp2);
    }

    @Override
    public boolean operateYMDurationYMDuration(IntegerPointable intp1, IntegerPointable intp2)
            throws SystemException, IOException {
        return !aOp.operateYMDurationYMDuration(intp1, intp2);
    }

    @Override
    public boolean operateNull(TaggedValuePointable tvp1, TaggedValuePointable tvp2)
            throws SystemException, IOException {
        return !aOp.operateNull(tvp1, tvp2);
    }

}
