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
package org.apache.vxquery.v0runtime.functions;

import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.types.AtomicType;
import org.apache.vxquery.types.BuiltinTypeConstants;
import org.apache.vxquery.types.BuiltinTypeRegistry;
import org.apache.vxquery.v0datamodel.XDMAtomicValue;
import org.apache.vxquery.v0datamodel.atomic.AtomicValueFactory;
import org.apache.vxquery.v0datamodel.atomic.DateTimeValue;
import org.apache.vxquery.v0datamodel.atomic.DateValue;
import org.apache.vxquery.v0datamodel.atomic.DurationValue;
import org.apache.vxquery.v0datamodel.atomic.NumericValue;
import org.apache.vxquery.v0datamodel.atomic.TimeValue;
import org.apache.vxquery.v0datamodel.atomic.arithmetics.ArithmeticOperation;
import org.apache.vxquery.v0runtime.CallStackFrame;
import org.apache.vxquery.v0runtime.RegisterAllocator;
import org.apache.vxquery.v0runtime.base.AbstractEagerlyEvaluatedFunctionIterator;
import org.apache.vxquery.v0runtime.base.RuntimeIterator;

public abstract class AbstractArithmeticOperationIterator extends AbstractEagerlyEvaluatedFunctionIterator {
    public AbstractArithmeticOperationIterator(RegisterAllocator allocator, Function fn, RuntimeIterator[] arguments,
            StaticContext ctx) {
        super(allocator, fn, arguments, ctx);
    }

    @Override
    public Object evaluateEagerly(CallStackFrame frame) throws SystemException {
        XDMAtomicValue v1 = (XDMAtomicValue) arguments[0].evaluateEagerly(frame);
        if (v1 == null) {
            return null;
        }
        XDMAtomicValue v2 = (XDMAtomicValue) arguments[1].evaluateEagerly(frame);
        if (v2 == null) {
            return null;
        }
        AtomicType t1 = v1.getAtomicType();
        AtomicType t2 = v2.getAtomicType();
        int tid1 = getBaseTypeForArithmetics(t1.getTypeId());
        int tid2 = getBaseTypeForArithmetics(t2.getTypeId());
        AtomicValueFactory avf = frame.getRuntimeControlBlock().getAtomicValueFactory();
        if (tid1 == BuiltinTypeConstants.XS_UNTYPED_ATOMIC_TYPE_ID) {
            v1 = (XDMAtomicValue) avf.createDouble(v1.getStringValue());
            t1 = BuiltinTypeRegistry.XS_DOUBLE;
            tid1 = BuiltinTypeConstants.XS_DOUBLE_TYPE_ID;
        }
        if (tid2 == BuiltinTypeConstants.XS_UNTYPED_ATOMIC_TYPE_ID) {
            v2 = (XDMAtomicValue) avf.createDouble(v2.getStringValue());
            t2 = BuiltinTypeRegistry.XS_DOUBLE;
            tid2 = BuiltinTypeConstants.XS_DOUBLE_TYPE_ID;
        }
        ArithmeticOperation aOp = getArithmeticOperation();
        switch (tid1) {
            case BuiltinTypeConstants.XS_DECIMAL_TYPE_ID:
                switch (tid2) {
                    case BuiltinTypeConstants.XS_DECIMAL_TYPE_ID:
                    case BuiltinTypeConstants.XS_INTEGER_TYPE_ID:
                        return aOp.operateDecimal(avf, (NumericValue) v1, (NumericValue) v2);

                    case BuiltinTypeConstants.XS_FLOAT_TYPE_ID:
                        return aOp.operateFloat(avf, (NumericValue) v1, (NumericValue) v2);

                    case BuiltinTypeConstants.XS_DOUBLE_TYPE_ID:
                        return aOp.operateDouble(avf, (NumericValue) v1, (NumericValue) v2);

                    case BuiltinTypeConstants.XS_DAY_TIME_DURATION_TYPE_ID:
                        return aOp.operateNumericDayTimeDuration(avf, (NumericValue) v1, (DurationValue) v2);

                    case BuiltinTypeConstants.XS_YEAR_MONTH_DURATION_TYPE_ID:
                        return aOp.operateNumericYearMonthDuration(avf, (NumericValue) v1, (DurationValue) v2);
                }
                break;

            case BuiltinTypeConstants.XS_INTEGER_TYPE_ID:
                switch (tid2) {
                    case BuiltinTypeConstants.XS_DECIMAL_TYPE_ID:
                        return aOp.operateDecimal(avf, (NumericValue) v1, (NumericValue) v2);

                    case BuiltinTypeConstants.XS_INTEGER_TYPE_ID:
                        return aOp.operateInteger(avf, (NumericValue) v1, (NumericValue) v2);

                    case BuiltinTypeConstants.XS_FLOAT_TYPE_ID:
                        return aOp.operateFloat(avf, (NumericValue) v1, (NumericValue) v2);

                    case BuiltinTypeConstants.XS_DOUBLE_TYPE_ID:
                        return aOp.operateDouble(avf, (NumericValue) v1, (NumericValue) v2);

                    case BuiltinTypeConstants.XS_DAY_TIME_DURATION_TYPE_ID:
                        return aOp.operateNumericDayTimeDuration(avf, (NumericValue) v1, (DurationValue) v2);

                    case BuiltinTypeConstants.XS_YEAR_MONTH_DURATION_TYPE_ID:
                        return aOp.operateNumericYearMonthDuration(avf, (NumericValue) v1, (DurationValue) v2);
                }
                break;

            case BuiltinTypeConstants.XS_FLOAT_TYPE_ID:
                switch (tid2) {
                    case BuiltinTypeConstants.XS_DECIMAL_TYPE_ID:
                    case BuiltinTypeConstants.XS_INTEGER_TYPE_ID:
                    case BuiltinTypeConstants.XS_FLOAT_TYPE_ID:
                        return aOp.operateFloat(avf, (NumericValue) v1, (NumericValue) v2);

                    case BuiltinTypeConstants.XS_DOUBLE_TYPE_ID:
                        return aOp.operateDouble(avf, (NumericValue) v1, (NumericValue) v2);

                    case BuiltinTypeConstants.XS_DAY_TIME_DURATION_TYPE_ID:
                        return aOp.operateNumericDayTimeDuration(avf, (NumericValue) v1, (DurationValue) v2);

                    case BuiltinTypeConstants.XS_YEAR_MONTH_DURATION_TYPE_ID:
                        return aOp.operateNumericYearMonthDuration(avf, (NumericValue) v1, (DurationValue) v2);
                }
                break;

            case BuiltinTypeConstants.XS_DOUBLE_TYPE_ID:
                switch (tid2) {
                    case BuiltinTypeConstants.XS_DECIMAL_TYPE_ID:
                    case BuiltinTypeConstants.XS_INTEGER_TYPE_ID:
                    case BuiltinTypeConstants.XS_FLOAT_TYPE_ID:
                    case BuiltinTypeConstants.XS_DOUBLE_TYPE_ID:
                        return aOp.operateDouble(avf, (NumericValue) v1, (NumericValue) v2);

                    case BuiltinTypeConstants.XS_DAY_TIME_DURATION_TYPE_ID:
                        return aOp.operateNumericDayTimeDuration(avf, (NumericValue) v1, (DurationValue) v2);

                    case BuiltinTypeConstants.XS_YEAR_MONTH_DURATION_TYPE_ID:
                        return aOp.operateNumericYearMonthDuration(avf, (NumericValue) v1, (DurationValue) v2);
                }
                break;

            case BuiltinTypeConstants.XS_DATE_TYPE_ID:
                switch (tid2) {
                    case BuiltinTypeConstants.XS_DATE_TYPE_ID:
                        return aOp.operateDate(avf, (DateValue) v1, (DateValue) v2);

                    case BuiltinTypeConstants.XS_DAY_TIME_DURATION_TYPE_ID:
                        return aOp.operateDateDayTimeDuration(avf, (DateValue) v1, (DurationValue) v2);

                    case BuiltinTypeConstants.XS_YEAR_MONTH_DURATION_TYPE_ID:
                        return aOp.operateDateYearMonthDuration(avf, (DateValue) v1, (DurationValue) v2);
                }
                break;

            case BuiltinTypeConstants.XS_DATETIME_TYPE_ID:
                switch (tid2) {
                    case BuiltinTypeConstants.XS_DATETIME_TYPE_ID:
                        return aOp.operateDateTime(avf, (DateTimeValue) v1, (DateTimeValue) v2);

                    case BuiltinTypeConstants.XS_DAY_TIME_DURATION_TYPE_ID:
                        return aOp.operateDateTimeDayTimeDuration(avf, (DateTimeValue) v1, (DurationValue) v2);

                    case BuiltinTypeConstants.XS_YEAR_MONTH_DURATION_TYPE_ID:
                        return aOp.operateDateTimeYearMonthDuration(avf, (DateTimeValue) v1, (DurationValue) v2);
                }
                break;

            case BuiltinTypeConstants.XS_TIME_TYPE_ID:
                switch (tid2) {
                    case BuiltinTypeConstants.XS_TIME_TYPE_ID:
                        return aOp.operateTime(avf, (TimeValue) v1, (TimeValue) v2);

                    case BuiltinTypeConstants.XS_DAY_TIME_DURATION_TYPE_ID:
                        return aOp.operateTimeDayTimeDuration(avf, (TimeValue) v1, (DurationValue) v2);

                    case BuiltinTypeConstants.XS_YEAR_MONTH_DURATION_TYPE_ID:
                        return aOp.operateTimeYearMonthDuration(avf, (TimeValue) v1, (DurationValue) v2);
                }
                break;

            case BuiltinTypeConstants.XS_DAY_TIME_DURATION_TYPE_ID:
                switch (tid2) {
                    case BuiltinTypeConstants.XS_DECIMAL_TYPE_ID:
                    case BuiltinTypeConstants.XS_INTEGER_TYPE_ID:
                    case BuiltinTypeConstants.XS_FLOAT_TYPE_ID:
                    case BuiltinTypeConstants.XS_DOUBLE_TYPE_ID:
                        return aOp.operateDayTimeDurationNumeric(avf, (DurationValue) v1, (NumericValue) v2);

                    case BuiltinTypeConstants.XS_DATE_TYPE_ID:
                        return aOp.operateDayTimeDurationDate(avf, (DurationValue) v1, (DateValue) v2);

                    case BuiltinTypeConstants.XS_TIME_TYPE_ID:
                        return aOp.operateDayTimeDurationTime(avf, (DurationValue) v1, (TimeValue) v2);

                    case BuiltinTypeConstants.XS_DATETIME_TYPE_ID:
                        return aOp.operateDayTimeDurationDateTime(avf, (DurationValue) v1, (DateTimeValue) v2);

                    case BuiltinTypeConstants.XS_DAY_TIME_DURATION_TYPE_ID:
                        return aOp.operateDayTimeDuration(avf, (DurationValue) v1, (DurationValue) v2);
                }
                break;

            case BuiltinTypeConstants.XS_YEAR_MONTH_DURATION_TYPE_ID:
                switch (tid2) {
                    case BuiltinTypeConstants.XS_DECIMAL_TYPE_ID:
                    case BuiltinTypeConstants.XS_INTEGER_TYPE_ID:
                    case BuiltinTypeConstants.XS_FLOAT_TYPE_ID:
                    case BuiltinTypeConstants.XS_DOUBLE_TYPE_ID:
                        return aOp.operateYearMonthDurationNumeric(avf, (DurationValue) v1, (NumericValue) v2);

                    case BuiltinTypeConstants.XS_DATE_TYPE_ID:
                        return aOp.operateYearMonthDurationDate(avf, (DurationValue) v1, (DateValue) v2);

                    case BuiltinTypeConstants.XS_TIME_TYPE_ID:
                        return aOp.operateYearMonthDurationTime(avf, (DurationValue) v1, (TimeValue) v2);

                    case BuiltinTypeConstants.XS_DATETIME_TYPE_ID:
                        return aOp.operateYearMonthDurationDateTime(avf, (DurationValue) v1, (DateTimeValue) v2);

                    case BuiltinTypeConstants.XS_YEAR_MONTH_DURATION_TYPE_ID:
                        return aOp.operateYearMonthDuration(avf, (DurationValue) v1, (DurationValue) v2);
                }
                break;
        }
        throw new SystemException(ErrorCode.XPTY0004);
    }

    private int getBaseTypeForArithmetics(int tid) throws SystemException {
        while (true) {
            switch (tid) {
                case BuiltinTypeConstants.XS_STRING_TYPE_ID:
                case BuiltinTypeConstants.XS_DECIMAL_TYPE_ID:
                case BuiltinTypeConstants.XS_INTEGER_TYPE_ID:
                case BuiltinTypeConstants.XS_FLOAT_TYPE_ID:
                case BuiltinTypeConstants.XS_DOUBLE_TYPE_ID:
                case BuiltinTypeConstants.XS_ANY_URI_TYPE_ID:
                case BuiltinTypeConstants.XS_BOOLEAN_TYPE_ID:
                case BuiltinTypeConstants.XS_DATE_TYPE_ID:
                case BuiltinTypeConstants.XS_DATETIME_TYPE_ID:
                case BuiltinTypeConstants.XS_TIME_TYPE_ID:
                case BuiltinTypeConstants.XS_DAY_TIME_DURATION_TYPE_ID:
                case BuiltinTypeConstants.XS_YEAR_MONTH_DURATION_TYPE_ID:
                case BuiltinTypeConstants.XS_BASE64_BINARY_TYPE_ID:
                case BuiltinTypeConstants.XS_HEX_BINARY_TYPE_ID:
                case BuiltinTypeConstants.XS_QNAME_TYPE_ID:
                case BuiltinTypeConstants.XS_G_DAY_TYPE_ID:
                case BuiltinTypeConstants.XS_G_MONTH_DAY_TYPE_ID:
                case BuiltinTypeConstants.XS_G_MONTH_TYPE_ID:
                case BuiltinTypeConstants.XS_G_YEAR_MONTH_TYPE_ID:
                case BuiltinTypeConstants.XS_G_YEAR_TYPE_ID:
                case BuiltinTypeConstants.XS_UNTYPED_ATOMIC_TYPE_ID:
                    return tid;

                case BuiltinTypeConstants.XS_ANY_ATOMIC_TYPE_ID:
                    throw new SystemException(ErrorCode.XPTY0004);

                default:
                    tid = BuiltinTypeRegistry.INSTANCE.getSchemaTypeById(tid).getBaseType().getTypeId();
            }
        }
    }

    protected abstract ArithmeticOperation getArithmeticOperation();
}