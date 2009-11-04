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
package org.apache.vxquery.runtime.functions;

import java.util.ArrayList;
import java.util.List;

import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.datamodel.XDMAtomicValue;
import org.apache.vxquery.datamodel.XDMItem;
import org.apache.vxquery.datamodel.XDMNode;
import org.apache.vxquery.datamodel.XDMSequence;
import org.apache.vxquery.datamodel.XDMValue;
import org.apache.vxquery.datamodel.atomic.AtomicValueFactory;
import org.apache.vxquery.datamodel.atomic.Base64BinaryValue;
import org.apache.vxquery.datamodel.atomic.BooleanValue;
import org.apache.vxquery.datamodel.atomic.DateTimeValue;
import org.apache.vxquery.datamodel.atomic.DateValue;
import org.apache.vxquery.datamodel.atomic.DurationValue;
import org.apache.vxquery.datamodel.atomic.GDayValue;
import org.apache.vxquery.datamodel.atomic.GMonthDayValue;
import org.apache.vxquery.datamodel.atomic.GMonthValue;
import org.apache.vxquery.datamodel.atomic.GYearMonthValue;
import org.apache.vxquery.datamodel.atomic.GYearValue;
import org.apache.vxquery.datamodel.atomic.HexBinaryValue;
import org.apache.vxquery.datamodel.atomic.NumericValue;
import org.apache.vxquery.datamodel.atomic.QNameValue;
import org.apache.vxquery.datamodel.atomic.StringValue;
import org.apache.vxquery.datamodel.atomic.TimeValue;
import org.apache.vxquery.datamodel.atomic.UntypedAtomicValue;
import org.apache.vxquery.datamodel.atomic.compare.ValueComparator;
import org.apache.vxquery.exceptions.DefaultSystemExceptionFactory;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.runtime.CallStackFrame;
import org.apache.vxquery.runtime.RegisterAllocator;
import org.apache.vxquery.runtime.RuntimeControlBlock;
import org.apache.vxquery.runtime.base.AbstractEagerlyEvaluatedFunctionIterator;
import org.apache.vxquery.runtime.base.CloseableIterator;
import org.apache.vxquery.runtime.base.RuntimeIterator;
import org.apache.vxquery.types.AtomicType;
import org.apache.vxquery.types.BuiltinTypeConstants;
import org.apache.vxquery.types.BuiltinTypeRegistry;

public abstract class AbstractGeneralComparisonIterator extends AbstractEagerlyEvaluatedFunctionIterator {
    public AbstractGeneralComparisonIterator(RegisterAllocator rAllocator, Function fn, RuntimeIterator[] arguments,
            StaticContext ctx) {
        super(rAllocator, fn, arguments, ctx);
    }

    @Override
    public Object evaluateEagerly(CallStackFrame frame) throws SystemException {
        XDMValue v1 = (XDMValue) arguments[0].evaluateEagerly(frame);
        if (v1 == null) {
            return BooleanValue.FALSE;
        }
        XDMValue v2 = (XDMValue) arguments[1].evaluateEagerly(frame);
        if (v2 == null) {
            return BooleanValue.FALSE;
        }
        List<XDMAtomicValue> left = new ArrayList<XDMAtomicValue>();
        atomize(v1, left);
        List<XDMAtomicValue> right = new ArrayList<XDMAtomicValue>();
        atomize(v2, right);
        for (XDMAtomicValue avLeft : left) {
            for (XDMAtomicValue avRight : right) {
                if (generalCompare(frame, avLeft, avRight)) {
                    return BooleanValue.TRUE;
                }
            }
        }
        return BooleanValue.FALSE;
    }

    private void atomize(XDMValue v, List<XDMAtomicValue> aList) throws SystemException {
        switch (v.getDMOKind()) {
            case ATOMIC_VALUE:
                aList.add((XDMAtomicValue) v);
                break;

            case SEQUENCE:
                CloseableIterator ci = ((XDMSequence) v).createItemIterator();
                XDMItem i;
                while ((i = (XDMItem) ci.next()) != null) {
                    atomize(i, aList);
                }
                ci.close();
                break;

            default:
                XDMNode node = (XDMNode) v;
                XDMValue av = node.getAtomizedValue();
                if (av != null) {
                    atomize(av, aList);
                }
        }
    }

    private boolean generalCompare(CallStackFrame frame, XDMAtomicValue v1, XDMAtomicValue v2) throws SystemException {
        final ValueComparator vComp = getComparator();
        AtomicType t1 = v1.getAtomicType();
        AtomicType t2 = v2.getAtomicType();
        final RuntimeControlBlock rcb = frame.getRuntimeControlBlock();
        final AtomicValueFactory avf = rcb.getAtomicValueFactory();
        int tid1 = getBaseTypeForComparison(t1.getTypeId());
        int tid2 = getBaseTypeForComparison(t2.getTypeId());
        switch (tid1) {
            case BuiltinTypeConstants.XS_UNTYPED_ATOMIC_TYPE_ID:
                switch (tid2) {
                    case BuiltinTypeConstants.XS_STRING_TYPE_ID:
                    case BuiltinTypeConstants.XS_ANY_URI_TYPE_ID:
                        return vComp.compareString(avf.createString(((UntypedAtomicValue) v1).getStringValue()),
                                (StringValue) v2, rcb.getDefaultCollation());

                    case BuiltinTypeConstants.XS_UNTYPED_ATOMIC_TYPE_ID:
                        return vComp.compareString(avf.createString(((UntypedAtomicValue) v1).getStringValue()), avf
                                .createString(((UntypedAtomicValue) v2).getStringValue()), rcb.getDefaultCollation());

                    case BuiltinTypeConstants.XS_DECIMAL_TYPE_ID:
                    case BuiltinTypeConstants.XS_INTEGER_TYPE_ID:
                    case BuiltinTypeConstants.XS_DOUBLE_TYPE_ID:
                    case BuiltinTypeConstants.XS_FLOAT_TYPE_ID:
                        return vComp.compareDouble((NumericValue) avf.createDouble(v1.getStringValue()),
                                (NumericValue) v2);

                    default:
                        v1 = (XDMAtomicValue) t2.getCastProcessor(t1).cast(avf, v1,
                                DefaultSystemExceptionFactory.INSTANCE, ctx);
                        t1 = t2;
                        tid1 = tid2;
                }
                break;
        }

        switch (tid2) {
            case BuiltinTypeConstants.XS_UNTYPED_ATOMIC_TYPE_ID:
                switch (tid1) {
                    case BuiltinTypeConstants.XS_STRING_TYPE_ID:
                    case BuiltinTypeConstants.XS_ANY_URI_TYPE_ID:
                        return vComp.compareString((StringValue) v1, avf.createString(((UntypedAtomicValue) v2)
                                .getStringValue()), rcb.getDefaultCollation());

                    case BuiltinTypeConstants.XS_DECIMAL_TYPE_ID:
                    case BuiltinTypeConstants.XS_INTEGER_TYPE_ID:
                    case BuiltinTypeConstants.XS_DOUBLE_TYPE_ID:
                    case BuiltinTypeConstants.XS_FLOAT_TYPE_ID:
                        return vComp.compareDouble((NumericValue) v1, (NumericValue) avf.createDouble(v2
                                .getStringValue()));

                    default:
                        v2 = (XDMAtomicValue) t1.getCastProcessor(t2).cast(avf, v2,
                                DefaultSystemExceptionFactory.INSTANCE, ctx);
                        t2 = t1;
                        tid2 = tid1;
                }
                break;
        }

        switch (tid1) {
            case BuiltinTypeConstants.XS_STRING_TYPE_ID:
                switch (tid2) {
                    case BuiltinTypeConstants.XS_STRING_TYPE_ID:
                    case BuiltinTypeConstants.XS_ANY_URI_TYPE_ID:
                        return vComp.compareString((StringValue) v1, (StringValue) v2, rcb.getDefaultCollation());
                }
                break;

            case BuiltinTypeConstants.XS_DECIMAL_TYPE_ID:
                switch (tid2) {
                    case BuiltinTypeConstants.XS_DECIMAL_TYPE_ID:
                    case BuiltinTypeConstants.XS_INTEGER_TYPE_ID:
                        return vComp.compareDecimal((NumericValue) v1, (NumericValue) v2);

                    case BuiltinTypeConstants.XS_FLOAT_TYPE_ID:
                        return vComp.compareFloat((NumericValue) v1, (NumericValue) v2);

                    case BuiltinTypeConstants.XS_DOUBLE_TYPE_ID:
                        return vComp.compareDouble((NumericValue) v1, (NumericValue) v2);
                }
                break;

            case BuiltinTypeConstants.XS_INTEGER_TYPE_ID:
                switch (tid2) {
                    case BuiltinTypeConstants.XS_DECIMAL_TYPE_ID:
                        return vComp.compareDecimal((NumericValue) v1, (NumericValue) v2);

                    case BuiltinTypeConstants.XS_INTEGER_TYPE_ID:
                        return vComp.compareInteger((NumericValue) v1, (NumericValue) v2);

                    case BuiltinTypeConstants.XS_FLOAT_TYPE_ID:
                        return vComp.compareFloat((NumericValue) v1, (NumericValue) v2);

                    case BuiltinTypeConstants.XS_DOUBLE_TYPE_ID:
                        return vComp.compareDouble((NumericValue) v1, (NumericValue) v2);
                }
                break;

            case BuiltinTypeConstants.XS_FLOAT_TYPE_ID:
                switch (tid2) {
                    case BuiltinTypeConstants.XS_DECIMAL_TYPE_ID:
                    case BuiltinTypeConstants.XS_INTEGER_TYPE_ID:
                    case BuiltinTypeConstants.XS_FLOAT_TYPE_ID:
                        return vComp.compareFloat((NumericValue) v1, (NumericValue) v2);

                    case BuiltinTypeConstants.XS_DOUBLE_TYPE_ID:
                        return vComp.compareDouble((NumericValue) v1, (NumericValue) v2);
                }
                break;

            case BuiltinTypeConstants.XS_DOUBLE_TYPE_ID:
                switch (tid2) {
                    case BuiltinTypeConstants.XS_DECIMAL_TYPE_ID:
                    case BuiltinTypeConstants.XS_INTEGER_TYPE_ID:
                    case BuiltinTypeConstants.XS_FLOAT_TYPE_ID:
                    case BuiltinTypeConstants.XS_DOUBLE_TYPE_ID:
                        return vComp.compareDouble((NumericValue) v1, (NumericValue) v2);
                }
                break;

            case BuiltinTypeConstants.XS_ANY_URI_TYPE_ID:
                switch (tid2) {
                    case BuiltinTypeConstants.XS_STRING_TYPE_ID:
                    case BuiltinTypeConstants.XS_ANY_URI_TYPE_ID:
                        return vComp.compareString((StringValue) v1, (StringValue) v2, rcb.getDefaultCollation());
                }
                break;

            case BuiltinTypeConstants.XS_BOOLEAN_TYPE_ID:
                if (tid2 == BuiltinTypeConstants.XS_BOOLEAN_TYPE_ID) {
                    return vComp.compareBoolean((BooleanValue) v1, (BooleanValue) v2);
                }
                break;

            case BuiltinTypeConstants.XS_DATE_TYPE_ID:
                if (tid2 == BuiltinTypeConstants.XS_DATE_TYPE_ID) {
                    return vComp.compareDate((DateValue) v1, (DateValue) v2, rcb.getImplicitTimezone());
                }
                break;

            case BuiltinTypeConstants.XS_DATETIME_TYPE_ID:
                if (tid2 == BuiltinTypeConstants.XS_DATETIME_TYPE_ID) {
                    return vComp.compareDateTime((DateTimeValue) v1, (DateTimeValue) v2, rcb.getImplicitTimezone());
                }
                break;

            case BuiltinTypeConstants.XS_TIME_TYPE_ID:
                if (tid2 == BuiltinTypeConstants.XS_TIME_TYPE_ID) {
                    return vComp.compareTime((TimeValue) v1, (TimeValue) v2, rcb.getImplicitTimezone());
                }
                break;

            case BuiltinTypeConstants.XS_DURATION_TYPE_ID:
                if (tid2 == BuiltinTypeConstants.XS_DURATION_TYPE_ID) {
                    return vComp.compareDuration((DurationValue) v1, (DurationValue) v2);
                }
                break;

            case BuiltinTypeConstants.XS_QNAME_TYPE_ID:
                if (tid2 == BuiltinTypeConstants.XS_QNAME_TYPE_ID) {
                    return vComp.compareQName((QNameValue) v1, (QNameValue) v2);
                }
                break;

            case BuiltinTypeConstants.XS_BASE64_BINARY_TYPE_ID:
                if (tid2 == BuiltinTypeConstants.XS_BASE64_BINARY_TYPE_ID) {
                    return vComp.compareBase64Binary((Base64BinaryValue) v1, (Base64BinaryValue) v2);
                }
                break;

            case BuiltinTypeConstants.XS_HEX_BINARY_TYPE_ID:
                if (tid2 == BuiltinTypeConstants.XS_HEX_BINARY_TYPE_ID) {
                    return vComp.compareHexBinary((HexBinaryValue) v1, (HexBinaryValue) v2);
                }
                break;

            case BuiltinTypeConstants.XS_G_DAY_TYPE_ID:
                if (tid2 == BuiltinTypeConstants.XS_G_DAY_TYPE_ID) {
                    return vComp.compareGDay((GDayValue) v1, (GDayValue) v2, rcb.getImplicitTimezone());
                }
                break;

            case BuiltinTypeConstants.XS_G_MONTH_DAY_TYPE_ID:
                if (tid2 == BuiltinTypeConstants.XS_G_MONTH_DAY_TYPE_ID) {
                    return vComp.compareGMonthDay((GMonthDayValue) v1, (GMonthDayValue) v2, rcb.getImplicitTimezone());
                }
                break;

            case BuiltinTypeConstants.XS_G_MONTH_TYPE_ID:
                if (tid2 == BuiltinTypeConstants.XS_G_MONTH_TYPE_ID) {
                    return vComp.compareGMonth((GMonthValue) v1, (GMonthValue) v2, rcb.getImplicitTimezone());
                }
                break;

            case BuiltinTypeConstants.XS_G_YEAR_MONTH_TYPE_ID:
                if (tid2 == BuiltinTypeConstants.XS_G_YEAR_MONTH_TYPE_ID) {
                    return vComp.compareGYearMonth((GYearMonthValue) v1, (GYearMonthValue) v2, rcb
                            .getImplicitTimezone());
                }
                break;

            case BuiltinTypeConstants.XS_G_YEAR_TYPE_ID:
                if (tid2 == BuiltinTypeConstants.XS_G_YEAR_TYPE_ID) {
                    return vComp.compareGYear((GYearValue) v1, (GYearValue) v2, rcb.getImplicitTimezone());
                }
                break;
        }
        throw new SystemException(ErrorCode.XPTY0004);
    }

    private int getBaseTypeForComparison(int tid) throws SystemException {
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
                case BuiltinTypeConstants.XS_DURATION_TYPE_ID:
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

    protected abstract ValueComparator getComparator();
}