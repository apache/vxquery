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
package org.apache.vxquery.v0datamodel.atomic.compare;

import java.util.Comparator;

import org.apache.vxquery.collations.Collation;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.types.AtomicType;
import org.apache.vxquery.types.BuiltinTypeConstants;
import org.apache.vxquery.types.BuiltinTypeRegistry;
import org.apache.vxquery.v0datamodel.XDMAtomicValue;
import org.apache.vxquery.v0datamodel.XDMItem;
import org.apache.vxquery.v0datamodel.XDMNode;
import org.apache.vxquery.v0datamodel.atomic.AtomicValueFactory;
import org.apache.vxquery.v0datamodel.atomic.Base64BinaryValue;
import org.apache.vxquery.v0datamodel.atomic.BooleanValue;
import org.apache.vxquery.v0datamodel.atomic.DateTimeValue;
import org.apache.vxquery.v0datamodel.atomic.DateValue;
import org.apache.vxquery.v0datamodel.atomic.DurationValue;
import org.apache.vxquery.v0datamodel.atomic.GDayValue;
import org.apache.vxquery.v0datamodel.atomic.GMonthDayValue;
import org.apache.vxquery.v0datamodel.atomic.GMonthValue;
import org.apache.vxquery.v0datamodel.atomic.GYearMonthValue;
import org.apache.vxquery.v0datamodel.atomic.GYearValue;
import org.apache.vxquery.v0datamodel.atomic.HexBinaryValue;
import org.apache.vxquery.v0datamodel.atomic.NumericValue;
import org.apache.vxquery.v0datamodel.atomic.QNameValue;
import org.apache.vxquery.v0datamodel.atomic.StringValue;
import org.apache.vxquery.v0datamodel.atomic.TimeValue;
import org.apache.vxquery.v0datamodel.atomic.UntypedAtomicValue;
import org.apache.vxquery.v0runtime.RuntimeControlBlock;

public class ComparisonUtils {
    public static final Comparator<XDMItem> SORT_NODE_ASC_COMPARATOR = new Comparator<XDMItem>() {
        @Override
        public int compare(XDMItem o1, XDMItem o2) {
            XDMNode n1 = (XDMNode) o1;
            XDMNode n2 = (XDMNode) o2;
            return n1.compareDocumentOrder(n2);
        }
    };

    public static final Comparator<XDMItem> SORT_NODE_DESC_COMPARATOR = new Comparator<XDMItem>() {
        @Override
        public int compare(XDMItem o1, XDMItem o2) {
            XDMNode n1 = (XDMNode) o1;
            XDMNode n2 = (XDMNode) o2;
            return n2.compareDocumentOrder(n1);
        }
    };

    public static int getBaseTypeForValueComparison(int tid) throws SystemException {
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

    public static Boolean valueCompare(RuntimeControlBlock rcb, final XDMAtomicValue v1, final XDMAtomicValue v2,
            ValueComparator vComp, Collation collation) throws SystemException {
        final AtomicType t1 = v1.getAtomicType();
        final AtomicType t2 = v2.getAtomicType();
        final int tid1 = ComparisonUtils.getBaseTypeForValueComparison(t1.getTypeId());
        final int tid2 = ComparisonUtils.getBaseTypeForValueComparison(t2.getTypeId());
        final AtomicValueFactory avf = rcb.getAtomicValueFactory();
        switch (tid1) {
            case BuiltinTypeConstants.XS_UNTYPED_ATOMIC_TYPE_ID:
                switch (tid2) {
                    case BuiltinTypeConstants.XS_STRING_TYPE_ID:
                    case BuiltinTypeConstants.XS_ANY_URI_TYPE_ID:
                        return Boolean.valueOf(vComp.compareString(avf.createString(((UntypedAtomicValue) v1)
                                .getStringValue()), (StringValue) v2, collation));

                    case BuiltinTypeConstants.XS_UNTYPED_ATOMIC_TYPE_ID:
                        return Boolean.valueOf(vComp.compareString(avf.createString(((UntypedAtomicValue) v1)
                                .getStringValue()), avf.createString(((UntypedAtomicValue) v2).getStringValue()),
                                collation));
                }
                break;

            case BuiltinTypeConstants.XS_STRING_TYPE_ID:
                switch (tid2) {
                    case BuiltinTypeConstants.XS_STRING_TYPE_ID:
                    case BuiltinTypeConstants.XS_ANY_URI_TYPE_ID:
                        return Boolean.valueOf(vComp.compareString((StringValue) v1, (StringValue) v2, collation));

                    case BuiltinTypeConstants.XS_UNTYPED_ATOMIC_TYPE_ID:
                        return Boolean.valueOf(vComp.compareString((StringValue) v1, avf
                                .createString(((UntypedAtomicValue) v2).getStringValue()), collation));
                }
                break;

            case BuiltinTypeConstants.XS_DECIMAL_TYPE_ID:
                switch (tid2) {
                    case BuiltinTypeConstants.XS_DECIMAL_TYPE_ID:
                    case BuiltinTypeConstants.XS_INTEGER_TYPE_ID:
                        return Boolean.valueOf(vComp.compareDecimal((NumericValue) v1, (NumericValue) v2));

                    case BuiltinTypeConstants.XS_FLOAT_TYPE_ID:
                        return Boolean.valueOf(vComp.compareFloat((NumericValue) v1, (NumericValue) v2));

                    case BuiltinTypeConstants.XS_DOUBLE_TYPE_ID:
                        return Boolean.valueOf(vComp.compareDouble((NumericValue) v1, (NumericValue) v2));
                }
                break;

            case BuiltinTypeConstants.XS_INTEGER_TYPE_ID:
                switch (tid2) {
                    case BuiltinTypeConstants.XS_DECIMAL_TYPE_ID:
                        return Boolean.valueOf(vComp.compareDecimal((NumericValue) v1, (NumericValue) v2));

                    case BuiltinTypeConstants.XS_INTEGER_TYPE_ID:
                        return Boolean.valueOf(vComp.compareInteger((NumericValue) v1, (NumericValue) v2));

                    case BuiltinTypeConstants.XS_FLOAT_TYPE_ID:
                        return Boolean.valueOf(vComp.compareFloat((NumericValue) v1, (NumericValue) v2));

                    case BuiltinTypeConstants.XS_DOUBLE_TYPE_ID:
                        return Boolean.valueOf(vComp.compareDouble((NumericValue) v1, (NumericValue) v2));
                }
                break;

            case BuiltinTypeConstants.XS_FLOAT_TYPE_ID:
                switch (tid2) {
                    case BuiltinTypeConstants.XS_DECIMAL_TYPE_ID:
                    case BuiltinTypeConstants.XS_INTEGER_TYPE_ID:
                    case BuiltinTypeConstants.XS_FLOAT_TYPE_ID:
                        return Boolean.valueOf(vComp.compareFloat((NumericValue) v1, (NumericValue) v2));

                    case BuiltinTypeConstants.XS_DOUBLE_TYPE_ID:
                        return Boolean.valueOf(vComp.compareDouble((NumericValue) v1, (NumericValue) v2));
                }
                break;

            case BuiltinTypeConstants.XS_DOUBLE_TYPE_ID:
                switch (tid2) {
                    case BuiltinTypeConstants.XS_DECIMAL_TYPE_ID:
                    case BuiltinTypeConstants.XS_INTEGER_TYPE_ID:
                    case BuiltinTypeConstants.XS_FLOAT_TYPE_ID:
                    case BuiltinTypeConstants.XS_DOUBLE_TYPE_ID:
                        return Boolean.valueOf(vComp.compareDouble((NumericValue) v1, (NumericValue) v2));
                }
                break;

            case BuiltinTypeConstants.XS_ANY_URI_TYPE_ID:
                switch (tid2) {
                    case BuiltinTypeConstants.XS_STRING_TYPE_ID:
                    case BuiltinTypeConstants.XS_ANY_URI_TYPE_ID:
                        return Boolean.valueOf(vComp.compareString((StringValue) v1, (StringValue) v2, collation));
                }
                break;

            case BuiltinTypeConstants.XS_BOOLEAN_TYPE_ID:
                if (tid2 == BuiltinTypeConstants.XS_BOOLEAN_TYPE_ID) {
                    return Boolean.valueOf(vComp.compareBoolean((BooleanValue) v1, (BooleanValue) v2));
                }
                break;

            case BuiltinTypeConstants.XS_DATE_TYPE_ID:
                if (tid2 == BuiltinTypeConstants.XS_DATE_TYPE_ID) {
                    return Boolean
                            .valueOf(vComp.compareDate((DateValue) v1, (DateValue) v2, rcb.getImplicitTimezone()));
                }
                break;

            case BuiltinTypeConstants.XS_DATETIME_TYPE_ID:
                if (tid2 == BuiltinTypeConstants.XS_DATETIME_TYPE_ID) {
                    return Boolean.valueOf(vComp.compareDateTime((DateTimeValue) v1, (DateTimeValue) v2, rcb
                            .getImplicitTimezone()));
                }
                break;

            case BuiltinTypeConstants.XS_TIME_TYPE_ID:
                if (tid2 == BuiltinTypeConstants.XS_TIME_TYPE_ID) {
                    return Boolean
                            .valueOf(vComp.compareTime((TimeValue) v1, (TimeValue) v2, rcb.getImplicitTimezone()));
                }
                break;

            case BuiltinTypeConstants.XS_DURATION_TYPE_ID:
                if (tid2 == BuiltinTypeConstants.XS_DURATION_TYPE_ID) {
                    return Boolean.valueOf(vComp.compareDuration((DurationValue) v1, (DurationValue) v2));
                }
                break;

            case BuiltinTypeConstants.XS_QNAME_TYPE_ID:
                if (tid2 == BuiltinTypeConstants.XS_QNAME_TYPE_ID) {
                    return Boolean.valueOf(vComp.compareQName((QNameValue) v1, (QNameValue) v2));
                }
                break;

            case BuiltinTypeConstants.XS_BASE64_BINARY_TYPE_ID:
                if (tid2 == BuiltinTypeConstants.XS_BASE64_BINARY_TYPE_ID) {
                    return Boolean.valueOf(vComp.compareBase64Binary((Base64BinaryValue) v1, (Base64BinaryValue) v2));
                }
                break;

            case BuiltinTypeConstants.XS_HEX_BINARY_TYPE_ID:
                if (tid2 == BuiltinTypeConstants.XS_HEX_BINARY_TYPE_ID) {
                    return Boolean.valueOf(vComp.compareHexBinary((HexBinaryValue) v1, (HexBinaryValue) v2));
                }
                break;

            case BuiltinTypeConstants.XS_G_DAY_TYPE_ID:
                if (tid2 == BuiltinTypeConstants.XS_G_DAY_TYPE_ID) {
                    return Boolean
                            .valueOf(vComp.compareGDay((GDayValue) v1, (GDayValue) v2, rcb.getImplicitTimezone()));
                }
                break;

            case BuiltinTypeConstants.XS_G_MONTH_DAY_TYPE_ID:
                if (tid2 == BuiltinTypeConstants.XS_G_MONTH_DAY_TYPE_ID) {
                    return Boolean.valueOf(vComp.compareGMonthDay((GMonthDayValue) v1, (GMonthDayValue) v2, rcb
                            .getImplicitTimezone()));
                }
                break;

            case BuiltinTypeConstants.XS_G_MONTH_TYPE_ID:
                if (tid2 == BuiltinTypeConstants.XS_G_MONTH_TYPE_ID) {
                    return Boolean.valueOf(vComp.compareGMonth((GMonthValue) v1, (GMonthValue) v2, rcb
                            .getImplicitTimezone()));
                }
                break;

            case BuiltinTypeConstants.XS_G_YEAR_MONTH_TYPE_ID:
                if (tid2 == BuiltinTypeConstants.XS_G_YEAR_MONTH_TYPE_ID) {
                    return Boolean.valueOf(vComp.compareGYearMonth((GYearMonthValue) v1, (GYearMonthValue) v2, rcb
                            .getImplicitTimezone()));
                }
                break;

            case BuiltinTypeConstants.XS_G_YEAR_TYPE_ID:
                if (tid2 == BuiltinTypeConstants.XS_G_YEAR_TYPE_ID) {
                    return Boolean.valueOf(vComp.compareGYear((GYearValue) v1, (GYearValue) v2, rcb
                            .getImplicitTimezone()));
                }
                break;
        }
        return null;
    }
}