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
package org.apache.vxquery.types;

import javax.xml.namespace.QName;

import org.apache.vxquery.xmlquery.query.XQueryConstants;

public class BuiltinTypeQNames {
    public static final String UNTYPED_STR = "untyped";

    public static final String UNTYPED_ATOMIC_STR = "untyped";

    public static final String ANY_TYPE_STR = "anyType";

    public static final QName XS_ANY_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, ANY_TYPE_STR,
            XQueryConstants.XS_PREFIX);
    public static final QName XS_UNTYPED_QNAME = new QName(XQueryConstants.XS_NSURI, UNTYPED_STR,
            XQueryConstants.XS_PREFIX);
    public static final QName XS_ANY_ATOMIC_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "anyAtomicType",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_ANY_SIMPLE_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "anySimpleType",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_STRING_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "string",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_NORMALIZED_STRING_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "normalizedString",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_TOKEN_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "token",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_LANGUAGE_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "language",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_NMTOKEN_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "NMTOKEN",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_NAME_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "Name",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_NCNAME_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "NCName",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_ID_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "ID", XQueryConstants.XS_PREFIX);
    public static final QName XS_IDREF_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "IDREF",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_ENTITY_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "ENTITY",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_UNTYPED_ATOMIC_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "untypedAtomic",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_DATETIME_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "dateTime",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_DATE_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "date",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_TIME_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "time",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_DURATION_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "duration",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_YEAR_MONTH_DURATION_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI,
            "yearMonthDuration", XQueryConstants.XS_PREFIX);
    public static final QName XS_DAY_TIME_DURATION_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "dayTimeDuration",
            XQueryConstants.XS_PREFIX);
    public static final QName XSEXT_NUMERIC_TYPE_QNAME = new QName(XQueryConstants.XSEXT_NSURI, "numeric",
            XQueryConstants.XSEXT_PREFIX);
    public static final QName XS_FLOAT_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "float",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_DOUBLE_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "double",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_DECIMAL_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "decimal",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_INTEGER_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "integer",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_NON_POSITIVE_INTEGER_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI,
            "nonPositiveInteger", XQueryConstants.XS_PREFIX);
    public static final QName XS_NEGATIVE_INTEGER_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "negativeInteger",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_LONG_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "long",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_INT_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "int", XQueryConstants.XS_PREFIX);
    public static final QName XS_SHORT_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "short",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_BYTE_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "byte",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_NON_NEGATIVE_INTEGER_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI,
            "nonNegativeInteger", XQueryConstants.XS_PREFIX);
    public static final QName XS_UNSIGNED_LONG_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "unsignedLong",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_UNSIGNED_INT_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "unsignedInt",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_UNSIGNED_SHORT_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "unsignedShort",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_UNSIGNED_BYTE_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "unsignedByte",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_POSITIVE_INTEGER_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "positiveInteger",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_G_YEAR_MONTH_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "gYearMonth",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_G_YEAR_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "gYear",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_G_MONTH_DAY_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "gMonthDay",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_G_DAY_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "gDay",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_G_MONTH_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "gMonth",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_BOOLEAN_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "boolean",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_BASE64_BINARY_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "base64Binary",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_HEX_BINARY_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "hexBinary",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_ANY_URI_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "anyURI",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_QNAME_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "QName",
            XQueryConstants.XS_PREFIX);
    public static final QName XS_NOTATION_TYPE_QNAME = new QName(XQueryConstants.XS_NSURI, "NOTATION",
            XQueryConstants.XS_PREFIX);
    public static final QName XSEXT_TYPE_TYPE_QNAME = new QName(XQueryConstants.XSEXT_NSURI, "type",
            XQueryConstants.XSEXT_PREFIX);
}