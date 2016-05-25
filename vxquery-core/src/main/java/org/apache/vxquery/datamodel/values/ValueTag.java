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
package org.apache.vxquery.datamodel.values;

import org.apache.vxquery.types.BuiltinTypeConstants;

public class ValueTag {
    public static final int XS_ANY_TAG = BuiltinTypeConstants.XS_ANY_TYPE_ID;
    public static final int XS_ANY_SIMPLE_TAG = BuiltinTypeConstants.XS_ANY_SIMPLE_TYPE_ID;
    public static final int XS_UNTYPED_TAG = BuiltinTypeConstants.XS_UNTYPED_TYPE_ID;

    public static final int XS_ANY_ATOMIC_TAG = BuiltinTypeConstants.XS_ANY_ATOMIC_TYPE_ID;
    public static final int XS_STRING_TAG = BuiltinTypeConstants.XS_STRING_TYPE_ID;
    public static final int XS_NORMALIZED_STRING_TAG = BuiltinTypeConstants.XS_NORMALIZED_STRING_TYPE_ID;
    public static final int XS_TOKEN_TAG = BuiltinTypeConstants.XS_TOKEN_TYPE_ID;
    public static final int XS_LANGUAGE_TAG = BuiltinTypeConstants.XS_LANGUAGE_TYPE_ID;
    public static final int XS_NMTOKEN_TAG = BuiltinTypeConstants.XS_NMTOKEN_TYPE_ID;
    public static final int XS_NAME_TAG = BuiltinTypeConstants.XS_NAME_TYPE_ID;
    public static final int XS_NCNAME_TAG = BuiltinTypeConstants.XS_NCNAME_TYPE_ID;
    public static final int XS_ID_TAG = BuiltinTypeConstants.XS_ID_TYPE_ID;
    public static final int XS_IDREF_TAG = BuiltinTypeConstants.XS_IDREF_TYPE_ID;
    public static final int XS_ENTITY_TAG = BuiltinTypeConstants.XS_ENTITY_TYPE_ID;
    public static final int XS_UNTYPED_ATOMIC_TAG = BuiltinTypeConstants.XS_UNTYPED_ATOMIC_TYPE_ID;
    public static final int XS_DATETIME_TAG = BuiltinTypeConstants.XS_DATETIME_TYPE_ID;
    public static final int XS_DATE_TAG = BuiltinTypeConstants.XS_DATE_TYPE_ID;
    public static final int XS_TIME_TAG = BuiltinTypeConstants.XS_TIME_TYPE_ID;
    public static final int XS_DURATION_TAG = BuiltinTypeConstants.XS_DURATION_TYPE_ID;
    public static final int XS_YEAR_MONTH_DURATION_TAG = BuiltinTypeConstants.XS_YEAR_MONTH_DURATION_TYPE_ID;
    public static final int XS_DAY_TIME_DURATION_TAG = BuiltinTypeConstants.XS_DAY_TIME_DURATION_TYPE_ID;
    public static final int XSEXT_NUMERIC_TAG = BuiltinTypeConstants.XSEXT_NUMERIC_TYPE_ID;
    public static final int XS_FLOAT_TAG = BuiltinTypeConstants.XS_FLOAT_TYPE_ID;
    public static final int XS_DOUBLE_TAG = BuiltinTypeConstants.XS_DOUBLE_TYPE_ID;
    public static final int XS_DECIMAL_TAG = BuiltinTypeConstants.XS_DECIMAL_TYPE_ID;
    public static final int XS_INTEGER_TAG = BuiltinTypeConstants.XS_INTEGER_TYPE_ID;
    public static final int XS_NON_POSITIVE_INTEGER_TAG = BuiltinTypeConstants.XS_NON_POSITIVE_INTEGER_TYPE_ID;
    public static final int XS_NEGATIVE_INTEGER_TAG = BuiltinTypeConstants.XS_NEGATIVE_INTEGER_TYPE_ID;
    public static final int XS_LONG_TAG = BuiltinTypeConstants.XS_LONG_TYPE_ID;
    public static final int XS_INT_TAG = BuiltinTypeConstants.XS_INT_TYPE_ID;
    public static final int XS_SHORT_TAG = BuiltinTypeConstants.XS_SHORT_TYPE_ID;
    public static final int XS_BYTE_TAG = BuiltinTypeConstants.XS_BYTE_TYPE_ID;
    public static final int XS_NON_NEGATIVE_INTEGER_TAG = BuiltinTypeConstants.XS_NON_NEGATIVE_INTEGER_TYPE_ID;
    public static final int XS_UNSIGNED_LONG_TAG = BuiltinTypeConstants.XS_UNSIGNED_LONG_TYPE_ID;
    public static final int XS_UNSIGNED_INT_TAG = BuiltinTypeConstants.XS_UNSIGNED_INT_TYPE_ID;
    public static final int XS_UNSIGNED_SHORT_TAG = BuiltinTypeConstants.XS_UNSIGNED_SHORT_TYPE_ID;
    public static final int XS_UNSIGNED_BYTE_TAG = BuiltinTypeConstants.XS_UNSIGNED_BYTE_TYPE_ID;
    public static final int XS_POSITIVE_INTEGER_TAG = BuiltinTypeConstants.XS_POSITIVE_INTEGER_TYPE_ID;
    public static final int XS_G_YEAR_MONTH_TAG = BuiltinTypeConstants.XS_G_YEAR_MONTH_TYPE_ID;
    public static final int XS_G_YEAR_TAG = BuiltinTypeConstants.XS_G_YEAR_TYPE_ID;
    public static final int XS_G_MONTH_DAY_TAG = BuiltinTypeConstants.XS_G_MONTH_DAY_TYPE_ID;
    public static final int XS_G_DAY_TAG = BuiltinTypeConstants.XS_G_DAY_TYPE_ID;
    public static final int XS_G_MONTH_TAG = BuiltinTypeConstants.XS_G_MONTH_TYPE_ID;
    public static final int XS_BOOLEAN_TAG = BuiltinTypeConstants.XS_BOOLEAN_TYPE_ID;
    public static final int XS_BASE64_BINARY_TAG = BuiltinTypeConstants.XS_BASE64_BINARY_TYPE_ID;
    public static final int XS_HEX_BINARY_TAG = BuiltinTypeConstants.XS_HEX_BINARY_TYPE_ID;
    public static final int XS_ANY_URI_TAG = BuiltinTypeConstants.XS_ANY_URI_TYPE_ID;
    public static final int XS_QNAME_TAG = BuiltinTypeConstants.XS_QNAME_TYPE_ID;
    public static final int XS_NOTATION_TAG = BuiltinTypeConstants.XS_NOTATION_TYPE_ID;

    public static final int XS_IDREFS_TAG = BuiltinTypeConstants.XS_IDREFS_TYPE_ID;
    public static final int XS_NMTOKENS_TAG = BuiltinTypeConstants.XS_NMTOKENS_TYPE_ID;
    public static final int XS_ENTITIES_TAG = BuiltinTypeConstants.XS_ENTITIES_TYPE_ID;

    public static final int JS_NULL_TAG = BuiltinTypeConstants.JS_NULL_TYPE_ID;

    public static final int SEQUENCE_TAG = 100;
    public static final int DOCUMENT_NODE_TAG = 101;
    public static final int ELEMENT_NODE_TAG = 102;
    public static final int ATTRIBUTE_NODE_TAG = 103;
    public static final int TEXT_NODE_TAG = 104;
    public static final int COMMENT_NODE_TAG = 105;
    public static final int PI_NODE_TAG = 106;
    public static final int NODE_TREE_TAG = 107;
    public static final int ARRAY_TAG = 108;
    public static final int OBJECT_TAG = 109;

    public static boolean isAtomic(int tag) {
        return tag < 100;
    }

    public static boolean isNode(int tag) {
        switch (tag) {
            case DOCUMENT_NODE_TAG:
            case ELEMENT_NODE_TAG:
            case ATTRIBUTE_NODE_TAG:
            case TEXT_NODE_TAG:
            case COMMENT_NODE_TAG:
            case PI_NODE_TAG:
                return true;
            default:
                return false;
        }
    }
}
