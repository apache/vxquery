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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.xml.namespace.QName;

public final class BuiltinTypeRegistry {
    public static final BuiltinAtomicType XS_ANY_ATOMIC = new BuiltinAtomicType(
            BuiltinTypeConstants.XS_ANY_ATOMIC_TYPE_ID, AnySimpleType.INSTANCE, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_STRING = new BuiltinAtomicType(BuiltinTypeConstants.XS_STRING_TYPE_ID,
            XS_ANY_ATOMIC, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_NORMALIZED_STRING = new BuiltinAtomicType(
            BuiltinTypeConstants.XS_NORMALIZED_STRING_TYPE_ID, XS_STRING, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_TOKEN = new BuiltinAtomicType(BuiltinTypeConstants.XS_TOKEN_TYPE_ID,
            XS_NORMALIZED_STRING, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_LANGUAGE = new BuiltinAtomicType(BuiltinTypeConstants.XS_LANGUAGE_TYPE_ID,
            XS_TOKEN, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_NMTOKEN = new BuiltinAtomicType(BuiltinTypeConstants.XS_NMTOKEN_TYPE_ID,
            XS_TOKEN, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_NAME = new BuiltinAtomicType(BuiltinTypeConstants.XS_NAME_TYPE_ID,
            XS_TOKEN, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_NCNAME = new BuiltinAtomicType(BuiltinTypeConstants.XS_NCNAME_TYPE_ID,
            XS_NAME, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_ID = new BuiltinAtomicType(BuiltinTypeConstants.XS_ID_TYPE_ID, XS_NCNAME,
            DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_IDREF = new BuiltinAtomicType(BuiltinTypeConstants.XS_IDREF_TYPE_ID,
            XS_NCNAME, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_ENTITY = new BuiltinAtomicType(BuiltinTypeConstants.XS_ENTITY_TYPE_ID,
            XS_NCNAME, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_UNTYPED_ATOMIC = new BuiltinAtomicType(
            BuiltinTypeConstants.XS_UNTYPED_ATOMIC_TYPE_ID, XS_ANY_ATOMIC, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_DATETIME = new BuiltinAtomicType(BuiltinTypeConstants.XS_DATETIME_TYPE_ID,
            XS_ANY_ATOMIC, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_DATE = new BuiltinAtomicType(BuiltinTypeConstants.XS_DATE_TYPE_ID,
            XS_ANY_ATOMIC, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_TIME = new BuiltinAtomicType(BuiltinTypeConstants.XS_TIME_TYPE_ID,
            XS_ANY_ATOMIC, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_DURATION = new BuiltinAtomicType(BuiltinTypeConstants.XS_DURATION_TYPE_ID,
            XS_ANY_ATOMIC, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_YEAR_MONTH_DURATION = new BuiltinAtomicType(
            BuiltinTypeConstants.XS_YEAR_MONTH_DURATION_TYPE_ID, XS_DURATION, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_DAY_TIME_DURATION = new BuiltinAtomicType(
            BuiltinTypeConstants.XS_DAY_TIME_DURATION_TYPE_ID, XS_DURATION, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XSEXT_NUMERIC = new BuiltinAtomicType(
            BuiltinTypeConstants.XSEXT_NUMERIC_TYPE_ID, XS_ANY_ATOMIC, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_FLOAT = new BuiltinAtomicType(BuiltinTypeConstants.XS_FLOAT_TYPE_ID,
            XSEXT_NUMERIC, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_DOUBLE = new BuiltinAtomicType(BuiltinTypeConstants.XS_DOUBLE_TYPE_ID,
            XSEXT_NUMERIC, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_DECIMAL = new BuiltinAtomicType(BuiltinTypeConstants.XS_DECIMAL_TYPE_ID,
            XSEXT_NUMERIC, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_INTEGER = new BuiltinAtomicType(BuiltinTypeConstants.XS_INTEGER_TYPE_ID,
            XS_DECIMAL, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_NON_POSITIVE_INTEGER = new BuiltinAtomicType(
            BuiltinTypeConstants.XS_NON_POSITIVE_INTEGER_TYPE_ID, XS_INTEGER, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_NEGATIVE_INTEGER = new BuiltinAtomicType(
            BuiltinTypeConstants.XS_NEGATIVE_INTEGER_TYPE_ID, XS_NON_POSITIVE_INTEGER, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_LONG = new BuiltinAtomicType(BuiltinTypeConstants.XS_LONG_TYPE_ID,
            XS_INTEGER, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_INT = new BuiltinAtomicType(BuiltinTypeConstants.XS_INT_TYPE_ID, XS_LONG,
            DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_SHORT = new BuiltinAtomicType(BuiltinTypeConstants.XS_SHORT_TYPE_ID,
            XS_INT, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_BYTE = new BuiltinAtomicType(BuiltinTypeConstants.XS_BYTE_TYPE_ID,
            XS_SHORT, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_NON_NEGATIVE_INTEGER = new BuiltinAtomicType(
            BuiltinTypeConstants.XS_NON_NEGATIVE_INTEGER_TYPE_ID, XS_INTEGER, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_UNSIGNED_LONG = new BuiltinAtomicType(
            BuiltinTypeConstants.XS_UNSIGNED_LONG_TYPE_ID, XS_NON_NEGATIVE_INTEGER, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_UNSIGNED_INT = new BuiltinAtomicType(
            BuiltinTypeConstants.XS_UNSIGNED_INT_TYPE_ID, XS_UNSIGNED_LONG, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_UNSIGNED_SHORT = new BuiltinAtomicType(
            BuiltinTypeConstants.XS_UNSIGNED_SHORT_TYPE_ID, XS_UNSIGNED_INT, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_UNSIGNED_BYTE = new BuiltinAtomicType(
            BuiltinTypeConstants.XS_UNSIGNED_BYTE_TYPE_ID, XS_UNSIGNED_SHORT, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_POSITIVE_INTEGER = new BuiltinAtomicType(
            BuiltinTypeConstants.XS_POSITIVE_INTEGER_TYPE_ID, XS_NON_NEGATIVE_INTEGER, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_G_YEAR_MONTH = new BuiltinAtomicType(
            BuiltinTypeConstants.XS_G_YEAR_MONTH_TYPE_ID, XS_ANY_ATOMIC, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_G_YEAR = new BuiltinAtomicType(BuiltinTypeConstants.XS_G_YEAR_TYPE_ID,
            XS_ANY_ATOMIC, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_G_MONTH_DAY = new BuiltinAtomicType(
            BuiltinTypeConstants.XS_G_MONTH_DAY_TYPE_ID, XS_ANY_ATOMIC, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_G_DAY = new BuiltinAtomicType(BuiltinTypeConstants.XS_G_DAY_TYPE_ID,
            XS_ANY_ATOMIC, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_G_MONTH = new BuiltinAtomicType(BuiltinTypeConstants.XS_G_MONTH_TYPE_ID,
            XS_ANY_ATOMIC, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_BOOLEAN = new BuiltinAtomicType(BuiltinTypeConstants.XS_BOOLEAN_TYPE_ID,
            XS_ANY_ATOMIC, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_BASE64_BINARY = new BuiltinAtomicType(
            BuiltinTypeConstants.XS_BASE64_BINARY_TYPE_ID, XS_ANY_ATOMIC, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_HEX_BINARY = new BuiltinAtomicType(
            BuiltinTypeConstants.XS_HEX_BINARY_TYPE_ID, XS_ANY_ATOMIC, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_ANY_URI = new BuiltinAtomicType(BuiltinTypeConstants.XS_ANY_URI_TYPE_ID,
            XS_ANY_ATOMIC, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_QNAME = new BuiltinAtomicType(BuiltinTypeConstants.XS_QNAME_TYPE_ID,
            XS_ANY_ATOMIC, DerivationProcess.RESTRICTION);

    public static final BuiltinAtomicType XS_NOTATION = new BuiltinAtomicType(BuiltinTypeConstants.XS_NOTATION_TYPE_ID,
            XS_ANY_ATOMIC, DerivationProcess.RESTRICTION);

    public static final BuiltinTypeRegistry INSTANCE = new BuiltinTypeRegistry();

    private final SchemaType[] types;

    private final QName[] typeNames;

    private BuiltinTypeRegistry() {
        types = new SchemaType[BuiltinTypeConstants.BUILTIN_TYPE_COUNT];
        types[BuiltinTypeConstants.XS_ANY_SIMPLE_TYPE_ID] = AnySimpleType.INSTANCE;
        types[BuiltinTypeConstants.XS_ANY_ATOMIC_TYPE_ID] = XS_ANY_ATOMIC;
        types[BuiltinTypeConstants.XS_STRING_TYPE_ID] = XS_STRING;
        types[BuiltinTypeConstants.XS_NORMALIZED_STRING_TYPE_ID] = XS_NORMALIZED_STRING;
        types[BuiltinTypeConstants.XS_TOKEN_TYPE_ID] = XS_TOKEN;
        types[BuiltinTypeConstants.XS_LANGUAGE_TYPE_ID] = XS_LANGUAGE;
        types[BuiltinTypeConstants.XS_NMTOKEN_TYPE_ID] = XS_NMTOKEN;
        types[BuiltinTypeConstants.XS_NAME_TYPE_ID] = XS_NAME;
        types[BuiltinTypeConstants.XS_NCNAME_TYPE_ID] = XS_NCNAME;
        types[BuiltinTypeConstants.XS_ID_TYPE_ID] = XS_ID;
        types[BuiltinTypeConstants.XS_IDREF_TYPE_ID] = XS_IDREF;
        types[BuiltinTypeConstants.XS_ENTITY_TYPE_ID] = XS_ENTITY;
        types[BuiltinTypeConstants.XS_UNTYPED_ATOMIC_TYPE_ID] = XS_UNTYPED_ATOMIC;
        types[BuiltinTypeConstants.XS_DATETIME_TYPE_ID] = XS_DATETIME;
        types[BuiltinTypeConstants.XS_DATE_TYPE_ID] = XS_DATE;
        types[BuiltinTypeConstants.XS_TIME_TYPE_ID] = XS_TIME;
        types[BuiltinTypeConstants.XS_DURATION_TYPE_ID] = XS_DURATION;
        types[BuiltinTypeConstants.XS_YEAR_MONTH_DURATION_TYPE_ID] = XS_YEAR_MONTH_DURATION;
        types[BuiltinTypeConstants.XS_DAY_TIME_DURATION_TYPE_ID] = XS_DAY_TIME_DURATION;
        types[BuiltinTypeConstants.XSEXT_NUMERIC_TYPE_ID] = XSEXT_NUMERIC;
        types[BuiltinTypeConstants.XS_FLOAT_TYPE_ID] = XS_FLOAT;
        types[BuiltinTypeConstants.XS_DOUBLE_TYPE_ID] = XS_DOUBLE;
        types[BuiltinTypeConstants.XS_DECIMAL_TYPE_ID] = XS_DECIMAL;
        types[BuiltinTypeConstants.XS_INTEGER_TYPE_ID] = XS_INTEGER;
        types[BuiltinTypeConstants.XS_NON_POSITIVE_INTEGER_TYPE_ID] = XS_NON_POSITIVE_INTEGER;
        types[BuiltinTypeConstants.XS_NEGATIVE_INTEGER_TYPE_ID] = XS_NEGATIVE_INTEGER;
        types[BuiltinTypeConstants.XS_LONG_TYPE_ID] = XS_LONG;
        types[BuiltinTypeConstants.XS_INT_TYPE_ID] = XS_INT;
        types[BuiltinTypeConstants.XS_SHORT_TYPE_ID] = XS_SHORT;
        types[BuiltinTypeConstants.XS_BYTE_TYPE_ID] = XS_BYTE;
        types[BuiltinTypeConstants.XS_NON_NEGATIVE_INTEGER_TYPE_ID] = XS_NON_NEGATIVE_INTEGER;
        types[BuiltinTypeConstants.XS_UNSIGNED_LONG_TYPE_ID] = XS_UNSIGNED_LONG;
        types[BuiltinTypeConstants.XS_UNSIGNED_INT_TYPE_ID] = XS_UNSIGNED_INT;
        types[BuiltinTypeConstants.XS_UNSIGNED_SHORT_TYPE_ID] = XS_UNSIGNED_SHORT;
        types[BuiltinTypeConstants.XS_UNSIGNED_BYTE_TYPE_ID] = XS_UNSIGNED_BYTE;
        types[BuiltinTypeConstants.XS_POSITIVE_INTEGER_TYPE_ID] = XS_POSITIVE_INTEGER;
        types[BuiltinTypeConstants.XS_G_YEAR_MONTH_TYPE_ID] = XS_G_YEAR_MONTH;
        types[BuiltinTypeConstants.XS_G_YEAR_TYPE_ID] = XS_G_YEAR;
        types[BuiltinTypeConstants.XS_G_MONTH_DAY_TYPE_ID] = XS_G_MONTH_DAY;
        types[BuiltinTypeConstants.XS_G_DAY_TYPE_ID] = XS_G_DAY;
        types[BuiltinTypeConstants.XS_G_MONTH_TYPE_ID] = XS_G_MONTH;
        types[BuiltinTypeConstants.XS_BOOLEAN_TYPE_ID] = XS_BOOLEAN;
        types[BuiltinTypeConstants.XS_BASE64_BINARY_TYPE_ID] = XS_BASE64_BINARY;
        types[BuiltinTypeConstants.XS_HEX_BINARY_TYPE_ID] = XS_HEX_BINARY;
        types[BuiltinTypeConstants.XS_ANY_URI_TYPE_ID] = XS_ANY_URI;
        types[BuiltinTypeConstants.XS_QNAME_TYPE_ID] = XS_QNAME;
        types[BuiltinTypeConstants.XS_NOTATION_TYPE_ID] = XS_NOTATION;

        typeNames = new QName[BuiltinTypeConstants.BUILTIN_TYPE_COUNT];
        typeNames[BuiltinTypeConstants.XS_ANY_TYPE_ID] = BuiltinTypeQNames.XS_ANY_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_ANY_SIMPLE_TYPE_ID] = BuiltinTypeQNames.XS_ANY_SIMPLE_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_UNTYPED_TYPE_ID] = BuiltinTypeQNames.XS_UNTYPED_QNAME;
        typeNames[BuiltinTypeConstants.XS_ANY_ATOMIC_TYPE_ID] = BuiltinTypeQNames.XS_ANY_ATOMIC_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_STRING_TYPE_ID] = BuiltinTypeQNames.XS_STRING_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_NORMALIZED_STRING_TYPE_ID] = BuiltinTypeQNames.XS_NORMALIZED_STRING_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_TOKEN_TYPE_ID] = BuiltinTypeQNames.XS_TOKEN_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_LANGUAGE_TYPE_ID] = BuiltinTypeQNames.XS_LANGUAGE_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_NMTOKEN_TYPE_ID] = BuiltinTypeQNames.XS_NMTOKEN_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_NAME_TYPE_ID] = BuiltinTypeQNames.XS_NAME_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_NCNAME_TYPE_ID] = BuiltinTypeQNames.XS_NCNAME_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_ID_TYPE_ID] = BuiltinTypeQNames.XS_ID_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_IDREF_TYPE_ID] = BuiltinTypeQNames.XS_IDREF_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_ENTITY_TYPE_ID] = BuiltinTypeQNames.XS_ENTITY_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_UNTYPED_ATOMIC_TYPE_ID] = BuiltinTypeQNames.XS_UNTYPED_ATOMIC_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_DATETIME_TYPE_ID] = BuiltinTypeQNames.XS_DATETIME_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_DATE_TYPE_ID] = BuiltinTypeQNames.XS_DATE_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_TIME_TYPE_ID] = BuiltinTypeQNames.XS_TIME_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_DURATION_TYPE_ID] = BuiltinTypeQNames.XS_DURATION_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_YEAR_MONTH_DURATION_TYPE_ID] = BuiltinTypeQNames.XS_YEAR_MONTH_DURATION_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_DAY_TIME_DURATION_TYPE_ID] = BuiltinTypeQNames.XS_DAY_TIME_DURATION_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XSEXT_NUMERIC_TYPE_ID] = BuiltinTypeQNames.XSEXT_NUMERIC_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_FLOAT_TYPE_ID] = BuiltinTypeQNames.XS_FLOAT_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_DOUBLE_TYPE_ID] = BuiltinTypeQNames.XS_DOUBLE_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_DECIMAL_TYPE_ID] = BuiltinTypeQNames.XS_DECIMAL_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_INTEGER_TYPE_ID] = BuiltinTypeQNames.XS_INTEGER_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_NON_POSITIVE_INTEGER_TYPE_ID] = BuiltinTypeQNames.XS_NON_POSITIVE_INTEGER_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_NEGATIVE_INTEGER_TYPE_ID] = BuiltinTypeQNames.XS_NEGATIVE_INTEGER_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_LONG_TYPE_ID] = BuiltinTypeQNames.XS_LONG_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_INT_TYPE_ID] = BuiltinTypeQNames.XS_INT_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_SHORT_TYPE_ID] = BuiltinTypeQNames.XS_SHORT_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_BYTE_TYPE_ID] = BuiltinTypeQNames.XS_BYTE_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_NON_NEGATIVE_INTEGER_TYPE_ID] = BuiltinTypeQNames.XS_NON_NEGATIVE_INTEGER_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_UNSIGNED_LONG_TYPE_ID] = BuiltinTypeQNames.XS_UNSIGNED_LONG_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_UNSIGNED_INT_TYPE_ID] = BuiltinTypeQNames.XS_UNSIGNED_INT_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_UNSIGNED_SHORT_TYPE_ID] = BuiltinTypeQNames.XS_UNSIGNED_SHORT_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_UNSIGNED_BYTE_TYPE_ID] = BuiltinTypeQNames.XS_UNSIGNED_BYTE_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_POSITIVE_INTEGER_TYPE_ID] = BuiltinTypeQNames.XS_POSITIVE_INTEGER_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_G_YEAR_MONTH_TYPE_ID] = BuiltinTypeQNames.XS_G_YEAR_MONTH_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_G_YEAR_TYPE_ID] = BuiltinTypeQNames.XS_G_YEAR_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_G_MONTH_DAY_TYPE_ID] = BuiltinTypeQNames.XS_G_MONTH_DAY_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_G_DAY_TYPE_ID] = BuiltinTypeQNames.XS_G_DAY_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_G_MONTH_TYPE_ID] = BuiltinTypeQNames.XS_G_MONTH_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_BOOLEAN_TYPE_ID] = BuiltinTypeQNames.XS_BOOLEAN_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_BASE64_BINARY_TYPE_ID] = BuiltinTypeQNames.XS_BASE64_BINARY_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_HEX_BINARY_TYPE_ID] = BuiltinTypeQNames.XS_HEX_BINARY_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_ANY_URI_TYPE_ID] = BuiltinTypeQNames.XS_ANY_URI_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_QNAME_TYPE_ID] = BuiltinTypeQNames.XS_QNAME_TYPE_QNAME;
        typeNames[BuiltinTypeConstants.XS_NOTATION_TYPE_ID] = BuiltinTypeQNames.XS_NOTATION_TYPE_QNAME;
    }

    public SchemaType getSchemaTypeById(int id) {
        return types[id];
    }

    public boolean isBuiltinTypeId(int id) {
        return id < BuiltinTypeConstants.BUILTIN_TYPE_COUNT;
    }

    public boolean isBuiltinType(SchemaType type) {
        return isBuiltinTypeId(type.getTypeId());
    }

    public SchemaType getBuiltinBaseType(SchemaType type) {
        while (!isBuiltinType(type)) {
            type = type.getBaseType();
        }
        return type;
    }

    public QName getTypeName(int id) {
        return typeNames[id];
    }

    public static final Map<QName, SchemaType> TYPE_MAP;

    static {
        Map<QName, SchemaType> typeMap = new LinkedHashMap<QName, SchemaType>();
        typeMap.put(BuiltinTypeQNames.XS_ANY_SIMPLE_TYPE_QNAME, AnySimpleType.INSTANCE);
        typeMap.put(BuiltinTypeQNames.XS_ANY_ATOMIC_TYPE_QNAME, XS_ANY_ATOMIC);
        typeMap.put(BuiltinTypeQNames.XS_STRING_TYPE_QNAME, XS_STRING);
        typeMap.put(BuiltinTypeQNames.XS_NORMALIZED_STRING_TYPE_QNAME, XS_NORMALIZED_STRING);
        typeMap.put(BuiltinTypeQNames.XS_TOKEN_TYPE_QNAME, XS_TOKEN);
        typeMap.put(BuiltinTypeQNames.XS_LANGUAGE_TYPE_QNAME, XS_LANGUAGE);
        typeMap.put(BuiltinTypeQNames.XS_NMTOKEN_TYPE_QNAME, XS_NMTOKEN);
        typeMap.put(BuiltinTypeQNames.XS_NAME_TYPE_QNAME, XS_NAME);
        typeMap.put(BuiltinTypeQNames.XS_NCNAME_TYPE_QNAME, XS_NCNAME);
        typeMap.put(BuiltinTypeQNames.XS_ID_TYPE_QNAME, XS_ID);
        typeMap.put(BuiltinTypeQNames.XS_IDREF_TYPE_QNAME, XS_IDREF);
        typeMap.put(BuiltinTypeQNames.XS_ENTITY_TYPE_QNAME, XS_ENTITY);
        typeMap.put(BuiltinTypeQNames.XS_UNTYPED_ATOMIC_TYPE_QNAME, XS_UNTYPED_ATOMIC);
        typeMap.put(BuiltinTypeQNames.XS_DATETIME_TYPE_QNAME, XS_DATETIME);
        typeMap.put(BuiltinTypeQNames.XS_DATE_TYPE_QNAME, XS_DATE);
        typeMap.put(BuiltinTypeQNames.XS_TIME_TYPE_QNAME, XS_TIME);
        typeMap.put(BuiltinTypeQNames.XS_DURATION_TYPE_QNAME, XS_DURATION);
        typeMap.put(BuiltinTypeQNames.XS_YEAR_MONTH_DURATION_TYPE_QNAME, XS_YEAR_MONTH_DURATION);
        typeMap.put(BuiltinTypeQNames.XS_DAY_TIME_DURATION_TYPE_QNAME, XS_DAY_TIME_DURATION);
        typeMap.put(BuiltinTypeQNames.XSEXT_NUMERIC_TYPE_QNAME, XSEXT_NUMERIC);
        typeMap.put(BuiltinTypeQNames.XS_FLOAT_TYPE_QNAME, XS_FLOAT);
        typeMap.put(BuiltinTypeQNames.XS_DOUBLE_TYPE_QNAME, XS_DOUBLE);
        typeMap.put(BuiltinTypeQNames.XS_DECIMAL_TYPE_QNAME, XS_DECIMAL);
        typeMap.put(BuiltinTypeQNames.XS_INTEGER_TYPE_QNAME, XS_INTEGER);
        typeMap.put(BuiltinTypeQNames.XS_NON_POSITIVE_INTEGER_TYPE_QNAME, XS_NON_POSITIVE_INTEGER);
        typeMap.put(BuiltinTypeQNames.XS_NEGATIVE_INTEGER_TYPE_QNAME, XS_NEGATIVE_INTEGER);
        typeMap.put(BuiltinTypeQNames.XS_LONG_TYPE_QNAME, XS_LONG);
        typeMap.put(BuiltinTypeQNames.XS_INT_TYPE_QNAME, XS_INT);
        typeMap.put(BuiltinTypeQNames.XS_SHORT_TYPE_QNAME, XS_SHORT);
        typeMap.put(BuiltinTypeQNames.XS_BYTE_TYPE_QNAME, XS_BYTE);
        typeMap.put(BuiltinTypeQNames.XS_NON_NEGATIVE_INTEGER_TYPE_QNAME, XS_NON_NEGATIVE_INTEGER);
        typeMap.put(BuiltinTypeQNames.XS_UNSIGNED_LONG_TYPE_QNAME, XS_UNSIGNED_LONG);
        typeMap.put(BuiltinTypeQNames.XS_UNSIGNED_INT_TYPE_QNAME, XS_UNSIGNED_INT);
        typeMap.put(BuiltinTypeQNames.XS_UNSIGNED_SHORT_TYPE_QNAME, XS_UNSIGNED_SHORT);
        typeMap.put(BuiltinTypeQNames.XS_UNSIGNED_BYTE_TYPE_QNAME, XS_UNSIGNED_BYTE);
        typeMap.put(BuiltinTypeQNames.XS_POSITIVE_INTEGER_TYPE_QNAME, XS_POSITIVE_INTEGER);
        typeMap.put(BuiltinTypeQNames.XS_G_YEAR_MONTH_TYPE_QNAME, XS_G_YEAR_MONTH);
        typeMap.put(BuiltinTypeQNames.XS_G_YEAR_TYPE_QNAME, XS_G_YEAR);
        typeMap.put(BuiltinTypeQNames.XS_G_MONTH_DAY_TYPE_QNAME, XS_G_MONTH_DAY);
        typeMap.put(BuiltinTypeQNames.XS_G_DAY_TYPE_QNAME, XS_G_DAY);
        typeMap.put(BuiltinTypeQNames.XS_G_MONTH_TYPE_QNAME, XS_G_MONTH);
        typeMap.put(BuiltinTypeQNames.XS_BOOLEAN_TYPE_QNAME, XS_BOOLEAN);
        typeMap.put(BuiltinTypeQNames.XS_BASE64_BINARY_TYPE_QNAME, XS_BASE64_BINARY);
        typeMap.put(BuiltinTypeQNames.XS_HEX_BINARY_TYPE_QNAME, XS_HEX_BINARY);
        typeMap.put(BuiltinTypeQNames.XS_ANY_URI_TYPE_QNAME, XS_ANY_URI);
        typeMap.put(BuiltinTypeQNames.XS_QNAME_TYPE_QNAME, XS_QNAME);
        typeMap.put(BuiltinTypeQNames.XS_NOTATION_TYPE_QNAME, XS_NOTATION);
        TYPE_MAP = Collections.unmodifiableMap(typeMap);
    }
}