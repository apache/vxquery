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

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class SequenceType implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final Map<ItemType, SequenceType[]> BUILTIN_SEQ_TYPES;

    private ItemType itemType;
    private Quantifier quantifier;

    static {
        Map<ItemType, SequenceType[]> types = new LinkedHashMap<>();

        createBuiltinEntry(types, BuiltinTypeRegistry.XS_ANY_ATOMIC);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_STRING);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_NORMALIZED_STRING);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_TOKEN);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_LANGUAGE);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_NMTOKEN);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_NAME);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_NCNAME);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_ID);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_IDREF);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_ENTITY);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_UNTYPED_ATOMIC);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_DATETIME);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_DATE);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_TIME);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_DURATION);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_YEAR_MONTH_DURATION);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_DAY_TIME_DURATION);
        createBuiltinEntry(types, BuiltinTypeRegistry.XSEXT_NUMERIC);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_FLOAT);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_DOUBLE);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_DECIMAL);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_INTEGER);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_NON_POSITIVE_INTEGER);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_NEGATIVE_INTEGER);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_LONG);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_LONG);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_INT);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_SHORT);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_BYTE);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_NON_NEGATIVE_INTEGER);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_UNSIGNED_LONG);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_UNSIGNED_INT);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_UNSIGNED_SHORT);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_UNSIGNED_BYTE);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_POSITIVE_INTEGER);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_G_YEAR_MONTH);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_G_YEAR);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_G_MONTH_DAY);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_G_DAY);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_G_MONTH);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_BOOLEAN);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_BASE64_BINARY);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_HEX_BINARY);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_ANY_URI);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_QNAME);
        createBuiltinEntry(types, BuiltinTypeRegistry.XS_NOTATION);
        createBuiltinEntry(types, BuiltinTypeRegistry.JS_NULL);

        createBuiltinEntry(types, AnyItemType.INSTANCE);
        createBuiltinEntry(types, AnyNodeType.INSTANCE);
        createBuiltinEntry(types, DocumentType.ANYDOCUMENT);
        createBuiltinEntry(types, ElementType.ANYELEMENT);
        createBuiltinEntry(types, AttributeType.ANYATTRIBUTE);
        createBuiltinEntry(types, CommentType.INSTANCE);
        createBuiltinEntry(types, ProcessingInstructionType.ANYPI);

        createBuiltinEntry(types, ArrayType.INSTANCE);
        createBuiltinEntry(types, ObjectType.INSTANCE);

        BUILTIN_SEQ_TYPES = Collections.unmodifiableMap(types);
    }

    private SequenceType(ItemType itemType, Quantifier quantifier) {
        this.itemType = itemType;
        this.quantifier = quantifier;
    }

    private static void createBuiltinEntry(Map<ItemType, SequenceType[]> types, ItemType itemType) {
        types.put(itemType,
                new SequenceType[] { new SequenceType(itemType, Quantifier.QUANT_ZERO),
                        new SequenceType(itemType, Quantifier.QUANT_ONE),
                        new SequenceType(itemType, Quantifier.QUANT_QUESTION),
                        new SequenceType(itemType, Quantifier.QUANT_STAR),
                        new SequenceType(itemType, Quantifier.QUANT_PLUS), });
    }

    public static SequenceType create(ItemType itemType, Quantifier quantifier) {
        SequenceType[] types = BUILTIN_SEQ_TYPES.get(itemType);
        if (types == null) {
            return new SequenceType(itemType, quantifier);
        }
        return types[quantifier.ordinal()];
    }

    public ItemType getItemType() {
        return itemType;
    }

    public Quantifier getQuantifier() {
        return quantifier;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((itemType == null) ? 0 : itemType.hashCode());
        result = prime * result + ((quantifier == null) ? 0 : quantifier.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SequenceType other = (SequenceType) obj;
        if (itemType == null) {
            if (other.itemType != null)
                return false;
        } else if (!itemType.equals(other.itemType))
            return false;
        if (quantifier != other.quantifier)
            return false;
        return true;
    }

    public String toString() {
        return String.valueOf(itemType) + Quantifier.toString(quantifier);
    }
}
