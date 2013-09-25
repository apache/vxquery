/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.vxquery.types;

import java.util.Arrays;

public class TypeOperations {
    public static Quantifier quantifier(XQType type) {
        if (type instanceof QuantifiedType) {
            return ((QuantifiedType) type).getQuantifier();
        } else if (type instanceof ItemType) {
            return Quantifier.QUANT_ONE;
        }
        return Quantifier.QUANT_STAR;
    }

    public static XQType primeType(XQType type) {
        if (type instanceof QuantifiedType) {
            return ((QuantifiedType) type).getContentType();
        } else if (type instanceof ItemType) {
            return type;
        }
        return AnyItemType.INSTANCE;
    }

    public static XQType quantified(XQType t, Quantifier q) {
        return Quantifier.QUANT_ONE.equals(q) ? t : new QuantifiedType(t, q);
    }

    public static XQType union(XQType... types) {
        return new ComposedType(Arrays.asList(types), Composer.UNION);
    }

    public static XQType sequence(XQType... types) {
        return new ComposedType(Arrays.asList(types), Composer.SEQUENCE);
    }

    public static XQType shuffle(XQType... types) {
        return new ComposedType(Arrays.asList(types), Composer.SHUFFLE);
    }

    public static XQType intersect(XQType t1, XQType t2) {
        return AnyItemType.INSTANCE;
    }

    public static boolean isSubtypeOf(XQType subtype, XQType supertype) {
        if (subtype instanceof SchemaType && supertype instanceof SchemaType) {
            SchemaType subSchType = (SchemaType) subtype;
            SchemaType supSchType = (SchemaType) supertype;
            SchemaType temp = subSchType;
            while (temp != null) {
                if (temp.getTypeId() == supSchType.getTypeId()) {
                    return true;
                }
                temp = temp.getBaseType();
            }
            return false;
        } else if (supertype instanceof AnyItemType && subtype instanceof ItemType) {
            return true;
        } else if (subtype instanceof QuantifiedType || supertype instanceof QuantifiedType) {
            XQType pSubType = primeType(subtype);
            XQType pSupType = primeType(supertype);
            Quantifier subQuant = quantifier(subtype);
            Quantifier supQuant = quantifier(supertype);

            boolean isSubtype = isSubtypeOf(pSubType, pSupType);
            if (isSubtype) {
                return supQuant.isSubQuantifier(subQuant);
            }
        }

        return false;
    }
}