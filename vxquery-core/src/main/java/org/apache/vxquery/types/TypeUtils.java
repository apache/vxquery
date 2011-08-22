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

import org.apache.vxquery.context.RootStaticContextImpl;
import org.apache.vxquery.context.StaticContext;

public class TypeUtils {
    public static SequenceType createSequenceType(String str) {
        return createSequenceType(RootStaticContextImpl.INSTANCE, str);
    }

    public static SequenceType createSequenceType(StaticContext sCtx, String str) {
        str = str.trim();
        Quantifier q = Quantifier.QUANT_ONE;
        if (str.endsWith("?")) {
            q = Quantifier.QUANT_QUESTION;
            str = str.substring(0, str.length() - 1);
        } else if (str.endsWith("*")) {
            q = Quantifier.QUANT_STAR;
            str = str.substring(0, str.length() - 1);
        } else if (str.endsWith("+")) {
            q = Quantifier.QUANT_PLUS;
            str = str.substring(0, str.length() - 1);
        }

        ItemType it;
        if (str.equals("item()")) {
            it = AnyItemType.INSTANCE;
        } else if (str.equals("node()")) {
            it = AnyNodeType.INSTANCE;
        } else if (str.equals("document-node()")) {
            it = DocumentType.ANYDOCUMENT;
        } else if (str.equals("none")) {
            it = NoneType.INSTANCE;
        } else if (str.equals("element()")) {
            it = ElementType.ANYELEMENT;
        } else if (str.equals("empty-sequence()")) {
            it = EmptySequenceType.INSTANCE;
        } else {
            int idx = str.indexOf(':');
            if (idx < 0) {
                throw new IllegalStateException("QName has no prefix: " + str);
            }
            String prefix = str.substring(0, idx);
            String local = str.substring(idx + 1);
            String uri = sCtx.lookupNamespaceUri(prefix);
            if (uri == null) {
                throw new IllegalStateException("Prefix has no URI mapping: " + prefix);
            }
            QName qname = new QName(uri, local, prefix);
            it = (ItemType) sCtx.lookupSchemaType(qname);
            if (it == null) {
                throw new IllegalStateException("No type found: " + qname);
            }
        }
        return SequenceType.create(it, q);
    }

    public static boolean isSubtypeTypeOf(AtomicType subType, AtomicType superType) {
        while (true) {
            if (subType.getTypeId() == superType.getTypeId()) {
                return true;
            }
            if (subType.getTypeId() == BuiltinTypeConstants.XS_ANY_ATOMIC_TYPE_ID) {
                return false;
            }
            subType = (AtomicType) subType.getBaseType();
        }
    }
}