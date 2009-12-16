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

import org.apache.vxquery.datamodel.DMOKind;
import org.apache.vxquery.datamodel.XDMNode;
import org.apache.vxquery.datamodel.XDMValue;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.util.Filter;

public final class ElementType extends AbstractNodeType {
    public static final ElementType ANYELEMENT = new ElementType(NameTest.STAR_NAMETEST, AnyType.INSTANCE, true);

    private NameTest nameTest;
    private SchemaType contentType;
    private boolean nilled;

    public ElementType(NameTest nameTest, SchemaType contentType, boolean nilled) {
        this.nameTest = nameTest;
        this.contentType = contentType;
        this.nilled = nilled;
    }

    @Override
    public NodeKind getNodeKind() {
        return NodeKind.ELEMENT;
    }

    public NameTest getNameTest() {
        return nameTest;
    }

    public SchemaType getContentType() {
        return contentType;
    }

    public boolean isNilled() {
        return nilled;
    }

    @Override
    public Filter<XDMValue> createInstanceOfFilter() {
        final Filter<XDMNode> nameTestMatchFilter = nameTest.createNameMatchFilter();
        return new Filter<XDMValue>() {
            @Override
            public boolean accept(XDMValue value) throws SystemException {
                if (value == null) {
                    return false;
                }
                if (value.getDMOKind() != DMOKind.ELEMENT_NODE) {
                    return false;
                }
                return nameTestMatchFilter.accept((XDMNode) value);
            }
        };
    }
}