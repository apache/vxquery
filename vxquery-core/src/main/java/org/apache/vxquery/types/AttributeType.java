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

import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.util.Filter;
import org.apache.vxquery.v0datamodel.DMOKind;
import org.apache.vxquery.v0datamodel.XDMNode;
import org.apache.vxquery.v0datamodel.XDMValue;

public final class AttributeType extends AbstractNodeType {
    public static final AttributeType ANYATTRIBUTE = new AttributeType(NameTest.STAR_NAMETEST,
            BuiltinTypeRegistry.XS_ANY_ATOMIC);

    private NameTest nameTest;
    private SchemaType contentType;

    public AttributeType(NameTest nameTest, SchemaType contentType) {
        this.nameTest = nameTest;
        this.contentType = contentType;
    }

    @Override
    public NodeKind getNodeKind() {
        return NodeKind.ATTRIBUTE;
    }

    public NameTest getNameTest() {
        return nameTest;
    }

    public SchemaType getContentType() {
        return contentType;
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
                if (value.getDMOKind() != DMOKind.ATTRIBUTE_NODE) {
                    return false;
                }
                return nameTestMatchFilter.accept((XDMNode) value);
            }
        };
    }
}