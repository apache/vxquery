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
    public String toString() {
        StringBuilder sb = new StringBuilder("attribute(");
        sb.append(nameTest != null ? nameTest : "*");
        if (contentType != null) {
            sb.append(", ").append(contentType);
        }
        return sb.append(")").toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((contentType == null) ? 0 : contentType.hashCode());
        result = prime * result + ((nameTest == null) ? 0 : nameTest.hashCode());
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
        AttributeType other = (AttributeType) obj;
        if (contentType == null) {
            if (other.contentType != null)
                return false;
        } else if (!contentType.equals(other.contentType))
            return false;
        if (nameTest == null) {
            if (other.nameTest != null)
                return false;
        } else if (!nameTest.equals(other.nameTest))
            return false;
        return true;
    }
}