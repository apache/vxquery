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

public final class DocumentType extends AbstractNodeType {
    public static final DocumentType ANYDOCUMENT = new DocumentType(ElementType.ANYELEMENT);

    private ElementType elementType;

    public DocumentType(ElementType elementType) {
        this.elementType = elementType;
    }

    @Override
    public NodeKind getNodeKind() {
        return NodeKind.DOCUMENT;
    }

    public ElementType getElementType() {
        return elementType;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("document(");
        sb.append(elementType != null ? elementType : "*");
        return sb.append(")").toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((elementType == null) ? 0 : elementType.hashCode());
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
        DocumentType other = (DocumentType) obj;
        if (elementType == null) {
            if (other.elementType != null)
                return false;
        } else if (!elementType.equals(other.elementType))
            return false;
        return true;
    }
}
