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
package org.apache.vxquery.v0datamodel;

import org.apache.vxquery.types.NodeKind;

public enum DMOKind {
    ATOMIC_VALUE(true, false, false, null), DOCUMENT_NODE(true, true, false, NodeKind.DOCUMENT), ELEMENT_NODE(true,
            true, true, NodeKind.ELEMENT), ATTRIBUTE_NODE(true, true, true, NodeKind.ATTRIBUTE), TEXT_NODE(true, true,
            false, NodeKind.TEXT), COMMENT_NODE(true, true, false, NodeKind.COMMENT), PI_NODE(true, true, true,
            NodeKind.PI), SEQUENCE(false, false, false, null);

    private final boolean isItem;
    private final boolean isNode;
    private final boolean hasNodeName;
    private final NodeKind nodeKind;

    private DMOKind(boolean isItem, boolean isNode, boolean hasNodeName, NodeKind nodeKind) {
        this.isItem = isItem;
        this.isNode = isNode;
        this.hasNodeName = hasNodeName;
        this.nodeKind = nodeKind;
    }

    public boolean isItem() {
        return isItem;
    }

    public boolean isNode() {
        return isNode;
    }

    public NodeKind getNodeKind() {
        return nodeKind;
    }

    public boolean hasNodeName() {
        return hasNodeName;
    }
}