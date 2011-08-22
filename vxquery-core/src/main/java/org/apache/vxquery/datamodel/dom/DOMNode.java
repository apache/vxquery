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
package org.apache.vxquery.datamodel.dom;

import org.apache.vxquery.datamodel.DMOKind;
import org.apache.vxquery.datamodel.Wrapper;
import org.apache.vxquery.datamodel.XDMNode;
import org.apache.vxquery.datamodel.XDMValue;
import org.apache.vxquery.datamodel.atomic.AnyUriValue;
import org.apache.vxquery.datamodel.atomic.QNameValue;
import org.apache.vxquery.runtime.base.CloseableIterator;
import org.w3c.dom.Attr;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

final class DOMNode implements XDMNode, Wrapper<Node> {
    private DOMDocumentManager manager;
    private Node node;
    private DOMNode parent;
    private QNameValue name;

    DOMNode(DOMDocumentManager manager, Node node, DOMNode parent) {
        this.manager = manager;
        this.node = node;
        this.parent = parent;
        name = null;
    }

    @Override
    public Object getImplementationIdentifier() {
        return DOMNode.class;
    }

    static DOMNode wrap(DOMDocumentManager manager, Node node, DOMNode parent) {
        switch (node.getNodeType()) {
            case Node.DOCUMENT_NODE:
            case Node.DOCUMENT_FRAGMENT_NODE:
                return manager.getDocumentDOMNode();

            case Node.ELEMENT_NODE:
            case Node.ATTRIBUTE_NODE:
            case Node.COMMENT_NODE:
            case Node.PROCESSING_INSTRUCTION_NODE:
            case Node.TEXT_NODE:
                return new DOMNode(manager, node, parent);
        }
        throw new IllegalArgumentException(String.valueOf(node));
    }

    @Override
    public int compareDocumentOrder(XDMNode other) {
        return 0;
    }

    @Override
    public AnyUriValue getBaseUri() {
        return manager.getBaseUri();
    }

    @Override
    public AnyUriValue getDocumentUri() {
        return manager.getDocumentUri();
    }

    @Override
    public boolean getIsNilled() {
        return false;
    }

    @Override
    public QNameValue getNodeName() {
        if (name == null) {
            int nameCode = -1;
            switch (node.getNodeType()) {
                case Node.ATTRIBUTE_NODE:
                case Node.ELEMENT_NODE:
                    String prefix = node.getPrefix();
                    if (prefix == null) {
                        prefix = "";
                    }
                    nameCode = manager.getNameCache().intern(prefix, getUri(), getLocalName());
                    break;

                case Node.PROCESSING_INSTRUCTION_NODE:
                    nameCode = manager.getNameCache().intern("", "", getLocalName());
                    break;
            }
            name = manager.getAtomizedValueFactory().createQName(manager.getNameCache(), nameCode);
        }
        return name;
    }

    private String getUri() {
        short nodeType = node.getNodeType();
        if (nodeType != Node.ATTRIBUTE_NODE && nodeType != Node.ELEMENT_NODE) {
            return "";
        }
        return node.getNamespaceURI();
    }

    private String getLocalName() {
        switch (node.getNodeType()) {
            case Node.ELEMENT_NODE:
            case Node.ATTRIBUTE_NODE:
                String n = node.getLocalName();
                if (n != null) {
                    return n;
                }
                // fall-through
            case Node.PROCESSING_INSTRUCTION_NODE:
                n = node.getNodeName();
                int idx = n.indexOf(':');
                if (idx >= 0) {
                    n = n.substring(idx + 1);
                }
                return n;
        }
        return null;
    }

    @Override
    public XDMNode getParent() {
        if (parent == null) {
            switch (node.getNodeType()) {
                case Node.ATTRIBUTE_NODE:
                    parent = wrap(manager, ((Attr) node).getOwnerElement(), null);
                    break;

                default:
                    Node parentNode = node.getParentNode();
                    if (parentNode != null) {
                        parent = wrap(manager, node.getParentNode(), null);
                    }
            }
        }
        return parent;
    }

    public XDMValue getTypedValue() {
        switch (node.getNodeType()) {
            case Node.COMMENT_NODE:
            case Node.PROCESSING_INSTRUCTION_NODE:
                return manager.getAtomizedValueFactory().createString(getStringValue());
        }
        return manager.getAtomizedValueFactory().createUntypedAtomic(getStringValue());
    }

    @Override
    public boolean isID() {
        return false;
    }

    @Override
    public boolean isIDREFS() {
        return false;
    }

    @Override
    public boolean isSameNode(XDMNode other) {
        if (other instanceof DOMNode) {
            return node.equals(((DOMNode) other).node);
        }
        return false;
    }

    @Override
    public CharSequence getStringValue() {
        switch (node.getNodeType()) {
            case Node.DOCUMENT_FRAGMENT_NODE:
            case Node.DOCUMENT_NODE:
            case Node.ELEMENT_NODE: {
                NodeList children = node.getChildNodes();
                StringBuilder sb = new StringBuilder();
                appendStringValue(sb, children);
                return sb;
            }

            case Node.ATTRIBUTE_NODE:
                return ((Attr) node).getValue();

            case Node.TEXT_NODE:
                Node nextSibling = node.getNextSibling();
                if (nextSibling == null || nextSibling.getNodeType() != Node.TEXT_NODE) {
                    return node.getNodeValue();
                } else {
                    Node curr = node;
                    StringBuilder sb = new StringBuilder();
                    while (curr != null && curr.getNodeType() == Node.TEXT_NODE) {
                        sb.append(curr.getNodeValue());
                        curr = curr.getNextSibling();
                    }
                    return sb;
                }

            case Node.COMMENT_NODE:
            case Node.PROCESSING_INSTRUCTION_NODE:
                return node.getNodeValue();
        }
        return "";
    }

    private void appendStringValue(StringBuilder buffer, NodeList list) {
        int len = list.getLength();
        for (int i = 0; i < len; ++i) {
            Node n = list.item(i);
            switch (n.getNodeType()) {
                case Node.ELEMENT_NODE:
                    appendStringValue(buffer, n.getChildNodes());

                case Node.COMMENT_NODE:
                case Node.PROCESSING_INSTRUCTION_NODE:
                    break;

                default:
                    buffer.append(n.getNodeValue());
            }
        }
    }

    @Override
    public DMOKind getDMOKind() {
        switch (node.getNodeType()) {
            case Node.DOCUMENT_FRAGMENT_NODE:
            case Node.DOCUMENT_NODE:
                return DMOKind.DOCUMENT_NODE;

            case Node.ELEMENT_NODE:
                return DMOKind.ELEMENT_NODE;

            case Node.ATTRIBUTE_NODE:
                return DMOKind.ATTRIBUTE_NODE;

            case Node.COMMENT_NODE:
                return DMOKind.COMMENT_NODE;

            case Node.PROCESSING_INSTRUCTION_NODE:
                return DMOKind.PI_NODE;

            case Node.TEXT_NODE:
                return DMOKind.TEXT_NODE;
        }
        throw new IllegalStateException();
    }

    @Override
    public Node getWrappedObject() {
        return node;
    }

    @Override
    public boolean hasAttributes() {
        return node.hasAttributes();
    }

    @Override
    public boolean hasChildren() {
        return node.hasChildNodes();
    }

    @Override
    public CloseableIterator getAttributes() {
        return new DOMAttributeIterator(this);
    }

    @Override
    public CloseableIterator getChildren() {
        return new DOMChildrenIterator(this);
    }

    @Override
    public CloseableIterator getFollowingSiblings() {
        return new DOMFollowingSiblingsIterator(this);
    }

    @Override
    public CloseableIterator getPrecedingSiblings() {
        return new DOMPrecedingSiblingsIterator(this);
    }

    DOMDocumentManager getDOMManager() {
        return manager;
    }
}