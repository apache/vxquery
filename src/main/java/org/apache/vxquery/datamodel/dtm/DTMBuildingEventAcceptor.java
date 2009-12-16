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
package org.apache.vxquery.datamodel.dtm;

import java.nio.CharBuffer;
import java.util.Stack;

import org.apache.vxquery.datamodel.NameCache;
import org.apache.vxquery.datamodel.NodeConstructingEventAcceptor;
import org.apache.vxquery.datamodel.XDMItem;
import org.apache.vxquery.datamodel.XDMNode;
import org.apache.vxquery.datamodel.atomic.AtomicValueFactory;
import org.apache.vxquery.datamodel.atomic.QNameValue;
import org.apache.vxquery.exceptions.SystemException;

public final class DTMBuildingEventAcceptor implements NodeConstructingEventAcceptor {
    private DTM dtm;
    private Stack<NodeState> nodeStack;
    private StringBuilder textContentBuffer;
    private StringBuilder auxContentBuffer;
    private int rootNodeIndex;

    public DTMBuildingEventAcceptor(NameCache nameCache, AtomicValueFactory avf) {
        dtm = new DTM(avf, nameCache);
        nodeStack = new Stack<NodeState>();
    }

    @Override
    public void open() throws SystemException {
        nodeStack.clear();
        textContentBuffer = dtm.textContent == null ? new StringBuilder() : new StringBuilder(CharBuffer
                .wrap(dtm.textContent));
        auxContentBuffer = dtm.auxContent == null ? new StringBuilder() : new StringBuilder(CharBuffer
                .wrap(dtm.auxContent));
        rootNodeIndex = dtm.nNodes;
    }

    @Override
    public void attribute(CharSequence uri, CharSequence localName, CharSequence prefix, CharSequence stringValue)
            throws SystemException {
        dtm.ensureCapacity(1);
        int aIndex = dtm.nNodes++;
        int lastAttr = DTM.NULL;
        int owningElement = DTM.NULL;
        if (!nodeStack.isEmpty()) {
            NodeState owner = nodeStack.peek();
            lastAttr = owner.lastAttribute;
            owner.lastAttribute = aIndex;
            owningElement = owner.index;
        }
        dtm.nodeKind[aIndex] = DTM.DTM_ATTRIBUTE;
        dtm.next[aIndex] = owningElement;
        if (lastAttr != DTM.NULL) {
            dtm.next[lastAttr] = aIndex;
        }
        dtm.param0[aIndex] = auxContentBuffer.length();
        dtm.param1[aIndex] = stringValue.length();
        auxContentBuffer.append(stringValue);
        dtm.nameCode[aIndex] = dtm.nameCache.intern(prefix.toString(), uri.toString(), localName.toString());
    }

    @Override
    public void endDocument() throws SystemException {
        nodeStack.pop();
    }

    @Override
    public void endElement() throws SystemException {
        nodeStack.pop();
    }

    @Override
    public void item(XDMItem item) throws SystemException {
        switch (item.getDMOKind()) {
            case ATOMIC_VALUE:
                text(item.getStringValue());
                break;

            case ATTRIBUTE_NODE: {
                XDMNode aNode = (XDMNode) item;
                QNameValue name = aNode.getNodeName();
                NameCache cache = name.getNameCache();
                int code = name.getCode();
                attribute(cache.getUri(code), cache.getLocalName(code), cache.getPrefix(code), aNode.getStringValue());
                break;
            }

            case COMMENT_NODE:
                XDMNode cNode = (XDMNode) item;
                comment(cNode.getStringValue());
                break;

            case PI_NODE:
                XDMNode pNode = (XDMNode) item;
                QNameValue name = pNode.getNodeName();
                NameCache cache = name.getNameCache();
                int code = name.getCode();
                pi(cache.getLocalName(code), pNode.getStringValue());
                break;

            case TEXT_NODE:
                XDMNode tNode = (XDMNode) item;
                text(tNode.getStringValue());
                break;

            case DOCUMENT_NODE:
            case ELEMENT_NODE:
                if (item instanceof DTMNodeImpl) {
                    NodeState parentNodeState = null;
                    int nIndex = dtm.nNodes;
                    if (!nodeStack.isEmpty()) {
                        parentNodeState = nodeStack.peek();
                        if (parentNodeState.lastChild != DTM.NULL) {
                            dtm.next[parentNodeState.lastChild] = nIndex;
                        } else {
                            dtm.param1[parentNodeState.index] = nIndex;
                        }
                        parentNodeState.lastChild = nIndex;
                    }
                    DTMNodeImpl dNode = (DTMNodeImpl) item;
                    dtm.copyNode(dNode.getDTM(), dNode.getDTMIndex(), auxContentBuffer, textContentBuffer);
                    dtm.next[nIndex] = parentNodeState == null ? -1 : parentNodeState.index;
                } else {
                    throw new UnsupportedOperationException();
                }
                break;
        }
    }

    @Override
    public void namespace(CharSequence prefix, CharSequence uri) throws SystemException {
    }

    @Override
    public void startDocument() throws SystemException {
        dtm.ensureCapacity(1);
        int nIndex = dtm.nNodes++;
        NodeState nodeState = new NodeState(nIndex);
        nodeStack.push(nodeState);

        dtm.nodeKind[nIndex] = DTM.DTM_DOCUMENT;
        dtm.next[nIndex] = DTM.NULL;
        dtm.param0[nIndex] = DTM.NULL;
        dtm.param1[nIndex] = DTM.NULL;
        dtm.nameCode[nIndex] = DTM.NULL;
    }

    @Override
    public void startElement(CharSequence uri, CharSequence localName, CharSequence prefix) throws SystemException {
        dtm.ensureCapacity(1);
        int nIndex = dtm.nNodes++;
        NodeState nodeState = new NodeState(nIndex);
        NodeState parentNodeState = null;
        if (!nodeStack.isEmpty()) {
            parentNodeState = nodeStack.peek();
            if (parentNodeState.lastChild != DTM.NULL) {
                dtm.next[parentNodeState.lastChild] = nIndex;
            }
            parentNodeState.lastChild = nIndex;
        }
        nodeStack.push(nodeState);

        dtm.nodeKind[nIndex] = DTM.DTM_ELEMENT;
        dtm.next[nIndex] = parentNodeState == null ? DTM.NULL : parentNodeState.index;
        dtm.param0[nIndex] = DTM.NULL;
        if (parentNodeState != null && dtm.param1[parentNodeState.index] == DTM.NULL) {
            dtm.param1[parentNodeState.index] = nIndex;
        }
        dtm.param1[nIndex] = DTM.NULL;
        dtm.nameCode[nIndex] = dtm.nameCache.intern(prefix.toString(), uri.toString(), localName.toString());
    }

    @Override
    public void comment(CharSequence content) throws SystemException {
        dtm.ensureCapacity(1);
        int nIndex = dtm.nNodes++;
        NodeState parentNodeState = null;
        if (!nodeStack.isEmpty()) {
            parentNodeState = nodeStack.peek();
            if (parentNodeState.lastChild != DTM.NULL) {
                dtm.next[parentNodeState.lastChild] = nIndex;
            }
            parentNodeState.lastChild = nIndex;
            if (dtm.param1[parentNodeState.index] == DTM.NULL) {
                dtm.param1[parentNodeState.index] = nIndex;
            }
        }

        dtm.nodeKind[nIndex] = DTM.DTM_COMMENT;
        dtm.next[nIndex] = parentNodeState == null ? DTM.NULL : parentNodeState.index;
        dtm.param0[nIndex] = auxContentBuffer.length();
        dtm.param1[nIndex] = content.length();
        auxContentBuffer.append(content);
        dtm.nameCode[nIndex] = DTM.NULL;
    }

    @Override
    public void pi(CharSequence target, CharSequence content) throws SystemException {
        dtm.ensureCapacity(1);
        int nIndex = dtm.nNodes++;
        NodeState parentNodeState = null;
        if (!nodeStack.isEmpty()) {
            parentNodeState = nodeStack.peek();
            if (parentNodeState.lastChild != DTM.NULL) {
                dtm.next[parentNodeState.lastChild] = nIndex;
            }
            parentNodeState.lastChild = nIndex;
            if (dtm.param1[parentNodeState.index] == DTM.NULL) {
                dtm.param1[parentNodeState.index] = nIndex;
            }
        }

        dtm.nodeKind[nIndex] = DTM.DTM_PI;
        dtm.next[nIndex] = parentNodeState == null ? DTM.NULL : parentNodeState.index;
        dtm.param0[nIndex] = auxContentBuffer.length();
        dtm.param1[nIndex] = content.length();
        auxContentBuffer.append(content);
        dtm.nameCode[nIndex] = dtm.nameCache.intern("", "", target.toString());
    }

    @Override
    public void text(CharSequence stringValue) throws SystemException {
        dtm.ensureCapacity(1);
        int nIndex = dtm.nNodes++;
        NodeState parentNodeState = null;
        if (!nodeStack.isEmpty()) {
            parentNodeState = nodeStack.peek();
            if (parentNodeState.lastChild != DTM.NULL) {
                dtm.next[parentNodeState.lastChild] = nIndex;
            }
            parentNodeState.lastChild = nIndex;
            if (dtm.param1[parentNodeState.index] == DTM.NULL) {
                dtm.param1[parentNodeState.index] = nIndex;
            }
        }

        dtm.nodeKind[nIndex] = DTM.DTM_TEXT;
        dtm.next[nIndex] = parentNodeState == null ? DTM.NULL : parentNodeState.index;
        dtm.param0[nIndex] = textContentBuffer.length();
        dtm.param1[nIndex] = stringValue.length();
        textContentBuffer.append(stringValue);
        dtm.nameCode[nIndex] = DTM.NULL;
    }

    @Override
    public void close() {
        dtm.auxContent = new char[auxContentBuffer.length()];
        auxContentBuffer.getChars(0, dtm.auxContent.length, dtm.auxContent, 0);
        dtm.textContent = new char[textContentBuffer.length()];
        textContentBuffer.getChars(0, dtm.textContent.length, dtm.textContent, 0);
    }

    @Override
    public XDMNode getConstructedNode() {
        return new DTMNodeImpl(dtm, rootNodeIndex);
    }
}