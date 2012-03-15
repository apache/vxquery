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
package org.apache.vxquery.v0datamodel.dtm;

import java.util.Arrays;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicLong;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Source;

import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.v0datamodel.NameCache;
import org.apache.vxquery.v0datamodel.atomic.AtomicValueFactory;

public final class DTM {
    public static final byte DTM_DOCUMENT = 0;
    public static final byte DTM_ELEMENT = 1;
    public static final byte DTM_ATTRIBUTE = 2;
    public static final byte DTM_TEXT = 3;
    public static final byte DTM_COMMENT = 4;
    public static final byte DTM_PI = 5;
    public static final byte DTM_NAMESPACE = 6;

    private static final int INITIAL_SIZE = 1024;
    private static final int DOUBLE_LIMIT = 1048576;
    private static final int LINEAR_INCREMENT = 16384;

    static final int NULL = -1;

    /* used to create unique ids for DTMs */
    private static AtomicLong counter = new AtomicLong(-1);
    
    private final long id;
    
    int nNodes;

    /**
     * Indicates the kind of node.
     */
    byte[] nodeKind;

    /**
     * Index of the next sibling of this node.
     */
    int[] next;

    /**
     * Index of the previous sibling of this node. This is constructed on
     * demand.
     */
    int[] previous;

    /**
     * The interpretation of this value is dependant on the value of nodeKind.
     * For DTM_DOCUMENT: - Not used: NULL For DTM_ELEMENT: - Index of first
     * namespace For DTM_ATTRIBUTE: - Start index of content in the text buffer
     * For DTM_TEXT: - Start index of content in the text buffer For
     * DTM_COMMENT: - Start index of content in the text buffer For DTM_PI: -
     * Start index of value in the text buffer
     */
    int[] param0;

    /**
     * The interpretation of this value is dependant on the value of nodeKind.
     * For DTM_DOCUMENT: - Not used: NULL For DTM_ELEMENT: - Index of first
     * child For DTM_ATTRIBUTE: - Length of content in the text buffer For
     * DTM_TEXT: - Length of content in the text buffer For DTM_COMMENT: -
     * Length of content in the text buffer For DTM_PI: - Length of value in the
     * text buffer
     */
    int[] param1;

    /**
     * Interned code of the name of the node. For nameless nodes, this is NULL.
     */
    int[] nameCode;

    /**
     * Atomic Value Factory
     */
    private AtomicValueFactory avf;

    /**
     * Name pool used to intern QNames for this DTM.
     */
    NameCache nameCache;

    /**
     * Text node content buffer.
     */
    char[] textContent;

    /**
     * Attribute / Comment / PI content buffer
     */
    char[] auxContent;

    public DTM(AtomicValueFactory avf, NameCache nameCache) {
        this(INITIAL_SIZE, avf, nameCache);
    }

    public DTM(int initialSize, AtomicValueFactory avf, NameCache nameCache) {
        id = counter.incrementAndGet();
        nNodes = 0;
        nodeKind = new byte[initialSize];
        next = new int[initialSize];
        param0 = new int[initialSize];
        param1 = new int[initialSize];
        nameCode = new int[initialSize];
        this.avf = avf;
        this.nameCache = nameCache;
    }
    
    public final long getId() {
        return id;
    }

    public DTMNodeImpl parse(Source source, boolean preserveWhitespace) throws SystemException {
        nNodes = 0;
        XMLInputFactory xif = XMLInputFactory.newInstance();
        xif.setProperty(XMLInputFactory.IS_COALESCING, true);
        try {
            XMLStreamReader xsr = xif.createXMLStreamReader(source);
            ParsingState state = new ParsingState(xsr);
            addDocumentNode(state);
            while (xsr.hasNext()) {
                switch (xsr.next()) {
                    case XMLStreamConstants.CDATA:
                    case XMLStreamConstants.CHARACTERS:
                    case XMLStreamConstants.SPACE:
                        if (preserveWhitespace || !xsr.isWhiteSpace()) {
                            addTextNode(state);
                        }
                        break;

                    case XMLStreamConstants.COMMENT:
                        addCommentNode(state);
                        break;

                    case XMLStreamConstants.PROCESSING_INSTRUCTION:
                        addPINode(state);
                        break;

                    case XMLStreamConstants.START_DOCUMENT:
                        addDocumentNode(state);
                        break;

                    case XMLStreamConstants.START_ELEMENT:
                        addElementNode(state);
                        break;

                    case XMLStreamConstants.END_DOCUMENT:
                    case XMLStreamConstants.END_ELEMENT:
                        popNodeStack(state);
                        break;

                    case XMLStreamConstants.ENTITY_DECLARATION:
                        System.err.println("Encountered: ENTITY_DECLARATION");
                        break;

                    case XMLStreamConstants.DTD:
                        System.err.println("Encountered: DTD");
                        break;

                    case XMLStreamConstants.ENTITY_REFERENCE:
                        System.err.println("Encountered: ENTITY_REFERENCE");
                        break;

                    case XMLStreamConstants.NAMESPACE:
                        System.err.println("Encountered: NAMESPACE");
                        break;

                    case XMLStreamConstants.NOTATION_DECLARATION:
                        System.err.println("Encountered: NOTATION_DECLARATION");
                        break;

                    default:
                        throw new IllegalStateException();
                }
            }
            textContent = new char[state.textContentBuffer.length()];
            state.textContentBuffer.getChars(0, textContent.length, textContent, 0);
            auxContent = new char[state.auxContentBuffer.length()];
            state.auxContentBuffer.getChars(0, auxContent.length, auxContent, 0);
            return new DTMNodeImpl(this, 0);
        } catch (XMLStreamException e) {
            throw new SystemException(ErrorCode.TODO, e);
        }
    }

    private void popNodeStack(ParsingState state) {
        state.nodeStack.pop();
    }

    private void addElementNode(ParsingState state) throws XMLStreamException {
        ensureCapacity(1);
        int nIndex = nNodes++;
        NodeState nodeState = new NodeState(nIndex);
        NodeState parentNodeState = null;
        if (!state.nodeStack.isEmpty()) {
            parentNodeState = state.nodeStack.peek();
            if (parentNodeState.lastChild != NULL) {
                next[parentNodeState.lastChild] = nIndex;
            }
            parentNodeState.lastChild = nIndex;
        }
        state.nodeStack.push(nodeState);

        nodeKind[nIndex] = DTM_ELEMENT;
        next[nIndex] = parentNodeState == null ? NULL : parentNodeState.index;
        param0[nIndex] = NULL;
        if (parentNodeState != null && param1[parentNodeState.index] == NULL) {
            param1[parentNodeState.index] = nIndex;
        }
        param1[nIndex] = NULL;
        QName nName = state.xsr.getName();
        nameCode[nIndex] = nameCache.intern(nName.getPrefix(), nName.getNamespaceURI(), nName.getLocalPart());

        int nAttrs = state.xsr.getAttributeCount();
        if (nAttrs > 0) {
            ensureCapacity(nAttrs);
            for (int i = 0; i < nAttrs; ++i) {
                int aIndex = nNodes++;
                nodeKind[aIndex] = DTM_ATTRIBUTE;
                next[aIndex] = (i < nAttrs - 1) ? (aIndex + 1) : nIndex;
                String aContent = state.xsr.getAttributeValue(i);
                param0[aIndex] = state.auxContentBuffer.length();
                param1[aIndex] = aContent.length();
                state.auxContentBuffer.append(aContent);
                QName aName = state.xsr.getAttributeName(i);
                nameCode[aIndex] = nameCache.intern(aName.getPrefix(), aName.getNamespaceURI(), aName.getLocalPart());
            }
        }
    }

    private void addDocumentNode(ParsingState state) {
        ensureCapacity(1);
        int nIndex = nNodes++;
        NodeState nodeState = new NodeState(nIndex);
        state.nodeStack.push(nodeState);

        nodeKind[nIndex] = DTM_DOCUMENT;
        next[nIndex] = NULL;
        param0[nIndex] = NULL;
        param1[nIndex] = NULL;
        nameCode[nIndex] = NULL;
    }

    private void addTextNode(ParsingState state) {
        ensureCapacity(1);
        int nIndex = nNodes++;
        NodeState parentNodeState = state.nodeStack.peek();
        if (parentNodeState.lastChild != NULL) {
            next[parentNodeState.lastChild] = nIndex;
        }
        parentNodeState.lastChild = nIndex;

        nodeKind[nIndex] = DTM_TEXT;
        next[nIndex] = parentNodeState.index;
        String nContent = state.xsr.getText();
        param0[nIndex] = state.textContentBuffer.length();
        if (param1[parentNodeState.index] == NULL) {
            param1[parentNodeState.index] = nIndex;
        }
        param1[nIndex] = nContent.length();
        state.textContentBuffer.append(nContent);
        nameCode[nIndex] = NULL;
    }

    private void addCommentNode(ParsingState state) {
        ensureCapacity(1);
        int nIndex = nNodes++;
        NodeState parentNodeState = state.nodeStack.peek();
        if (parentNodeState.lastChild != NULL) {
            next[parentNodeState.lastChild] = nIndex;
        }
        parentNodeState.lastChild = nIndex;

        nodeKind[nIndex] = DTM_COMMENT;
        next[nIndex] = parentNodeState.index;
        String nContent = state.xsr.getText();
        param0[nIndex] = state.auxContentBuffer.length();
        if (param1[parentNodeState.index] == NULL) {
            param1[parentNodeState.index] = nIndex;
        }
        param1[nIndex] = nContent.length();
        state.auxContentBuffer.append(nContent);
        nameCode[nIndex] = NULL;
    }

    private void addPINode(ParsingState state) {
        ensureCapacity(1);
        int nIndex = nNodes++;
        NodeState parentNodeState = state.nodeStack.peek();
        if (parentNodeState.lastChild != NULL) {
            next[parentNodeState.lastChild] = nIndex;
        }
        parentNodeState.lastChild = nIndex;

        nodeKind[nIndex] = DTM_PI;
        next[nIndex] = parentNodeState.index;
        String nContent = state.xsr.getPIData();
        param0[nIndex] = state.auxContentBuffer.length();
        if (param1[parentNodeState.index] == NULL) {
            param1[parentNodeState.index] = nIndex;
        }
        param1[nIndex] = nContent.length();
        state.auxContentBuffer.append(nContent);
        nameCode[nIndex] = nameCache.intern("", "", state.xsr.getPITarget());
    }

    void ensureCapacity(int i) {
        int minSize = nNodes + i;
        int newSize = nodeKind.length;
        boolean resize = false;
        while (newSize < minSize) {
            resize = true;
            if (newSize < DOUBLE_LIMIT) {
                newSize *= 2;
            } else {
                newSize += LINEAR_INCREMENT;
            }
        }
        if (resize) {
            resizeArrays(newSize);
        }
    }

    public void display() {
        for (int i = 0; i < nNodes; ++i) {
            System.err.println(i + ": " + nodeKind[i] + " next: " + next[i] + " p0: " + param0[i] + " p1: " + param1[i]
                    + " nc: " + nameCode[i]);
        }
        System.err.println("--- Text Content ---");
        System.err.println(new String(textContent));
        System.err.println("--- Aux Content ---");
        System.err.println(new String(auxContent));
    }

    private void resizeArrays(int newSize) {
        nodeKind = Arrays.copyOf(nodeKind, newSize);
        next = Arrays.copyOf(next, newSize);
        param0 = Arrays.copyOf(param0, newSize);
        param1 = Arrays.copyOf(param1, newSize);
        nameCode = Arrays.copyOf(nameCode, newSize);
    }

    AtomicValueFactory getAtomicValueFactory() {
        return avf;
    }

    void ensurePreviousIndexPresent() {
        if (previous != null) {
            return;
        }
        previous = new int[nNodes];
        Arrays.fill(previous, NULL);
        for (int i = 0; i < nNodes; ++i) {
            int n = next[i];
            if (n != NULL && n > i) {
                previous[n] = i;
            }
        }
    }

    void clearPreviousIndex() {
        previous = null;
    }

    void copyNode(DTM srcDTM, int srcIndex, StringBuilder auxTarget, StringBuilder textTarget) {
        int eon = srcDTM.findEndOfNode(srcIndex);
        int len = eon - srcIndex + 1;
        ensureCapacity(len);
        System.arraycopy(srcDTM.nodeKind, srcIndex, nodeKind, nNodes, len);
        System.arraycopy(srcDTM.next, srcIndex, next, nNodes, len);
        System.arraycopy(srcDTM.param0, srcIndex, param0, nNodes, len);
        System.arraycopy(srcDTM.param1, srcIndex, param1, nNodes, len);
        System.arraycopy(srcDTM.nameCode, srcIndex, nameCode, nNodes, len);
        int nodeOffset = nNodes - srcIndex;
        int firstAuxUser = NULL;
        int lastAuxUser = NULL;
        int auxIdxOffset = -1;
        int firstTextUser = NULL;
        int lastTextUser = NULL;
        int textIdxOffset = -1;
        for (int i = nNodes; i < nNodes + len; ++i) {
            next[i] += nodeOffset;
            switch (nodeKind[i]) {
                case DTM_ATTRIBUTE:
                case DTM_COMMENT:
                case DTM_NAMESPACE:
                case DTM_PI:
                    if (lastAuxUser == NULL) {
                        firstAuxUser = i;
                        auxIdxOffset = auxTarget.length() - param0[i];
                    }
                    lastAuxUser = i;
                    param0[i] += auxIdxOffset;
                    break;

                case DTM_TEXT:
                    if (lastTextUser == NULL) {
                        firstTextUser = i;
                        textIdxOffset = textTarget.length() - param0[i];
                    }
                    lastTextUser = i;
                    param0[i] += textIdxOffset;
                    break;

                case DTM_ELEMENT:
                    if (param0[i] != NULL) {
                        param0[i] += nodeOffset;
                    }
                    if (param1[i] != NULL) {
                        param1[i] += nodeOffset;
                    }
                    break;
            }
        }
        if (lastAuxUser != NULL) {
            int start = param0[firstAuxUser] - auxIdxOffset;
            int auxlen = param1[lastAuxUser] + param0[lastAuxUser] - param0[firstAuxUser];
            auxTarget.append(srcDTM.auxContent, start, auxlen);
        }
        if (lastTextUser != NULL) {
            int start = param0[firstTextUser] - textIdxOffset;
            int textlen = param1[lastTextUser] + param0[lastTextUser] - param0[firstTextUser];
            textTarget.append(srcDTM.textContent, start, textlen);
        }
        if (srcDTM.nameCache != nameCache) {
            for (int i = nNodes; i < nNodes + len; ++i) {
                int nc = nameCode[i];
                if (nc != NULL) {
                    nameCode[i] = nameCache.translateCode(srcDTM.nameCache, nc);
                }
            }
        }
        nNodes += len;
    }

    private int findEndOfNode(int index) {
        switch (nodeKind[index]) {
            case DTM_ATTRIBUTE:
            case DTM_COMMENT:
            case DTM_NAMESPACE:
            case DTM_PI:
            case DTM_TEXT:
                return index;

            case DTM_DOCUMENT:
            case DTM_ELEMENT:
                int i = index;
                while (i >= 0) {
                    int nxt = next[i];
                    if (nxt > i) {
                        return nxt - 1;
                    }
                    i = nxt;
                }
                return nNodes - 1;
        }
        throw new IllegalStateException();
    }

    static final class ParsingState {
        private XMLStreamReader xsr;
        private Stack<NodeState> nodeStack;
        private StringBuilder textContentBuffer;
        private StringBuilder auxContentBuffer;

        private ParsingState(XMLStreamReader xsr) {
            this.xsr = xsr;
            nodeStack = new Stack<NodeState>();
            textContentBuffer = new StringBuilder();
            auxContentBuffer = new StringBuilder();
        }
    }
}