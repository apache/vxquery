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
package org.apache.vxquery.xmlparser;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.vxquery.datamodel.accessors.nodes.NodeTreePointable;
import org.apache.vxquery.datamodel.builders.nodes.AbstractNodeBuilder;
import org.apache.vxquery.datamodel.builders.nodes.AttributeNodeBuilder;
import org.apache.vxquery.datamodel.builders.nodes.CommentNodeBuilder;
import org.apache.vxquery.datamodel.builders.nodes.DictionaryBuilder;
import org.apache.vxquery.datamodel.builders.nodes.DocumentNodeBuilder;
import org.apache.vxquery.datamodel.builders.nodes.ElementNodeBuilder;
import org.apache.vxquery.datamodel.builders.nodes.PINodeBuilder;
import org.apache.vxquery.datamodel.builders.nodes.TextNodeBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.types.BuiltinTypeQNames;
import org.apache.vxquery.xmlquery.query.XQueryConstants;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.ext.LexicalHandler;

import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class SAXContentHandler implements ContentHandler, LexicalHandler {
    private final ArrayBackedValueStorage docABVS;

    private final boolean createNodeIds;

    private final boolean attachTypes;

    private final ITreeNodeIdProvider nodeIdProvider;

    private final ArrayBackedValueStorage tempABVS;

    private final DocumentNodeBuilder docb;

    private final TextNodeBuilder tnb;

    private final CommentNodeBuilder cnb;

    private final PINodeBuilder pinb;

    private final AttributeNodeBuilder anb;

    private final DictionaryBuilder db;

    private final StringBuilder buffer;

    private final List<ElementNodeBuilder> enbStack;

    private final List<ElementNodeBuilder> freeENBList;

    private int nodeIdCounter;

    private boolean pendingText;

    public SAXContentHandler(boolean attachTypes, ITreeNodeIdProvider nodeIdProvider) {
        docABVS = new ArrayBackedValueStorage();
        this.createNodeIds = nodeIdProvider != null;
        this.attachTypes = attachTypes;
        this.nodeIdProvider = nodeIdProvider;
        this.tempABVS = new ArrayBackedValueStorage();
        docb = new DocumentNodeBuilder();
        tnb = new TextNodeBuilder();
        cnb = new CommentNodeBuilder();
        pinb = new PINodeBuilder();
        anb = new AttributeNodeBuilder();
        db = new DictionaryBuilder();
        buffer = new StringBuilder();
        enbStack = new ArrayList<ElementNodeBuilder>();
        freeENBList = new ArrayList<ElementNodeBuilder>();
        pendingText = false;
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        buffer.append(ch, start, length);
        pendingText = true;
    }

    @Override
    public void endDocument() throws SAXException {
        try {
            flushText();
            docb.endChildrenChunk();
            docb.finish();
        } catch (IOException e) {
            e.printStackTrace();
            throw new SAXException(e);
        }
    }

    @Override
    public void endElement(String uri, String localName, String name) throws SAXException {
        try {
            flushText();
            ElementNodeBuilder enb = enbStack.remove(enbStack.size() - 1);
            enb.endChildrenChunk();
            endChildInParent(enb);
            freeENB(enb);
        } catch (IOException e) {
            e.printStackTrace();
            throw new SAXException(e);
        }
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
    }

    @Override
    public void processingInstruction(String target, String data) throws SAXException {
        try {
            flushText();
            startChildInParent(pinb);
            tempABVS.reset();
            tempABVS.getDataOutput().writeUTF(target);
            if (createNodeIds) {
                pinb.setLocalNodeId(nodeIdCounter++);
            }
            pinb.setTarget(tempABVS);
            tempABVS.reset();
            tempABVS.getDataOutput().writeUTF(data);
            pinb.setContent(tempABVS);
            endChildInParent(pinb);
        } catch (IOException e) {
            e.printStackTrace();
            throw new SAXException(e);
        }
    }

    @Override
    public void setDocumentLocator(Locator locator) {
    }

    @Override
    public void skippedEntity(String name) throws SAXException {
    }

    @Override
    public void startDocument() throws SAXException {
        try {
            docb.reset(docABVS);
            if (createNodeIds) {
                docb.setLocalNodeId(nodeIdCounter++);
            }
            docb.startChildrenChunk();
            flushText();
        } catch (IOException e) {
            e.printStackTrace();
            throw new SAXException(e);
        }
    }

    @Override
    public void startElement(String uri, String localName, String name, Attributes atts) throws SAXException {
        try {
            flushText();
            int idx = name.indexOf(':');
            String prefix = idx < 0 ? "" : name.substring(0, idx);
            ElementNodeBuilder enb = createENB();
            startChildInParent(enb);
            int uriCode = db.lookup(uri);
            int localNameCode = db.lookup(localName);
            int prefixCode = db.lookup(prefix);
            enb.setName(uriCode, localNameCode, prefixCode);
            if (attachTypes) {
                int typeUriCode = db.lookup(XQueryConstants.XS_NSURI);
                int typeLocalNameCode = db.lookup(BuiltinTypeQNames.UNTYPED_STR);
                int typePrefixCode = db.lookup(XQueryConstants.XS_PREFIX);
                enb.setType(typeUriCode, typeLocalNameCode, typePrefixCode);
            }
            if (createNodeIds) {
                enb.setLocalNodeId(nodeIdCounter++);
            }
            enb.startAttributeChunk();
            final int nAttrs = atts.getLength();
            for (int i = 0; i < nAttrs; ++i) {
                String aName = atts.getQName(i);
                int aIdx = aName.indexOf(':');
                int aPrefixCode = db.lookup(aIdx < 0 ? "" : aName.substring(0, aIdx));
                int aLocalNameCode = db.lookup(atts.getLocalName(i));
                int aUriCode = db.lookup(atts.getURI(i));
                String aValue = atts.getValue(i);
                tempABVS.reset();
                DataOutput tempOut = tempABVS.getDataOutput();
                tempOut.write(ValueTag.XS_UNTYPED_ATOMIC_TAG);
                tempOut.writeUTF(aValue);
                enb.startAttribute(anb);
                anb.setName(aUriCode, aLocalNameCode, aPrefixCode);
                if (attachTypes) {
                    int typeUriCode = db.lookup(XQueryConstants.XS_NSURI);
                    int typeLocalNameCode = db.lookup(BuiltinTypeQNames.UNTYPED_ATOMIC_STR);
                    int typePrefixCode = db.lookup(XQueryConstants.XS_PREFIX);
                    anb.setType(typeUriCode, typeLocalNameCode, typePrefixCode);
                }
                if (createNodeIds) {
                    anb.setLocalNodeId(nodeIdCounter++);
                }
                anb.setValue(tempABVS);
                enb.endAttribute(anb);
            }
            enb.endAttributeChunk();
            enb.startChildrenChunk();
            enbStack.add(enb);
        } catch (IOException e) {
            e.printStackTrace();
            throw new SAXException(e);
        }
    }

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
    }

    @Override
    public void comment(char[] ch, int start, int length) throws SAXException {
        try {
            flushText();
            startChildInParent(cnb);
            buffer.append(ch, start, length);
            tempABVS.reset();
            tempABVS.getDataOutput().writeUTF(buffer.toString());
            if (createNodeIds) {
                cnb.setLocalNodeId(nodeIdCounter++);
            }
            cnb.setValue(tempABVS);
            endChildInParent(cnb);
            buffer.delete(0, buffer.length());
        } catch (IOException e) {
            e.printStackTrace();
            throw new SAXException(e);
        }
    }

    private void flushText() throws IOException {
        if (pendingText) {
            peekENBStackTop().startChild(tnb);
            tempABVS.reset();
            tempABVS.getDataOutput().writeUTF(buffer.toString());
            if (createNodeIds) {
                tnb.setLocalNodeId(nodeIdCounter++);
            }
            tnb.setValue(tempABVS);
            peekENBStackTop().endChild(tnb);
            buffer.delete(0, buffer.length());
            pendingText = false;
        }
    }

    @Override
    public void endCDATA() throws SAXException {
    }

    @Override
    public void endDTD() throws SAXException {
    }

    @Override
    public void endEntity(String name) throws SAXException {
    }

    @Override
    public void startCDATA() throws SAXException {
    }

    @Override
    public void startDTD(String name, String publicId, String systemId) throws SAXException {
    }

    @Override
    public void startEntity(String name) throws SAXException {
    }

    public void write(ArrayBackedValueStorage abvs) throws IOException {
        DataOutput out = abvs.getDataOutput();
        out.write(ValueTag.NODE_TREE_TAG);
        byte header = NodeTreePointable.HEADER_DICTIONARY_EXISTS_MASK;
        if (attachTypes) {
            header |= NodeTreePointable.HEADER_TYPE_EXISTS_MASK;
        }
        if (createNodeIds) {
            header |= NodeTreePointable.HEADER_NODEID_EXISTS_MASK;
        }
        out.write(header);
        if (createNodeIds) {
            out.writeLong(nodeIdProvider.getId());
        }
        db.write(abvs);
        out.write(docABVS.getByteArray(), docABVS.getStartOffset(), docABVS.getLength());
    }

    private ElementNodeBuilder createENB() {
        if (freeENBList.isEmpty()) {
            return new ElementNodeBuilder();
        }
        return freeENBList.remove(freeENBList.size() - 1);
    }

    private void freeENB(ElementNodeBuilder enb) {
        freeENBList.add(enb);
    }

    private ElementNodeBuilder peekENBStackTop() {
        return enbStack.get(enbStack.size() - 1);
    }

    private void startChildInParent(AbstractNodeBuilder anb) throws IOException {
        if (enbStack.isEmpty()) {
            docb.startChild(anb);
        } else {
            peekENBStackTop().startChild(anb);
        }
    }

    private void endChildInParent(AbstractNodeBuilder anb) throws IOException {
        if (enbStack.isEmpty()) {
            docb.endChild(anb);
        } else {
            peekENBStackTop().endChild(anb);
        }
    }
}