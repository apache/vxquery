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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
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
import org.apache.vxquery.types.ElementType;
import org.apache.vxquery.types.NameTest;
import org.apache.vxquery.types.NodeType;
import org.apache.vxquery.types.SequenceType;
import org.apache.vxquery.xmlquery.query.XQueryConstants;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.ext.LexicalHandler;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;

public class SAXContentHandler implements ContentHandler, LexicalHandler {
    private final AttributeNodeBuilder anb;

    private FrameTupleAppender appender;

    private final boolean attachTypes;

    private final StringBuilder buffer;

    private String[] childLocalName = null;

    private String[] childUri = null;

    private final CommentNodeBuilder cnb;

    private final boolean createNodeIds;

    private final DictionaryBuilder db;

    private int depth = 0;

    private final ArrayBackedValueStorage docABVS;

    private final DocumentNodeBuilder docb;

    private final ArrayBackedValueStorage elementABVS;

    private final List<ElementNodeBuilder> enbStack;

    private ByteBuffer frame;

    private final List<ElementNodeBuilder> freeENBList;

    private FrameTupleAccessor fta;

    private int nodeIdCounter;

    private final ITreeNodeIdProvider nodeIdProvider;

    private boolean pendingText;

    private final PINodeBuilder pinb;

    private final ArrayBackedValueStorage resultABVS;

    private boolean skipping;

    private boolean[] subElement = null;

    private int t;

    private final ArrayBackedValueStorage tempABVS;

    private final TextNodeBuilder tnb;

    private final TaggedValuePointable tvp;

    private IFrameWriter writer;

    public SAXContentHandler(boolean attachTypes, ITreeNodeIdProvider nodeIdProvider) {
        docABVS = new ArrayBackedValueStorage();
        elementABVS = new ArrayBackedValueStorage();
        resultABVS = new ArrayBackedValueStorage();
        tempABVS = new ArrayBackedValueStorage();
        createNodeIds = nodeIdProvider != null;
        this.attachTypes = attachTypes;
        this.nodeIdProvider = nodeIdProvider;
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
        tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        skipping = true;
    }

    public SAXContentHandler(boolean attachTypes, ITreeNodeIdProvider nodeIdProvider, ByteBuffer frame,
            FrameTupleAppender appender, List<SequenceType> childSequenceTypes) {
        this(attachTypes, nodeIdProvider);
        this.frame = frame;
        this.appender = appender;
        setChildPathSteps(childSequenceTypes);
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        if (!skipping) {
            buffer.append(ch, start, length);
            pendingText = true;
        }
    }

    @Override
    public void endDocument() throws SAXException {
        if (!skipping) {
            try {
                flushText();
                docb.endChildrenChunk();
                docb.finish();
                if (frame != null && appender != null) {
                    writeElement();
                }
            } catch (IOException e) {
                e.printStackTrace();
                throw new SAXException(e);
            }
        }
    }

    private void endElementChildPathStep() throws IOException {
        if (foundFirstNonSkippedElement()) {
            writeElement();
        }
        if (subElement != null && depth <= subElement.length) {
            subElement[depth - 1] = false;
        }
    }

    @Override
    public void endElement(String uri, String localName, String name) throws SAXException {
        if (!skipping) {
            try {
                flushText();
                ElementNodeBuilder enb = enbStack.remove(enbStack.size() - 1);
                enb.endChildrenChunk();
                endChildInParent(enb, foundFirstNonSkippedElement());
                freeENB(enb);
                endElementChildPathStep();
            } catch (IOException e) {
                e.printStackTrace();
                throw new SAXException(e);
            }
        }
        depth--;
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
    }

    @Override
    public void processingInstruction(String target, String data) throws SAXException {
        if (!skipping) {
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
    }

    @Override
    public void setDocumentLocator(Locator locator) {
    }

    @Override
    public void skippedEntity(String name) throws SAXException {
    }

    @Override
    public void startDocument() throws SAXException {
        if (subElement == null) {
            skipping = false;
        }
        db.reset();
        docABVS.reset();
        if (!skipping) {
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
    }

    private boolean startElementChildPathStep(String uri, String localName) {
        if (subElement != null && depth <= subElement.length) {
            // Check path step if it exists.
            if (uri.compareTo(childUri[depth - 1]) == 0) {
                if (localName.compareTo(childLocalName[depth - 1]) == 0) {
                    subElement[depth - 1] = true;
                }
            }
        }
        boolean start = foundFirstNonSkippedElement();
        if (start) {
            skipping = false;
        }
        return start;
    }

    @Override
    public void startElement(String uri, String localName, String name, Attributes atts) throws SAXException {
        depth++;
        boolean start = startElementChildPathStep(uri, localName);

        if (!skipping) {
            try {
                flushText();
                int idx = name.indexOf(':');
                String prefix = idx < 0 ? "" : name.substring(0, idx);
                ElementNodeBuilder enb = createENB();
                startChildInParent(enb, start);
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
    }

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
    }

    @Override
    public void comment(char[] ch, int start, int length) throws SAXException {
        if (!skipping) {
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

    private void setChildPathSteps(List<SequenceType> childSeq) {
        //        this.childSeq = childSeq;
        if (!childSeq.isEmpty()) {
            subElement = new boolean[childSeq.size()];
            childUri = new String[childSeq.size()];
            childLocalName = new String[childSeq.size()];
        }

        int index = 0;
        for (SequenceType sType : childSeq) {
            NodeType nodeType = (NodeType) sType.getItemType();
            ElementType eType = (ElementType) nodeType;
            NameTest nameTest = eType.getNameTest();
            childUri[index] = getStringFromBytes(nameTest.getUri());
            childLocalName[index] = getStringFromBytes(nameTest.getLocalName());;
            index++;
        }
    }

    public void setupElementWriter(IFrameWriter writer, FrameTupleAccessor fta, int t) {
        this.writer = writer;
        this.fta = fta;
        this.t = t;
    }

    public void writeElement() throws IOException {
        resultABVS.reset();
        DataOutput out = resultABVS.getDataOutput();
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
            out.writeInt(nodeIdProvider.getId());
        }
        db.write(resultABVS);
        if (subElement == null) {
            out.write(docABVS.getByteArray(), docABVS.getStartOffset(), docABVS.getLength());
        } else {
            out.write(elementABVS.getByteArray(), elementABVS.getStartOffset(), elementABVS.getLength());
        }
        tvp.set(resultABVS.getByteArray(), resultABVS.getStartOffset(), resultABVS.getLength());
        addNodeToTuple(tvp, t);
        skipping = true;
    }

    public void writeDocument(ArrayBackedValueStorage abvs) throws IOException {
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
            out.writeInt(nodeIdProvider.getId());
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
        startChildInParent(anb, false);
    }

    private void startChildInParent(AbstractNodeBuilder anb, boolean startNewElement) throws IOException {
        if (startNewElement) {
            elementABVS.reset();
            anb.reset(elementABVS);
        } else if (enbStack.isEmpty()) {
            docb.startChild(anb);
        } else {
            peekENBStackTop().startChild(anb);
        }
    }

    private void endChildInParent(AbstractNodeBuilder anb) throws IOException {
        endChildInParent(anb, false);
    }

    private void endChildInParent(AbstractNodeBuilder anb, boolean endNewElement) throws IOException {
        if (endNewElement) {
            anb.finish();
        } else if (enbStack.isEmpty()) {
            docb.endChild(anb);
        } else {
            peekENBStackTop().endChild(anb);
        }
    }

    private void addNodeToTuple(TaggedValuePointable result, int t) throws HyracksDataException {
        // Send to the writer.
        if (!addNodeToTupleAppender(result, t)) {
            FrameUtils.flushFrame(frame, writer);
            appender.reset(frame, true);
            if (!addNodeToTupleAppender(result, t)) {
                throw new HyracksDataException("Could not write frame.");
            }
        }
    }

    private boolean addNodeToTupleAppender(TaggedValuePointable result, int t) throws HyracksDataException {
        // First copy all new fields over.
        if (fta.getFieldCount() > 0) {
            for (int f = 0; f < fta.getFieldCount(); ++f) {
                if (!appender.appendField(fta, t, f)) {
                    return false;
                }
            }
        }
        return appender.appendField(result.getByteArray(), result.getStartOffset(), result.getLength());
    }

    private String getStringFromBytes(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        UTF8StringPointable.toString(sb, bytes, 0);
        return sb.toString();
    }

    /**
     * Determines if the correct path step is active.
     */
    private boolean foundFirstNonSkippedElement() {
        if (subElement == null || subElement.length != depth) {
            // Not the correct depth.
            return false;
        }
        for (boolean b : subElement) {
            if (!b) {
                // Found a path step that did not match.
                return false;
            }
        }
        return true;
    }

}