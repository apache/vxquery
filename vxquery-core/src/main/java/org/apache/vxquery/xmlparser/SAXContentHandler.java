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
import org.apache.vxquery.datamodel.builders.atomic.UTF8StringBuilder;
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
    // XML node builders
    private final AttributeNodeBuilder anb;
    private final CommentNodeBuilder cnb;
    private final DictionaryBuilder db;
    private final DocumentNodeBuilder docb;
    private final PINodeBuilder pinb;
    private final TextNodeBuilder tnb;
    private final UTF8StringBuilder utf8b;
    private final List<ElementNodeBuilder> enbStack;
    private final List<ElementNodeBuilder> freeENBList;

    // Frame writing variables
    private FrameTupleAppender appender;
    private ByteBuffer frame;
    private FrameTupleAccessor fta;
    private int tupleIndex;
    private IFrameWriter writer;

    // Element writing and path step variables
    private boolean skipping;
    private String[] childLocalName = null;
    private String[] childUri = null;
    private boolean[] subElement = null;
    private final TaggedValuePointable tvp;

    // Basic tracking and setting variables
    private final boolean attachTypes;
    private final boolean createNodeIds;
    private int depth;
    private final ArrayBackedValueStorage resultABVS;
    private boolean pendingText;
    private int nodeIdCounter;
    private final ITreeNodeIdProvider nodeIdProvider;
    private final ArrayBackedValueStorage tempABVS;
    private final ArrayBackedValueStorage textABVS;

    public SAXContentHandler(boolean attachTypes, ITreeNodeIdProvider nodeIdProvider) {
        // XML node builders
        anb = new AttributeNodeBuilder();
        cnb = new CommentNodeBuilder();
        db = new DictionaryBuilder();
        docb = new DocumentNodeBuilder();
        pinb = new PINodeBuilder();
        tnb = new TextNodeBuilder();
        utf8b = new UTF8StringBuilder();
        enbStack = new ArrayList<ElementNodeBuilder>();
        freeENBList = new ArrayList<ElementNodeBuilder>();

        // Element writing and path step variables
        skipping = true;
        tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();

        // Basic tracking and setting variables
        this.attachTypes = attachTypes;
        createNodeIds = nodeIdProvider != null;
        depth = 0;
        resultABVS = new ArrayBackedValueStorage();
        pendingText = false;
        nodeIdCounter = 0;
        this.nodeIdProvider = nodeIdProvider;
        tempABVS = new ArrayBackedValueStorage();
        textABVS = new ArrayBackedValueStorage();
    }

    public SAXContentHandler(boolean attachTypes, ITreeNodeIdProvider nodeIdProvider, ByteBuffer frame,
            FrameTupleAppender appender, List<SequenceType> childSequenceTypes) {
        this(attachTypes, nodeIdProvider);

        // Frame writing variables
        this.frame = frame;
        this.appender = appender;
        setChildPathSteps(childSequenceTypes);
    }

    private void setChildPathSteps(List<SequenceType> childSeq) {
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
            childLocalName[index] = getStringFromBytes(nameTest.getLocalName());
            ++index;
        }
    }

    public void setupElementWriter(IFrameWriter writer, FrameTupleAccessor fta, int tupleIndex) {
        this.writer = writer;
        this.fta = fta;
        this.tupleIndex = tupleIndex;
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        if (skipping) {
            return;
        }
        try {
            utf8b.appendCharArray(ch, start, length);
        } catch (IOException e) {
            e.printStackTrace();
            throw new SAXException(e);
        }
        pendingText = true;
    }

    @Override
    public void endDocument() throws SAXException {
        if (skipping) {
            return;
        }
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

    private void endElementChildPathStep() throws IOException {
        if (subElement != null && depth <= subElement.length) {
            subElement[depth - 1] = false;
        }
    }

    @Override
    public void endElement(String uri, String localName, String name) throws SAXException {
        if (skipping) {
            --depth;
            return;
        }
        try {
            boolean nonSkipped = foundFirstNonSkippedElement();
            flushText();
            ElementNodeBuilder enb = enbStack.remove(enbStack.size() - 1);
            enb.endChildrenChunk();
            endChildInParent(enb, nonSkipped);
            freeENB(enb);
            if (nonSkipped) {
                writeElement();
            }
            endElementChildPathStep();
        } catch (IOException e) {
            e.printStackTrace();
            throw new SAXException(e);
        }
        --depth;
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
    }

    @Override
    public void processingInstruction(String target, String data) throws SAXException {
        if (skipping) {
            return;
        }
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
        if (subElement == null) {
            skipping = false;
        }
        db.reset();
        try {
            textABVS.reset();
            utf8b.reset(textABVS);
        } catch (IOException e) {
            throw new SAXException(e);
        }
        if (skipping) {
            return;
        }
        try {
            resultABVS.reset();
            docb.reset(resultABVS);
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

    /**
     * The filter settings here are similar to one in the class linked below.
     * 
     * @see org.apache.vxquery.runtime.functions.step.NodeTestFilter.java
     */
    private boolean startElementChildPathStep(String uri, String localName) {
        if (subElement != null && depth <= subElement.length) {
            // Check path step if it exists.
            subElement[depth - 1] = true;
            if (uri != null) {
                if (childUri[depth - 1] != null && uri.compareTo(childUri[depth - 1]) != 0) {
                    subElement[depth - 1] = false;
                }
            }
            if (localName != null) {
                if (childLocalName[depth - 1] != null && localName.compareTo(childLocalName[depth - 1]) != 0) {
                    subElement[depth - 1] = false;
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
        ++depth;
        boolean start = startElementChildPathStep(uri, localName);

        if (skipping) {
            return;
        }
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

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
    }

    @Override
    public void comment(char[] ch, int start, int length) throws SAXException {
        if (skipping) {
            return;
        }
        try {
            flushText();
            startChildInParent(cnb);
            if (createNodeIds) {
                cnb.setLocalNodeId(nodeIdCounter++);
            }
            utf8b.appendCharArray(ch, start, length);
            utf8b.finish();
            cnb.setValue(textABVS);
            endChildInParent(cnb);
            textABVS.reset();
            utf8b.reset(textABVS);
        } catch (IOException e) {
            e.printStackTrace();
            throw new SAXException(e);
        }
    }

    private void flushText() throws IOException {
        if (pendingText) {
            peekENBStackTop().startChild(tnb);
            if (createNodeIds) {
                tnb.setLocalNodeId(nodeIdCounter++);
            }
            utf8b.finish();
            tnb.setValue(textABVS);
            peekENBStackTop().endChild(tnb);
            textABVS.reset();
            utf8b.reset(textABVS);
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

    public void writeElement() throws IOException {
        tempABVS.reset();
        DataOutput out = tempABVS.getDataOutput();
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
        db.writeFromCache(tempABVS);
        out.write(resultABVS.getByteArray(), resultABVS.getStartOffset(), resultABVS.getLength());
        tvp.set(tempABVS.getByteArray(), tempABVS.getStartOffset(), tempABVS.getLength());
        addNodeToTuple(tvp, tupleIndex);
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
        db.writeFromCache(abvs);
        out.write(resultABVS.getByteArray(), resultABVS.getStartOffset(), resultABVS.getLength());
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
            resultABVS.reset();
            anb.reset(resultABVS);
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
        if (bytes == null) {
            return null;
        }
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