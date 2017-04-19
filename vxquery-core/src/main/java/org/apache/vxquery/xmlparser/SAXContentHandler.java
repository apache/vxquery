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

import org.apache.hyracks.api.comm.IFrameFieldAppender;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.data.std.util.UTF8StringBuilder;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.util.string.UTF8StringUtil;
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
import org.apache.vxquery.runtime.functions.util.FunctionHelper;
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

public class SAXContentHandler implements ContentHandler, LexicalHandler {
    private static final int STRING_EXPECTED_LENGTH = 300;

    // XML node builders
    protected final AttributeNodeBuilder anb;
    protected final CommentNodeBuilder cnb;
    protected final DictionaryBuilder db;
    protected final DocumentNodeBuilder docb;
    protected final PINodeBuilder pinb;
    protected final TextNodeBuilder tnb;
    protected final UTF8StringBuilder utf8b;
    private final UTF8StringBuilder utf8bInternal;
    protected final List<ElementNodeBuilder> enbStack;
    protected final List<ElementNodeBuilder> freeENBList;
    protected boolean isIndexHandler;

    // Frame writing variables
    protected IFrameFieldAppender appender;
    private int tupleIndex;
    private IFrameWriter writer;

    // Element writing and path step variables
    protected boolean skipping;
    private String[] childLocalName = null;
    private String[] childUri = null;
    private boolean[] subElement = null;
    private final TaggedValuePointable tvp;

    // Basic tracking and setting variables
    protected final boolean attachTypes;
    protected final boolean createNodeIds;
    private int depth;
    protected final ArrayBackedValueStorage resultABVS;
    protected boolean pendingText;
    protected int nodeIdCounter;
    protected final ITreeNodeIdProvider nodeIdProvider;
    protected final ArrayBackedValueStorage tempABVS;
    private final GrowableArray textGA;
    private final GrowableArray textGAInternal;

    public SAXContentHandler(boolean attachTypes, ITreeNodeIdProvider nodeIdProvider, boolean isIndexHandler) {
        // XML node builders
        anb = new AttributeNodeBuilder();
        cnb = new CommentNodeBuilder();
        db = new DictionaryBuilder();
        docb = new DocumentNodeBuilder();
        pinb = new PINodeBuilder();
        tnb = new TextNodeBuilder();
        utf8b = new UTF8StringBuilder();
        utf8bInternal = new UTF8StringBuilder();
        enbStack = new ArrayList<>();
        freeENBList = new ArrayList<>();

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
        textGA = new GrowableArray();
        textGAInternal = new GrowableArray();
        this.isIndexHandler = isIndexHandler;
        if (isIndexHandler) {
            this.appender = null;
            this.skipping = false;
        }
    }

    public SAXContentHandler(boolean attachTypes, ITreeNodeIdProvider nodeIdProvider, IFrameFieldAppender appender,
            List<SequenceType> childSequenceTypes) {
        this(attachTypes, nodeIdProvider, false);

        // Frame writing variables
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

    public void setupElementWriter(IFrameWriter writer, int tupleIndex) {
        this.writer = writer;
        this.tupleIndex = tupleIndex;
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        if (skipping) {
            return;
        }
        try {
            appendCharArray(ch, start, length);
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
            if (appender != null) {
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
            boolean nonSkipped = false;
            if (!isIndexHandler) {
                nonSkipped = foundFirstNonSkippedElement();
            }
            flushText();
            ElementNodeBuilder enb = enbStack.remove(enbStack.size() - 1);
            enb.endChildrenChunk();
            endChildInParent(enb, nonSkipped);
            freeENB(enb);
            if (!isIndexHandler) {
                if (nonSkipped) {
                    writeElement();
                }
                endElementChildPathStep();
            }
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
            if (createNodeIds) {
                pinb.setLocalNodeId(nodeIdCounter++);
            }
            pinb.setTarget(stringToGrowableArray(target));
            pinb.setContent(stringToGrowableArray(data));
            endChildInParent(pinb);
        } catch (IOException e) {
            e.printStackTrace();
            throw new SAXException(e);
        }
    }

    private GrowableArray stringToGrowableArray(String value) throws IOException {
        FunctionHelper.stringToGrowableArray(value, textGAInternal, utf8bInternal, STRING_EXPECTED_LENGTH);
        return textGAInternal;
    }

    @Override
    public void setDocumentLocator(Locator locator) {
    }

    @Override
    public void skippedEntity(String name) throws SAXException {
    }

    @Override
    public void startDocument() throws SAXException {
        if (isIndexHandler || subElement == null) {
            skipping = false;
        }
        db.reset();
        try {
            textGA.reset();
            utf8b.reset(textGA, STRING_EXPECTED_LENGTH);
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
        boolean start = false;
        if (!isIndexHandler) {
            start = startElementChildPathStep(uri, localName);
        }

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
                stringToGrowableArray(aValue);
                tempOut.write(textGAInternal.getByteArray(), 0, textGAInternal.getLength());
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
            appendCharArray(ch, start, length);
            utf8b.finish();
            cnb.setValue(textGA);
            endChildInParent(cnb);
            textGA.reset();
            utf8b.reset(textGA, STRING_EXPECTED_LENGTH);
        } catch (IOException e) {
            e.printStackTrace();
            throw new SAXException(e);
        }
    }

    private void appendCharArray(char[] ch, int start, int length) throws IOException {
        for (int i = 0; i < length; ++i) {
            utf8b.appendChar(ch[i + start]);
        }
    }

    protected void flushText() throws IOException {
        if (pendingText) {
            peekENBStackTop().startChild(tnb);
            if (createNodeIds) {
                tnb.setLocalNodeId(nodeIdCounter++);
            }
            utf8b.finish();
            tnb.setValue(textGA);
            peekENBStackTop().endChild(tnb);
            textGA.reset();
            utf8b.reset(textGA, STRING_EXPECTED_LENGTH);
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

    protected ElementNodeBuilder createENB() {
        if (freeENBList.isEmpty()) {
            return new ElementNodeBuilder();
        }
        return freeENBList.remove(freeENBList.size() - 1);
    }

    private void freeENB(ElementNodeBuilder enb) {
        freeENBList.add(enb);
    }

    protected ElementNodeBuilder peekENBStackTop() {
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
        FrameUtils.appendFieldToWriter(writer, appender, result.getByteArray(), result.getStartOffset(),
                result.getLength());
        //        // Send to the writer.
        //        if (!addNodeToTupleAppender(result, t)) {
        //            FrameUtils.flushFrame(frame, writer);
        //            appender.reset(frame, true);
        //            if (!addNodeToTupleAppender(result, t)) {
        //                throw new HyracksDataException("Could not write frame.");
        //            }
        //        }
    }
    //
    //    private boolean addNodeToTupleAppender(TaggedValuePointable result, int t) throws HyracksDataException {
    //        // First copy all new fields over.
    //        if (fta.getFieldCount() > 0) {
    //            for (int f = 0; f < fta.getFieldCount(); ++f) {
    //                if (!appender.appendField(fta, t, f)) {
    //                    return false;
    //                }
    //            }
    //        }
    //        return appender.appendField(result.getByteArray(), result.getStartOffset(), result.getLength());
    //    }

    private String getStringFromBytes(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        UTF8StringUtil.toString(sb, bytes, 0);
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
