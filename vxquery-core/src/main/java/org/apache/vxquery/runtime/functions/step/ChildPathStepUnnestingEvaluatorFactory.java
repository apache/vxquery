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
package org.apache.vxquery.runtime.functions.step;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.atomic.CodedQNamePointable;
import org.apache.vxquery.datamodel.accessors.nodes.AttributeNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.DocumentNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.ElementNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.NodeTreePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentUnnestingEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentUnnestingEvaluatorFactory;
import org.apache.vxquery.types.AttributeType;
import org.apache.vxquery.types.ElementType;
import org.apache.vxquery.types.NameTest;
import org.apache.vxquery.types.NodeType;
import org.apache.vxquery.types.SequenceType;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IUnnestingEvaluator;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class ChildPathStepUnnestingEvaluatorFactory extends AbstractTaggedValueArgumentUnnestingEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public ChildPathStepUnnestingEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IUnnestingEvaluator createEvaluator(IScalarEvaluator[] args) throws AlgebricksException {

        final SequencePointable seqp = (SequencePointable) SequencePointable.FACTORY.createPointable();
        final TaggedValuePointable rootTVP = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final DocumentNodePointable dnp = (DocumentNodePointable) DocumentNodePointable.FACTORY.createPointable();
        final ElementNodePointable enp = (ElementNodePointable) ElementNodePointable.FACTORY.createPointable();
        final IntegerPointable ip = (IntegerPointable) IntegerPointable.FACTORY.createPointable();
        final NodeTreePointable ntp = (NodeTreePointable) NodeTreePointable.FACTORY.createPointable();
        final TaggedValuePointable itemTvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();

        return new AbstractTaggedValueArgumentUnnestingEvaluator(args) {
            private int index;
            private int seqLength;

            private boolean first;
            private ArrayBackedValueStorage nodeAbvs;
            private INodeFilter filter;

            @Override
            public boolean step(IPointable result) throws AlgebricksException {
                while (index < seqLength) {
                    // Get the next item
                    seqp.getEntry(index, itemTvp);
                    ++index;
                    
                    // Test to see if the item fits the path step
                    if (matches()) {
                        try {
                            setNodeToResult(result);
                            return true;
                        } catch (IOException e) {
                            String description = ErrorCode.SYSE0001 + ": " + ErrorCode.SYSE0001.getDescription();
                            throw new AlgebricksException(description);
                        }
                    }
                }
                return false;
            }

            @Override
            protected void init(TaggedValuePointable[] args) throws SystemException {
                first = true;
                nodeAbvs = new ArrayBackedValueStorage();

                index = 0;
                if (first) {
                    if (args[1].getTag() != ValueTag.XS_INT_TAG) {
                        throw new IllegalArgumentException("Expected int value tag, got: " + args[1].getTag());
                    }
                    args[1].getValue(ip);
                    int typeCode = ip.getInteger();
                    SequenceType sType = dCtx.getStaticContext().lookupSequenceType(typeCode);
                    setNodeTest(sType);
                    first = false;
                }
                if (args[0].getTag() != ValueTag.NODE_TREE_TAG) {
                    throw new SystemException(ErrorCode.SYSE0001);
                }
                args[0].getValue(ntp);
                getSequence(ntp, seqp);
                seqLength = seqp.getEntryCount();
            }

            protected void setNodeTest(SequenceType sType) {
                final NodeType nodeType = (NodeType) sType.getItemType();
                switch (nodeType.getNodeKind()) {
                    case ANY:
                        filter = new INodeFilter() {
                            @Override
                            public boolean accept(NodeTreePointable ntp, TaggedValuePointable tvp) {
                                return true;
                            }
                        };
                        break;

                    case ATTRIBUTE: {
                        AttributeType aType = (AttributeType) nodeType;
                        NameTest nameTest = aType.getNameTest();
                        byte[] uri = nameTest.getUri();
                        byte[] localName = nameTest.getLocalName();
                        final UTF8StringPointable urip = (UTF8StringPointable) (uri == null ? null
                                : UTF8StringPointable.FACTORY.createPointable());
                        final UTF8StringPointable localp = (UTF8StringPointable) (localName == null ? null
                                : UTF8StringPointable.FACTORY.createPointable());
                        if (uri != null) {
                            urip.set(uri, 0, uri.length);
                        }
                        if (localName != null) {
                            localp.set(localName, 0, localName.length);
                        }
                        final IPointable temp = VoidPointable.FACTORY.createPointable();
                        final AttributeNodePointable anp = (AttributeNodePointable) AttributeNodePointable.FACTORY
                                .createPointable();
                        final CodedQNamePointable cqp = (CodedQNamePointable) CodedQNamePointable.FACTORY
                                .createPointable();
                        filter = new INodeFilter() {
                            @Override
                            public boolean accept(NodeTreePointable ntp, TaggedValuePointable tvp) {
                                if (tvp.getTag() != ValueTag.ATTRIBUTE_NODE_TAG) {
                                    return false;
                                }
                                tvp.getValue(anp);
                                anp.getName(cqp);
                                if (urip != null) {
                                    ntp.getString(cqp.getNamespaceCode(), temp);
                                    if (urip.compareTo(temp) != 0) {
                                        return false;
                                    }
                                }
                                if (localp != null) {
                                    ntp.getString(cqp.getLocalCode(), temp);
                                    if (localp.compareTo(temp) != 0) {
                                        return false;
                                    }
                                }
                                return true;
                            }
                        };
                        break;
                    }

                    case COMMENT:
                        filter = new INodeFilter() {
                            @Override
                            public boolean accept(NodeTreePointable ntp, TaggedValuePointable tvp) {
                                return tvp.getTag() == ValueTag.COMMENT_NODE_TAG;
                            }
                        };
                        break;

                    case DOCUMENT:
                        filter = new INodeFilter() {
                            @Override
                            public boolean accept(NodeTreePointable ntp, TaggedValuePointable tvp) {
                                return tvp.getTag() == ValueTag.DOCUMENT_NODE_TAG;
                            }
                        };
                        break;

                    case ELEMENT: {
                        ElementType eType = (ElementType) nodeType;
                        NameTest nameTest = eType.getNameTest();
                        byte[] uri = nameTest.getUri();
                        byte[] localName = nameTest.getLocalName();
                        final UTF8StringPointable urip = (UTF8StringPointable) (uri == null ? null
                                : UTF8StringPointable.FACTORY.createPointable());
                        final UTF8StringPointable localp = (UTF8StringPointable) (localName == null ? null
                                : UTF8StringPointable.FACTORY.createPointable());
                        if (uri != null) {
                            urip.set(uri, 0, uri.length);
                        }
                        if (localName != null) {
                            localp.set(localName, 0, localName.length);
                        }
                        final IPointable temp = VoidPointable.FACTORY.createPointable();
                        final ElementNodePointable enp = (ElementNodePointable) ElementNodePointable.FACTORY
                                .createPointable();
                        final CodedQNamePointable cqp = (CodedQNamePointable) CodedQNamePointable.FACTORY
                                .createPointable();
                        filter = new INodeFilter() {
                            @Override
                            public boolean accept(NodeTreePointable ntp, TaggedValuePointable tvp) {
                                if (tvp.getTag() != ValueTag.ELEMENT_NODE_TAG) {
                                    return false;
                                }
                                tvp.getValue(enp);
                                enp.getName(cqp);
                                if (urip != null) {
                                    ntp.getString(cqp.getNamespaceCode(), temp);
                                    if (urip.compareTo(temp) != 0) {
                                        return false;
                                    }
                                }
                                if (localp != null) {
                                    ntp.getString(cqp.getLocalCode(), temp);
                                    if (localp.compareTo(temp) != 0) {
                                        return false;
                                    }
                                }
                                return true;
                            }
                        };
                        break;
                    }

                    case PI:
                        filter = new INodeFilter() {
                            @Override
                            public boolean accept(NodeTreePointable ntp, TaggedValuePointable tvp) {
                                return tvp.getTag() == ValueTag.PI_NODE_TAG;
                            }
                        };
                        break;

                    case TEXT:
                        filter = new INodeFilter() {
                            @Override
                            public boolean accept(NodeTreePointable ntp, TaggedValuePointable tvp) {
                                return tvp.getTag() == ValueTag.TEXT_NODE_TAG;
                            }
                        };
                        break;
                }
            }

            protected boolean matches() {
                return filter.accept(ntp, itemTvp);
            }

            protected void setNodeToResult(IPointable result) throws IOException {
                nodeAbvs.reset();
                DataOutput mainOut = nodeAbvs.getDataOutput();
                mainOut.write(ValueTag.NODE_TREE_TAG);
                boolean hasDictionary = ntp.dictionaryExists() && hasDictionary(itemTvp.getTag());
                byte header = (byte) (hasDictionary ? NodeTreePointable.HEADER_DICTIONARY_EXISTS_MASK : 0);
                // TODO add all header flags to this setting.
                boolean hasNodeIds = ntp.nodeIdExists();
                if (hasNodeIds) {
                    header |= NodeTreePointable.HEADER_NODEID_EXISTS_MASK;
                }
                mainOut.write(header);
                if (hasNodeIds) {
                    mainOut.writeInt(ntp.getRootNodeId());
                }
                if (hasDictionary) {
                    mainOut.write(ntp.getByteArray(), ntp.getDictionaryOffset(), ntp.getDictionarySize());
                }
                mainOut.write(itemTvp.getByteArray(), itemTvp.getStartOffset(), itemTvp.getLength());
                result.set(nodeAbvs.getByteArray(), nodeAbvs.getStartOffset(), nodeAbvs.getLength());
            }

            private boolean hasDictionary(byte tag) {
                switch (tag) {
                    case ValueTag.ATTRIBUTE_NODE_TAG:
                    case ValueTag.DOCUMENT_NODE_TAG:
                    case ValueTag.ELEMENT_NODE_TAG:
                        return true;
                }
                return false;
            }

            protected void getSequence(NodeTreePointable ntp, SequencePointable seqp) throws SystemException {
                ntp.getRootNode(rootTVP);
                switch (rootTVP.getTag()) {
                    case ValueTag.DOCUMENT_NODE_TAG:
                        rootTVP.getValue(dnp);
                        dnp.getContent(ntp, seqp);
                        return;

                    case ValueTag.ELEMENT_NODE_TAG:
                        rootTVP.getValue(enp);
                        if (enp.childrenChunkExists()) {
                            enp.getChildrenSequence(ntp, seqp);
                            return;
                        }
                }
                XDMConstants.setEmptySequence(seqp);
            }

        };
    }

    private interface INodeFilter {
        public boolean accept(NodeTreePointable ntp, TaggedValuePointable tvp);
    }

}