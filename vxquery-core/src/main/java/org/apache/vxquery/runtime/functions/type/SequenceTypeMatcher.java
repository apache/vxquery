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
package org.apache.vxquery.runtime.functions.type;

import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.nodes.NodeTreePointable;
import org.apache.vxquery.datamodel.accessors.nodes.PINodePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.types.AnyItemType;
import org.apache.vxquery.types.AtomicType;
import org.apache.vxquery.types.AttributeType;
import org.apache.vxquery.types.BuiltinTypeRegistry;
import org.apache.vxquery.types.DocumentType;
import org.apache.vxquery.types.ElementType;
import org.apache.vxquery.types.ItemType;
import org.apache.vxquery.types.NodeKind;
import org.apache.vxquery.types.NodeType;
import org.apache.vxquery.types.ProcessingInstructionType;
import org.apache.vxquery.types.Quantifier;
import org.apache.vxquery.types.SchemaType;
import org.apache.vxquery.types.SequenceType;

import org.apache.hyracks.data.std.primitive.UTF8StringPointable;

public class SequenceTypeMatcher {
    private final NodeTreePointable ntp = (NodeTreePointable) NodeTreePointable.FACTORY.createPointable();
    private final TaggedValuePointable tempTVP1 = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    private final PINodePointable pinp = (PINodePointable) PINodePointable.FACTORY.createPointable();
    private final UTF8StringPointable utf8sp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
    private final SequencePointable seqp = (SequencePointable) SequencePointable.FACTORY.createPointable();
    private final TaggedValuePointable tempTVP2 = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();

    private SequenceType sequenceType;

    public boolean sequenceTypeMatch(TaggedValuePointable tvp) {
        byte tag = tvp.getTag();
        Quantifier stq = sequenceType.getQuantifier();
        ItemType it = sequenceType.getItemType();
        if (tag == ValueTag.SEQUENCE_TAG) {
            tvp.getValue(seqp);
            Quantifier vq = getSequenceQuantifier(seqp);
            if (stq.isSubQuantifier(vq)) {
                if (it instanceof AnyItemType) {
                    return true;
                } else {
                    int n = seqp.getEntryCount();
                    for (int i = 0; i < n; ++i) {
                        seqp.getEntry(i, tempTVP2);
                        if (!itemSequenceTypeMatch(tempTVP2, it)) {
                            return false;
                        }
                    }
                    return true;
                }
            }
        } else {
            if (stq.isSubQuantifier(Quantifier.QUANT_ONE)) {
                return itemSequenceTypeMatch(tvp, it);
            }
        }
        return false;
    }

    private boolean itemSequenceTypeMatch(TaggedValuePointable tvp, ItemType it) {
        byte tag = tvp.getTag();
        if (it instanceof AnyItemType) {
            return true;
        } else if (it.isAtomicType()) {
            AtomicType ait = (AtomicType) it;
            if (BuiltinTypeRegistry.INSTANCE.isBuiltinTypeId(tag)) {
                SchemaType vType = BuiltinTypeRegistry.INSTANCE.getSchemaTypeById(tag);
                while (vType != null && vType.getTypeId() != ait.getTypeId()) {
                    vType = vType.getBaseType();
                }
                return vType != null;
            }
        } else if (it instanceof NodeType && tag == ValueTag.NODE_TREE_TAG) {
            NodeType nt = (NodeType) it;
            NodeKind kind = nt.getNodeKind();
            if (kind == NodeKind.ANY) {
                return true;
            } else {
                tvp.getValue(ntp);
                ntp.getRootNode(tempTVP1);
                switch (tempTVP1.getTag()) {
                    case ValueTag.ATTRIBUTE_NODE_TAG:
                        if (kind == NodeKind.ATTRIBUTE) {
                            if (nt.equals(AttributeType.ANYATTRIBUTE)) {
                                return true;
                            } else {

                            }
                        }
                        break;

                    case ValueTag.COMMENT_NODE_TAG:
                        return kind == NodeKind.ATTRIBUTE;

                    case ValueTag.DOCUMENT_NODE_TAG:
                        if (kind == NodeKind.DOCUMENT) {
                            if (nt.equals(DocumentType.ANYDOCUMENT)) {
                                return true;
                            } else {

                            }
                        }
                        break;

                    case ValueTag.ELEMENT_NODE_TAG:
                        if (kind == NodeKind.ELEMENT) {
                            if (nt.equals(ElementType.ANYELEMENT)) {
                                return true;
                            } else {

                            }
                        }
                        break;

                    case ValueTag.PI_NODE_TAG:
                        if (kind == NodeKind.PI) {
                            if (nt.equals(ProcessingInstructionType.ANYPI)) {
                                return true;
                            } else {
                                ProcessingInstructionType pit = (ProcessingInstructionType) nt;
                                tempTVP1.getValue(pinp);
                                pinp.getTarget(ntp, utf8sp);
                                byte[] target = pit.getTarget();
                                return utf8sp.compareTo(target, 0, target.length) == 0;
                            }
                        }
                        break;

                    case ValueTag.TEXT_NODE_TAG:
                        return kind == NodeKind.TEXT;
                }
            }
        }
        return false;
    }

    public boolean matchesAllInstances(SequenceType testST) {
        Quantifier stq = sequenceType.getQuantifier();
        ItemType it = sequenceType.getItemType();
        if (stq.isSubQuantifier(testST.getQuantifier())) {
            if (it instanceof AnyItemType) {
                return true;
            } else if (it.isAtomicType() && testST.getItemType().isAtomicType()) {
                AtomicType ait = (AtomicType) it;
                AtomicType testIT = (AtomicType) testST.getItemType();
                if (BuiltinTypeRegistry.INSTANCE.isBuiltinTypeId(testIT.getTypeId())) {
                    SchemaType vType = BuiltinTypeRegistry.INSTANCE.getSchemaTypeById(testIT.getTypeId());
                    while (vType != null && vType.getTypeId() != ait.getTypeId()) {
                        vType = vType.getBaseType();
                    }
                    return vType != null;
                }
            } else if (it instanceof NodeType && testST.getItemType() instanceof NodeType) {
                NodeType nt = (NodeType) it;
                NodeKind kind = nt.getNodeKind();
                NodeType testNT = (NodeType) testST.getItemType();
                NodeKind testKind = testNT.getNodeKind();
                if (kind == NodeKind.ANY || kind == testKind) {
                    return true;
                }
            }
            return false;
        }
        return false;
    }

    private Quantifier getSequenceQuantifier(SequencePointable seqp) {
        switch (seqp.getEntryCount()) {
            case 0:
                return Quantifier.QUANT_ZERO;

            case 1:
                return Quantifier.QUANT_ONE;
        }
        return Quantifier.QUANT_PLUS;
    }

    public void setSequenceType(SequenceType sType) {
        this.sequenceType = sType;
    }

    public String toString() {
        return "sequenceMatcher[" + this.sequenceType + "]";
    }
}
