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
package org.apache.vxquery.runtime.functions.util;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.PointablePool;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.nodes.AttributeNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.DocumentNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.ElementNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.NodeTreePointable;
import org.apache.vxquery.datamodel.accessors.nodes.PINodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.TextOrCommentNodePointable;
import org.apache.vxquery.datamodel.values.ValueTag;

import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;

public class AtomizeHelper {
    AttributeNodePointable anp = (AttributeNodePointable) AttributeNodePointable.FACTORY.createPointable();
    DocumentNodePointable dnp = (DocumentNodePointable) DocumentNodePointable.FACTORY.createPointable();
    ElementNodePointable enp = (ElementNodePointable) ElementNodePointable.FACTORY.createPointable();
    NodeTreePointable ntp = (NodeTreePointable) NodeTreePointable.FACTORY.createPointable();
    PINodePointable pnp = (PINodePointable) PINodePointable.FACTORY.createPointable();
    SequencePointable sp = (SequencePointable) SequencePointable.FACTORY.createPointable();
    TextOrCommentNodePointable tcnp = (TextOrCommentNodePointable) TextOrCommentNodePointable.FACTORY.createPointable();
    ArrayBackedValueStorage tempABVS = new ArrayBackedValueStorage();
    TaggedValuePointable tempTVP = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    VoidPointable vp = (VoidPointable) VoidPointable.FACTORY.createPointable();

    public void atomize(TaggedValuePointable tvp, PointablePool pp, IPointable result)
            throws SystemException, IOException {
        switch (tvp.getTag()) {
            case ValueTag.NODE_TREE_TAG:
                tvp.getValue(ntp);
                atomizeNode(ntp, pp, result);
                break;
            case ValueTag.ARRAY_TAG:
            case ValueTag.OBJECT_TAG:
                throw new SystemException(ErrorCode.JNTY0004);
            default:
                result.set(tvp);
        }
    }

    public void atomizeNode(NodeTreePointable ntp, PointablePool pp, IPointable result) throws IOException {
        ntp.getRootNode(tempTVP);
        switch (tempTVP.getTag()) {
            case ValueTag.ATTRIBUTE_NODE_TAG: {
                tempTVP.getValue(anp);
                anp.getValue(ntp, result);
                break;
            }

            case ValueTag.TEXT_NODE_TAG:
            case ValueTag.COMMENT_NODE_TAG: {
                tempTVP.getValue(tcnp);
                tcnp.getValue(ntp, vp);
                tempABVS.reset();
                tempABVS.getDataOutput().write(ValueTag.XS_UNTYPED_ATOMIC_TAG);
                tempABVS.append(vp);
                result.set(tempABVS.getByteArray(), tempABVS.getStartOffset(), tempABVS.getLength());
                break;
            }

            case ValueTag.DOCUMENT_NODE_TAG: {
                tempTVP.getValue(dnp);
                dnp.getContent(ntp, sp);
                buildStringConcatenation(sp, pp, tempABVS, ntp);
                result.set(tempABVS.getByteArray(), tempABVS.getStartOffset(), tempABVS.getLength());
                break;
            }

            case ValueTag.ELEMENT_NODE_TAG: {
                tempTVP.getValue(enp);
                if (enp.childrenChunkExists()) {
                    enp.getChildrenSequence(ntp, sp);
                    buildStringConcatenation(sp, pp, tempABVS, ntp);
                    result.set(tempABVS.getByteArray(), tempABVS.getStartOffset(), tempABVS.getLength());
                }
                break;
            }

            case ValueTag.PI_NODE_TAG: {
                tempTVP.getValue(pnp);
                pnp.getContent(ntp, vp);
                tempABVS.reset();
                tempABVS.getDataOutput().write(ValueTag.XS_UNTYPED_ATOMIC_TAG);
                tempABVS.append(vp);
                result.set(tempABVS.getByteArray(), tempABVS.getStartOffset(), tempABVS.getLength());
                break;
            }

        }
    }

    public static void buildConcatenationRec(SequencePointable sp, PointablePool pp, DataOutput out,
            NodeTreePointable ntp) throws IOException {
        TaggedValuePointable tempTVP2 = pp.takeOne(TaggedValuePointable.class);
        int nItems = sp.getEntryCount();
        for (int i = 0; i < nItems; ++i) {
            sp.getEntry(i, tempTVP2);
            switch (tempTVP2.getTag()) {
                case ValueTag.TEXT_NODE_TAG: {
                    TextOrCommentNodePointable tcnp = pp.takeOne(TextOrCommentNodePointable.class);
                    VoidPointable vp = pp.takeOne(VoidPointable.class);
                    try {
                        tempTVP2.getValue(tcnp);
                        tcnp.getValue(ntp, vp);
                        out.write(vp.getByteArray(), vp.getStartOffset() + 2, vp.getLength() - 2);
                    } finally {
                        pp.giveBack(vp);
                        pp.giveBack(tcnp);
                    }
                    break;
                }
                case ValueTag.ELEMENT_NODE_TAG: {
                    ElementNodePointable enp = pp.takeOne(ElementNodePointable.class);
                    SequencePointable sp2 = pp.takeOne(SequencePointable.class);
                    try {
                        tempTVP2.getValue(enp);
                        if (enp.childrenChunkExists()) {
                            enp.getChildrenSequence(ntp, sp2);
                            buildConcatenationRec(sp2, pp, out, ntp);
                        }
                    } finally {
                        pp.giveBack(sp2);
                        pp.giveBack(enp);
                    }
                }
            }
        }
        pp.giveBack(tempTVP2);
    }

    public static void buildStringConcatenation(SequencePointable sp, PointablePool pp,
            ArrayBackedValueStorage tempABVS, NodeTreePointable ntp) throws IOException {
        tempABVS.reset();
        DataOutput out = tempABVS.getDataOutput();
        out.write(ValueTag.XS_UNTYPED_ATOMIC_TAG);
        // Leave room for the utf-8 length
        out.write(0);
        out.write(0);
        buildConcatenationRec(sp, pp, out, ntp);
        int utflen = tempABVS.getLength() - 3;
        byte[] bytes = tempABVS.getByteArray();
        // Patch utf-8 length at bytes 1 and 2
        bytes[1] = (byte) ((utflen >>> 8) & 0xFF);
        bytes[2] = (byte) ((utflen >>> 0) & 0xFF);
    }

}
