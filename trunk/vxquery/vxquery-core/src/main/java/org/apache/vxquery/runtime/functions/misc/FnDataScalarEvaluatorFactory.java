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
package org.apache.vxquery.runtime.functions.misc;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.PointablePool;
import org.apache.vxquery.datamodel.accessors.PointablePoolFactory;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.nodes.AttributeNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.DocumentNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.ElementNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.NodeTreePointable;
import org.apache.vxquery.datamodel.accessors.nodes.PINodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.TextOrCommentNodePointable;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class FnDataScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnDataScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final ArrayBackedValueStorage tempABVS = new ArrayBackedValueStorage();
        final SequenceBuilder sb = new SequenceBuilder();
        final SequencePointable seq = new SequencePointable();
        final TaggedValuePointable p = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final TaggedValuePointable tempTVP = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final TaggedValuePointable tempTVP2 = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final NodeTreePointable ntp = (NodeTreePointable) NodeTreePointable.FACTORY.createPointable();
        final VoidPointable vp = (VoidPointable) VoidPointable.FACTORY.createPointable();
        final PointablePool pp = PointablePoolFactory.INSTANCE.createPointablePool();
        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                try {
                    abvs.reset();
                    sb.reset(abvs);
                    TaggedValuePointable tvp = args[0];
                    if (tvp.getTag() == ValueTag.SEQUENCE_TAG) {
                        tvp.getValue(seq);
                        int seqLen = seq.getEntryCount();
                        for (int j = 0; j < seqLen; ++j) {
                            seq.getEntry(j, p);
                            atomize(p, sb);
                        }
                    } else {
                        atomize(tvp, sb);
                    }
                    sb.finish();
                    result.set(abvs);
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001);
                }
            }

            private void atomize(TaggedValuePointable tvp, SequenceBuilder sb) throws IOException {
                switch (tvp.getTag()) {
                    case ValueTag.NODE_TREE_TAG:
                        tvp.getValue(ntp);
                        atomizeNode(ntp, sb);
                        break;

                    default:
                        sb.addItem(tvp);
                }
            }

            private void atomizeNode(NodeTreePointable ntp, SequenceBuilder sb) throws IOException {
                ntp.getRootNode(tempTVP);
                switch (tempTVP.getTag()) {
                    case ValueTag.ATTRIBUTE_NODE_TAG: {
                        AttributeNodePointable anp = pp.takeOne(AttributeNodePointable.class);
                        try {
                            tempTVP.getValue(anp);
                            anp.getValue(ntp, vp);
                            sb.addItem(vp);
                        } finally {
                            pp.giveBack(anp);
                        }
                        break;
                    }

                    case ValueTag.TEXT_NODE_TAG:
                    case ValueTag.COMMENT_NODE_TAG: {
                        TextOrCommentNodePointable tcnp = pp.takeOne(TextOrCommentNodePointable.class);
                        try {
                            tempTVP.getValue(tcnp);
                            tcnp.getValue(ntp, vp);
                            tempABVS.reset();
                            tempABVS.getDataOutput().write(ValueTag.XS_UNTYPED_ATOMIC_TAG);
                            tempABVS.append(vp);
                            sb.addItem(tempABVS);
                        } finally {
                            pp.giveBack(tcnp);
                        }
                        break;
                    }

                    case ValueTag.DOCUMENT_NODE_TAG: {
                        DocumentNodePointable dnp = pp.takeOne(DocumentNodePointable.class);
                        SequencePointable sp = pp.takeOne(SequencePointable.class);
                        try {
                            tempTVP.getValue(dnp);
                            dnp.getContent(ntp, sp);
                            buildStringConcatenation(sp, tempABVS, ntp);
                            sb.addItem(tempABVS);
                        } finally {
                            pp.giveBack(sp);
                            pp.giveBack(dnp);
                        }
                        break;
                    }

                    case ValueTag.ELEMENT_NODE_TAG: {
                        ElementNodePointable enp = pp.takeOne(ElementNodePointable.class);
                        SequencePointable sp = pp.takeOne(SequencePointable.class);
                        try {
                            tempTVP.getValue(enp);
                            if (enp.childrenChunkExists()) {
                                enp.getChildrenSequence(ntp, sp);
                                buildStringConcatenation(sp, tempABVS, ntp);
                                sb.addItem(tempABVS);
                            }
                        } finally {
                            pp.giveBack(sp);
                            pp.giveBack(enp);
                        }
                        break;
                    }

                    case ValueTag.PI_NODE_TAG: {
                        PINodePointable pnp = pp.takeOne(PINodePointable.class);
                        try {
                            tempTVP.getValue(pnp);
                            pnp.getContent(ntp, vp);
                            tempABVS.reset();
                            tempABVS.getDataOutput().write(ValueTag.XS_UNTYPED_ATOMIC_TAG);
                            tempABVS.append(vp);
                            sb.addItem(tempABVS);
                        } finally {
                            pp.giveBack(pnp);
                        }
                        break;
                    }

                }
            }

            private void buildStringConcatenation(SequencePointable sp, ArrayBackedValueStorage tempABVS,
                    NodeTreePointable ntp) throws IOException {
                tempABVS.reset();
                DataOutput out = tempABVS.getDataOutput();
                out.write(ValueTag.XS_UNTYPED_ATOMIC_TAG);
                // Leave room for the utf-8 length
                out.write(0);
                out.write(0);
                buildConcatenationRec(sp, out, ntp);
                int utflen = tempABVS.getLength() - 3;
                byte[] bytes = tempABVS.getByteArray();
                // Patch utf-8 length at bytes 1 and 2
                bytes[1] = (byte) ((utflen >>> 8) & 0xFF);
                bytes[2] = (byte) ((utflen >>> 0) & 0xFF);
            }

            private void buildConcatenationRec(SequencePointable sp, DataOutput out, NodeTreePointable ntp)
                    throws IOException {
                int nItems = sp.getEntryCount();
                for (int i = 0; i < nItems; ++i) {
                    sp.getEntry(i, tempTVP2);
                    switch (tempTVP2.getTag()) {
                        case ValueTag.TEXT_NODE_TAG: {
                            TextOrCommentNodePointable tcnp = pp.takeOne(TextOrCommentNodePointable.class);
                            try {
                                tempTVP2.getValue(tcnp);
                                tcnp.getValue(ntp, vp);
                                out.write(vp.getByteArray(), vp.getStartOffset() + 2, vp.getLength() - 2);
                            } finally {
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
                                    buildConcatenationRec(sp2, out, ntp);
                                }
                            } finally {
                                pp.giveBack(sp2);
                                pp.giveBack(enp);
                            }
                        }
                    }
                }
            }
        };
    }
}