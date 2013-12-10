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

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.nodes.DocumentNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.ElementNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.NodeTreePointable;
import org.apache.vxquery.datamodel.builders.nodes.NodeSubTreeBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.step.NodeTestFilter.INodeFilter;
import org.apache.vxquery.types.SequenceType;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class ChildPathStep {
    private final DynamicContext dCtx;
    private List<INodeFilter> filter;
    private int indexSeqArgs;
    private int[] indexSequence;
    private ArrayBackedValueStorage nodeAbvs;
    private final NodeTreePointable ntp = (NodeTreePointable) NodeTreePointable.FACTORY.createPointable();
    private int seqArgsLength;
    private final SequencePointable seqNtp = (SequencePointable) SequencePointable.FACTORY.createPointable();
    private final TaggedValuePointable tvpItem = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();

    public ChildPathStep(IHyracksTaskContext ctx) {
        dCtx = (DynamicContext) ctx.getJobletContext().getGlobalJobData();
        nodeAbvs = new ArrayBackedValueStorage();
        filter = new ArrayList<INodeFilter>();
    }

    private void getSequence(TaggedValuePointable tvp, SequencePointable seqp) {
        switch (tvp.getTag()) {
            case ValueTag.DOCUMENT_NODE_TAG:
                DocumentNodePointable dnp = (DocumentNodePointable) DocumentNodePointable.FACTORY.createPointable();
                tvp.getValue(dnp);
                dnp.getContent(ntp, seqp);
                return;

            case ValueTag.ELEMENT_NODE_TAG:
                ElementNodePointable enp = (ElementNodePointable) ElementNodePointable.FACTORY.createPointable();
                tvp.getValue(enp);
                if (enp.childrenChunkExists()) {
                    enp.getChildrenSequence(ntp, seqp);
                    return;
                }
        }
        TaggedValuePointable seqTvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        XDMConstants.setEmptySequence(seqTvp);
        seqTvp.getValue(seqp);
    }

    public void init(TaggedValuePointable tvp, List<Integer> typeCodes) throws SystemException {
        indexSequence = new int[typeCodes.size()];
        for (int i = 0; i < typeCodes.size(); ++i) {
            indexSequence[i] = 0;
        }
        indexSeqArgs = 0;

        setFilterCode(typeCodes);

        if (tvp.getTag() != ValueTag.NODE_TREE_TAG) {
            throw new SystemException(ErrorCode.SYSE0001);
        }
        tvp.getValue(ntp);
        seqArgsLength = -1;
    }

    protected void init(TaggedValuePointable[] args) throws SystemException {
        indexSequence = new int[1];
        indexSequence[0] = 0;
        indexSeqArgs = 0;

        if (args[1].getTag() != ValueTag.XS_INT_TAG) {
            throw new IllegalArgumentException("Expected int value tag, got: " + args[1].getTag());
        }
        IntegerPointable ip = (IntegerPointable) IntegerPointable.FACTORY.createPointable();
        args[1].getValue(ip);
        int typeCode = ip.getInteger();
        setFilterCode(typeCode);

        if (args[0].getTag() == ValueTag.SEQUENCE_TAG) {
            args[0].getValue(seqNtp);
            seqArgsLength = seqNtp.getEntryCount();
        } else if (args[0].getTag() == ValueTag.NODE_TREE_TAG) {
            args[0].getValue(ntp);
            seqArgsLength = -1;
        } else {
            throw new SystemException(ErrorCode.SYSE0001);
        }
    }

    public void setFilterCode(int typeCode) {
        SequenceType sType = dCtx.getStaticContext().lookupSequenceType(typeCode);
        filter.add(NodeTestFilter.getNodeTestFilter(sType));
    }

    private void setFilterCode(List<Integer> typeCodes) {
        for (int typeCode : typeCodes) {
            SequenceType sType = dCtx.getStaticContext().lookupSequenceType(typeCode);
            INodeFilter f = NodeTestFilter.getNodeTestFilter(sType);
            filter.add(f);
        }
    }

    private void setNodeToResult(IPointable result) throws IOException {
        nodeAbvs.reset();
        NodeSubTreeBuilder nstb = new NodeSubTreeBuilder();
        nstb.reset(nodeAbvs);
        nstb.setChildNode(ntp, tvpItem);
        nstb.finish();
        result.set(nodeAbvs.getByteArray(), nodeAbvs.getStartOffset(), nodeAbvs.getLength());
    }

    public boolean step(IPointable result) throws AlgebricksException {
        TaggedValuePointable tvpRoot = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        if (seqArgsLength > 0) {
            while (indexSeqArgs < seqArgsLength) {
                TaggedValuePointable tvpNtp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
                seqNtp.getEntry(indexSeqArgs, tvpNtp);
                if (tvpNtp.getTag() != ValueTag.NODE_TREE_TAG) {
                    String description = ErrorCode.SYSE0001 + ": " + ErrorCode.SYSE0001.getDescription();
                    throw new AlgebricksException(description);
                }
                tvpNtp.getValue(ntp);
                ntp.getRootNode(tvpRoot);
                if (stepNodeTree(tvpRoot, 0, result)) {
                    return true;
                }
            }
        } else {
            // Single node tree input.
            ntp.getRootNode(tvpRoot);
            if (stepNodeTree(tvpRoot, 0, result)) {
                return true;
            }
        }
        return false;
    }

    private boolean stepNodeTree(TaggedValuePointable tvpInput, int level, IPointable result) throws AlgebricksException {
        SequencePointable seqItem = (SequencePointable) SequencePointable.FACTORY.createPointable();
        getSequence(tvpInput, seqItem);
        int seqLength = seqItem.getEntryCount();
        while (indexSequence[level] < seqLength) {
            // Get the next item
            seqItem.getEntry(indexSequence[level], tvpItem);

            // Test to see if the item fits the path step
            if (filter.get(level).accept(ntp, tvpItem)) {
                if (level + 1 < indexSequence.length) {
                    if (stepNodeTree(tvpItem, level + 1, result)) {
                        return true;
                    }
                } else {
                    try {
                        setNodeToResult(result);
                        ++indexSequence[level];
                        return true;
                    } catch (IOException e) {
                        String description = ErrorCode.SYSE0001 + ": " + ErrorCode.SYSE0001.getDescription();
                        throw new AlgebricksException(description);
                    }
                }
            }
            ++indexSequence[level];
        }
        // Reset for next node tree.
        indexSequence[level] = 0;
        return false;
    }
}