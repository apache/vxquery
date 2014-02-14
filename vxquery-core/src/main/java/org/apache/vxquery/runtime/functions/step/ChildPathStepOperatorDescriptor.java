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
import java.util.ArrayList;
import java.util.List;

import org.apache.vxquery.datamodel.accessors.PointablePool;
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
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class ChildPathStepOperatorDescriptor extends AbstractChildPathStep {
    private List<INodeFilter> filter = new ArrayList<INodeFilter>();
    private int[] indexSequence;
    private final ArrayBackedValueStorage nodeAbvs = new ArrayBackedValueStorage();
    protected final NodeTreePointable ntp = (NodeTreePointable) NodeTreePointable.FACTORY.createPointable();
    private final TaggedValuePointable tvpItem = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    private final DocumentNodePointable dnp = (DocumentNodePointable) DocumentNodePointable.FACTORY.createPointable();
    private final ElementNodePointable enp = (ElementNodePointable) ElementNodePointable.FACTORY.createPointable();
    private final TaggedValuePointable tvpStep = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    private final NodeSubTreeBuilder nstb = new NodeSubTreeBuilder();

    public ChildPathStepOperatorDescriptor(IHyracksTaskContext ctx, PointablePool pp) {
        super(ctx, pp);
    }

    protected void getSequence(TaggedValuePointable tvp, SequencePointable seqp) {
        switch (tvp.getTag()) {
            case ValueTag.DOCUMENT_NODE_TAG:
                tvp.getValue(dnp);
                dnp.getContent(ntp, seqp);
                return;

            case ValueTag.ELEMENT_NODE_TAG:
                tvp.getValue(enp);
                if (enp.childrenChunkExists()) {
                    enp.getChildrenSequence(ntp, seqp);
                    return;
                }
        }
        XDMConstants.setEmptySequence(seqp);
    }

    public void init(TaggedValuePointable tvp, List<Integer> typeCodes) throws SystemException {
        indexSequence = new int[typeCodes.size()];
        for (int i = 0; i < typeCodes.size(); ++i) {
            indexSequence[i] = 0;
        }
        setFilterCode(typeCodes);
        if (tvp.getTag() != ValueTag.NODE_TREE_TAG) {
            throw new SystemException(ErrorCode.SYSE0001);
        }
        tvp.getValue(ntp);
    }

    protected void setFilterCode(List<Integer> typeCodes) {
        for (int typeCode : typeCodes) {
            SequenceType sType = dCtx.getStaticContext().lookupSequenceType(typeCode);
            INodeFilter f = NodeTestFilter.getNodeTestFilter(sType);
            filter.add(f);
        }
    }

    protected void setNodeToResult(TaggedValuePointable tvpItem, IPointable result) throws IOException {
        nodeAbvs.reset();
        nstb.reset(nodeAbvs);
        nstb.setChildNode(ntp, tvpItem);
        nstb.finish();
        result.set(nodeAbvs.getByteArray(), nodeAbvs.getStartOffset(), nodeAbvs.getLength());
    }

    /**
     * Single node tree input.
     * Requirement: "ntp" must be the node tree.
     */
    public boolean step(IPointable result) throws AlgebricksException {
        ntp.getRootNode(tvpStep);
        if (stepNodeTree(tvpStep, 0, result)) {
            return true;
        }
        return false;
    }

    /**
     * Find the next node to return.
     */
    protected boolean stepNodeTree(TaggedValuePointable tvpInput, int level, IPointable result)
            throws AlgebricksException {
        SequencePointable seqItem = pp.takeOne(SequencePointable.class);
        try {
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
                            setNodeToResult(tvpItem, result);
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
        } finally {
            pp.giveBack(seqItem);
        }
    }
}