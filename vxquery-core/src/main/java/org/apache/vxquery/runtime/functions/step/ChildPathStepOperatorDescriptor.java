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
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.step.NodeTestFilter.INodeFilter;
import org.apache.vxquery.types.SequenceType;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.api.IPointable;

public class ChildPathStepOperatorDescriptor extends AbstractForwardAxisPathStep {
    private List<INodeFilter> filter = new ArrayList<INodeFilter>();
    private int[] indexSequence;
    private final TaggedValuePointable tvpItem = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    private final TaggedValuePointable tvpStep = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();

    public ChildPathStepOperatorDescriptor(IHyracksTaskContext ctx, PointablePool pp) {
        super(ctx, pp);
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

    /**
     * Single node tree input.
     * Requirement: "ntp" must be the node tree.
     *
     * @param result
     *            Node tree pointable
     * @return found result
     * @throws AlgebricksException
     *             Could not save result.
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
     *
     * @param tvpInput
     *            pointable
     * @param level
     *            level
     * @param result
     *            result
     * @return found result
     * @throws AlgebricksException
     *             Could not save result.
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
