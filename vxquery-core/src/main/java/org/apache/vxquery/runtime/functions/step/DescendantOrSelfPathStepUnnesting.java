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

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.vxquery.datamodel.accessors.PointablePool;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.step.NodeTestFilter.INodeFilter;
import org.apache.vxquery.types.SequenceType;

public class DescendantOrSelfPathStepUnnesting extends AbstractForwardAxisPathStep {
    private boolean testSelf;
    private boolean returnSelf;
    private int indexSeqArgs;
    private int seqArgsLength;
    private List<Integer> indexSequence = new ArrayList<Integer>();
    private List<Integer> returnSequence = new ArrayList<Integer>();

    private final IntegerPointable ip = (IntegerPointable) IntegerPointable.FACTORY.createPointable();
    private final SequencePointable seqNtp = (SequencePointable) SequencePointable.FACTORY.createPointable();
    private final TaggedValuePointable tvpItem = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    private final TaggedValuePointable tvpNtp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    private final TaggedValuePointable tvpStep = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    private INodeFilter filter;
    private int filterLookupID = -1;
    private boolean isfilter = false;

    public DescendantOrSelfPathStepUnnesting(IHyracksTaskContext ctx, PointablePool pp, boolean testSelf) {
        super(ctx, pp);
        this.testSelf = testSelf;
    }

    protected void init(TaggedValuePointable[] args) throws SystemException {
        returnSelf = true;
        indexSeqArgs = 0;
        indexSequence.add(0);
        returnSequence.add(0);

        if (args.length > 1) {
            isfilter = true;
            if (args[1].getTag() != ValueTag.XS_INT_TAG) {
                throw new IllegalArgumentException("Expected int value tag, got: " + args[1].getTag());
            }
            args[1].getValue(ip);
            if (ip.getInteger() != filterLookupID) {
                filterLookupID = ip.getInteger();
                SequenceType sType = dCtx.getStaticContext().lookupSequenceType(ip.getInteger());
                filter = NodeTestFilter.getNodeTestFilter(sType);
            }
        }
        // Check the argument passed in as sequence or node tree.
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

    public boolean step(IPointable result) throws HyracksDataException {
        if (seqArgsLength > 0) {
            while (indexSeqArgs < seqArgsLength) {
                seqNtp.getEntry(indexSeqArgs, tvpNtp);
                if (tvpNtp.getTag() != ValueTag.NODE_TREE_TAG) {
                    String description = ErrorCode.SYSE0001 + ": " + ErrorCode.SYSE0001.getDescription();
                    throw new HyracksDataException(description);
                }
                tvpNtp.getValue(ntp);
                ntp.getRootNode(tvpStep);
                if (processNodeTree(tvpStep, result)) {
                    return true;
                }
                // Next node tree in sequence.
                indexSeqArgs++;
                returnSelf = true;
            }
        } else {
            // Single node tree input.
            ntp.getRootNode(tvpStep);
            if (processNodeTree(tvpStep, result)) {
                return true;
            }
        }
        return false;
    }

    private boolean processNodeTree(TaggedValuePointable rootTVP, IPointable result) throws HyracksDataException {
        if (testSelf && returnSelf) {
            returnSelf = false;
            tvpItem.set(rootTVP);
            try {
                if (!isfilter || (isfilter && filter.accept(ntp, tvpItem))) {
                    setNodeToResult(tvpItem, result);
                    return true;
                }
            } catch (IOException e) {
                String description = ErrorCode.SYSE0001 + ": " + ErrorCode.SYSE0001.getDescription();
                throw new HyracksDataException(description);
            }
        }
        // Solve for descendants.
        return stepNodeTree(rootTVP, 0, result);
    }

    /**
     * Search through all tree children and children's children.
     *
     * @param tvpInput
     *            pointable
     * @param level
     *            level
     * @param result
     *            result
     * @return found result
     * @throws HyracksDataException
     *             Could not save result.
     */
    protected boolean stepNodeTree(TaggedValuePointable tvpInput, int level, IPointable result)
            throws HyracksDataException {
        // Set up next level tracking.
        if (level + 1 > indexSequence.size()) {
            indexSequence.add(0);
            returnSequence.add(0);
        }

        SequencePointable seqItem = pp.takeOne(SequencePointable.class);
        try {
            getSequence(tvpInput, seqItem);
            int seqLength = seqItem.getEntryCount();
            while (indexSequence.get(level) < seqLength) {
                // Get the next item
                seqItem.getEntry(indexSequence.get(level), tvpItem);
                // Check current node
                if (indexSequence.get(level) == returnSequence.get(level)) {
                    returnSequence.set(level, returnSequence.get(level) + 1);
                    if (!isfilter || (isfilter && filter.accept(ntp, tvpItem))) {
                        setNodeToResult(tvpItem, result);
                        return true;
                    }
                }
                // Check children nodes
                if (level + 1 <= indexSequence.size()) {
                    if (stepNodeTree(tvpItem, level + 1, result)) {
                        return true;
                    }
                }
                indexSequence.set(level, indexSequence.get(level) + 1);
            }
            // Reset for next node tree.
            if (level == 0) {
                indexSequence.set(level, 0);
                returnSequence.set(level, 0);
            } else {
                indexSequence.remove(level);
                returnSequence.remove(level);
            }
            return false;
        } catch (IOException e) {
            String description = ErrorCode.SYSE0001 + ": " + ErrorCode.SYSE0001.getDescription();
            throw new HyracksDataException(description);
        } finally {
            pp.giveBack(seqItem);
        }
    }
}
