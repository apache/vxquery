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

public class ChildPathStepUnnesting extends AbstractForwardAxisPathStep {
    private int indexSeqArgs;
    private int seqArgsLength;
    private int indexSequence;
    private final IntegerPointable ip = (IntegerPointable) IntegerPointable.FACTORY.createPointable();
    private final SequencePointable seqItem = (SequencePointable) SequencePointable.FACTORY.createPointable();
    private final SequencePointable seqNtp = (SequencePointable) SequencePointable.FACTORY.createPointable();
    private final TaggedValuePointable tvpItem = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    private final TaggedValuePointable tvpNtp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    private final TaggedValuePointable tvpStep = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    INodeFilter filter;
    int filterLookupID = -1;

    public ChildPathStepUnnesting(IHyracksTaskContext ctx, PointablePool pp) {
        super(ctx, pp);
    }

    protected void init(TaggedValuePointable[] args) throws HyracksDataException {
        indexSeqArgs = 0;
        indexSequence = 0;

        if (args[1].getTag() != ValueTag.XS_INT_TAG) {
            throw new IllegalArgumentException("Expected int value tag, got: " + args[1].getTag());
        }
        args[1].getValue(ip);
        if (ip.getInteger() != filterLookupID) {
            filterLookupID = ip.getInteger();
            SequenceType sType = dCtx.getStaticContext().lookupSequenceType(ip.getInteger());
            filter = NodeTestFilter.getNodeTestFilter(sType);
        }

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
                if (stepNodeTree(tvpStep, 0, result)) {
                    return true;
                }
                indexSeqArgs++;
            }
        } else {
            // Single node tree input.
            ntp.getRootNode(tvpStep);
            if (stepNodeTree(tvpStep, 0, result)) {
                return true;
            }
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
     * @throws HyracksDataException
     *             Could not save result.
     */
    protected boolean stepNodeTree(TaggedValuePointable tvpInput, int level, IPointable result)
            throws HyracksDataException {
        getSequence(tvpInput, seqItem);
        int seqLength = seqItem.getEntryCount();
        while (indexSequence < seqLength) {
            // Get the next item
            seqItem.getEntry(indexSequence, tvpItem);

            // Test to see if the item fits the path step
            if (filter.accept(ntp, tvpItem)) {
                try {
                    setNodeToResult(tvpItem, result);
                    ++indexSequence;
                    return true;
                } catch (IOException e) {
                    String description = ErrorCode.SYSE0001 + ": " + ErrorCode.SYSE0001.getDescription();
                    throw new HyracksDataException(description);
                }
            }
            ++indexSequence;
        }
        // Reset for next node tree.
        indexSequence = 0;
        return false;
    }

}
