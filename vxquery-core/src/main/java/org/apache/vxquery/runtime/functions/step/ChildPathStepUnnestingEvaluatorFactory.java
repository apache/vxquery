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
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentUnnestingEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentUnnestingEvaluatorFactory;
import org.apache.vxquery.runtime.functions.step.NodeTestFilter.INodeFilter;
import org.apache.vxquery.types.SequenceType;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IUnnestingEvaluator;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class ChildPathStepUnnestingEvaluatorFactory extends AbstractTaggedValueArgumentUnnestingEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public ChildPathStepUnnestingEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IUnnestingEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args) throws AlgebricksException {

        final SequencePointable seqp = (SequencePointable) SequencePointable.FACTORY.createPointable();
        final TaggedValuePointable rootTVP = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final DocumentNodePointable dnp = (DocumentNodePointable) DocumentNodePointable.FACTORY.createPointable();
        final ElementNodePointable enp = (ElementNodePointable) ElementNodePointable.FACTORY.createPointable();
        final IntegerPointable ip = (IntegerPointable) IntegerPointable.FACTORY.createPointable();
        final NodeTreePointable ntp = (NodeTreePointable) NodeTreePointable.FACTORY.createPointable();
        final TaggedValuePointable itemTvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final DynamicContext dCtx = (DynamicContext) ctx.getJobletContext().getGlobalJobData();

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
                    filter = NodeTestFilter.getNodeTestFilter(sType);
                    first = false;
                }
                if (args[0].getTag() != ValueTag.NODE_TREE_TAG) {
                    throw new SystemException(ErrorCode.SYSE0001);
                }
                args[0].getValue(ntp);
                getSequence(ntp, seqp);
                seqLength = seqp.getEntryCount();
            }

            protected boolean matches() {
                return filter.accept(ntp, itemTvp);
            }

            protected void setNodeToResult(IPointable result) throws IOException {
                nodeAbvs.reset();
                NodeSubTreeBuilder nstb = new NodeSubTreeBuilder();
                nstb.reset(nodeAbvs);
                nstb.setChildNode(ntp, itemTvp);
                nstb.finish();
                result.set(nodeAbvs.getByteArray(), nodeAbvs.getStartOffset(), nodeAbvs.getLength());
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
                TaggedValuePointable seqTvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
                XDMConstants.setEmptySequence(seqTvp);
                seqTvp.getValue(seqp);
            }

        };
    }
}