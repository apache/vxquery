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

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.nodes.NodeTreePointable;
import org.apache.vxquery.datamodel.builders.nodes.NodeSubTreeBuilder;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.step.NodeTestFilter.INodeFilter;
import org.apache.vxquery.types.SequenceType;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public abstract class AbstractPathStepScalarEvaluator extends AbstractTaggedValueArgumentScalarEvaluator {
    protected final SequenceBuilder seqb;

    protected final NodeTreePointable ntp;

    private final ArrayBackedValueStorage nodeAbvs;

    protected final TaggedValuePointable itemTvp;

    private INodeFilter filter;

    public AbstractPathStepScalarEvaluator(IScalarEvaluator[] args, IHyracksTaskContext ctx) {
        super(args);
        ntp = (NodeTreePointable) NodeTreePointable.FACTORY.createPointable();
        seqb = new SequenceBuilder();
        nodeAbvs = new ArrayBackedValueStorage();
        itemTvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    }

    protected void setNodeTest(SequenceType sType) {
        filter = NodeTestFilter.getNodeTestFilter(sType);
    }

    @Override
    protected abstract void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException;

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

    protected void appendNodeToResult() throws IOException {
        TaggedValuePointable node = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        setNodeToResult(node);
        seqb.addItem(node);
    }
}