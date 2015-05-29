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
import org.apache.vxquery.datamodel.accessors.PointablePool;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.nodes.DocumentNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.ElementNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.NodeTreePointable;
import org.apache.vxquery.datamodel.builders.nodes.NodeSubTreeBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.datamodel.values.XDMConstants;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public abstract class AbstractForwardAxisPathStep {
    protected final DynamicContext dCtx;
    protected final PointablePool pp;
    protected final NodeTreePointable ntp = (NodeTreePointable) NodeTreePointable.FACTORY.createPointable();
    private final ArrayBackedValueStorage nodeAbvs = new ArrayBackedValueStorage();
    private final TaggedValuePointable tvpConvert = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    private final DocumentNodePointable dnp = (DocumentNodePointable) DocumentNodePointable.FACTORY.createPointable();
    private final ElementNodePointable enp = (ElementNodePointable) ElementNodePointable.FACTORY.createPointable();
    private final NodeSubTreeBuilder nstb = new NodeSubTreeBuilder();

    public AbstractForwardAxisPathStep(IHyracksTaskContext ctx, PointablePool pp) {
        dCtx = (DynamicContext) ctx.getJobletContext().getGlobalJobData();
        this.pp = pp;
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
        XDMConstants.setEmptySequence(tvpConvert);
        tvpConvert.getValue(seqp);
    }

    protected void setNodeToResult(TaggedValuePointable tvpItem, IPointable result) throws IOException {
        nodeAbvs.reset();
        nstb.reset(nodeAbvs);
        nstb.setChildNode(ntp, tvpItem);
        nstb.finish();
        result.set(nodeAbvs.getByteArray(), nodeAbvs.getStartOffset(), nodeAbvs.getLength());
    }
}