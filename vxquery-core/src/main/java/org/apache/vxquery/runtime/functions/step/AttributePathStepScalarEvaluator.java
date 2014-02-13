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

import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.nodes.ElementNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.NodeTreePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.SystemException;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public class AttributePathStepScalarEvaluator extends AbstractSinglePathStepScalarEvaluator {
    private final TaggedValuePointable rootTVP;

    private final ElementNodePointable enp;

    public AttributePathStepScalarEvaluator(IScalarEvaluator[] args, IHyracksTaskContext ctx) {
        super(args, ctx);
        rootTVP = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        enp = (ElementNodePointable) ElementNodePointable.FACTORY.createPointable();
    }

    @Override
    protected void getSequence(NodeTreePointable ntp, SequencePointable seqp) throws SystemException {
        ntp.getRootNode(rootTVP);
        switch (rootTVP.getTag()) {
            case ValueTag.ELEMENT_NODE_TAG:
                rootTVP.getValue(enp);
                if (enp.attributesChunkExists()) {
                    enp.getAttributeSequence(ntp, seqp);
                    return;
                }
        }
        XDMConstants.setEmptySequence(seqp);
    }
}