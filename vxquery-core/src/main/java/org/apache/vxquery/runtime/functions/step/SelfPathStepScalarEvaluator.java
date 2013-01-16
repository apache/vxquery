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

import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.nodes.NodeTreePointable;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class SelfPathStepScalarEvaluator extends AbstractSinglePathStepScalarEvaluator {
    private final TaggedValuePointable rootTVP;

    final SequenceBuilder sb = new SequenceBuilder();

    private ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();

    public SelfPathStepScalarEvaluator(IScalarEvaluator[] args, IHyracksTaskContext ctx) {
        super(args, ctx);
        rootTVP = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    }

    @Override
    protected void getSequence(NodeTreePointable ntp, SequencePointable seqp) throws SystemException {
        ntp.getRootNode(rootTVP);

        // Create sequence with node.
        try {
            abvs.reset();
            sb.reset(abvs);
            sb.addItem(rootTVP);
            sb.finish();
            seqp.set(abvs);
        } catch (IOException e) {
            throw new SystemException(ErrorCode.SYSE0001);
        }
    }
}