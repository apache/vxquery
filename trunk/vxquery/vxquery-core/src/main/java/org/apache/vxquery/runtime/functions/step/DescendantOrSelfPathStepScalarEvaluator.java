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

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.types.ElementType;
import org.apache.vxquery.types.Quantifier;
import org.apache.vxquery.types.SequenceType;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class DescendantOrSelfPathStepScalarEvaluator extends AbstractDescendantPathStepScalarEvaluator {
    private final TaggedValuePointable rootTVP;

    private final IntegerPointable ip;

    private final ArrayBackedValueStorage seqAbvs;

    public DescendantOrSelfPathStepScalarEvaluator(IScalarEvaluator[] args, IHyracksTaskContext ctx) {
        super(args, ctx);
        rootTVP = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        ip = (IntegerPointable) IntegerPointable.FACTORY.createPointable();
        seqAbvs = new ArrayBackedValueStorage();
    }

    @Override
    protected final void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
        try {
            if (args[0].getTag() != ValueTag.NODE_TREE_TAG) {
                throw new SystemException(ErrorCode.SYSE0001);
            }
            args[0].getValue(ntp);

            // Set up the result sequence and get the root node.
            seqAbvs.reset();
            seqb.reset(seqAbvs);
            ntp.getRootNode(rootTVP);

            // Solve for self.
            if (args[1].getTag() != ValueTag.XS_INT_TAG) {
                throw new IllegalArgumentException("Expected int value tag, got: " + args[1].getTag());
            }
            args[1].getValue(ip);
            int typeCode = ip.getInteger();
            SequenceType sType = dCtx.getStaticContext().lookupSequenceType(typeCode);
            setNodeTest(sType);
            rootTVP.set(itemTvp);
            if (matches()) {
                appendNodeToResult();
            }

            // Solve for descendants.
            setNodeTest(SequenceType.create(ElementType.ANYELEMENT, Quantifier.QUANT_ONE));
            searchSubtree(rootTVP);

            seqb.finish();
            result.set(seqAbvs);
        } catch (IOException e) {
            throw new SystemException(ErrorCode.SYSE0001, e);
        }
    }

}