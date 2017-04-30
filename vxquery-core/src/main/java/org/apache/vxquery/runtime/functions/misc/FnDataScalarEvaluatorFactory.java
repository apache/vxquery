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
package org.apache.vxquery.runtime.functions.misc;

import java.io.IOException;

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.runtime.functions.util.AtomizeHelper;

public class FnDataScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnDataScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws HyracksDataException {
        return new FnDataScalarEvaluator(args);
    }

    private static class FnDataScalarEvaluator extends AbstractTaggedValueArgumentScalarEvaluator {
        final AtomizeHelper ah = new AtomizeHelper();
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final SequenceBuilder sb = new SequenceBuilder();
        final SequencePointable seq = (SequencePointable) SequencePointable.FACTORY.createPointable();
        final TaggedValuePointable p = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final TaggedValuePointable tempTVP = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();

        public FnDataScalarEvaluator(IScalarEvaluator[] args) {
            super(args);
        }

        @Override
        protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
            try {
                abvs.reset();
                sb.reset(abvs);
                TaggedValuePointable tvp = args[0];
                if (tvp.getTag() == ValueTag.SEQUENCE_TAG) {
                    tvp.getValue(seq);
                    int seqLen = seq.getEntryCount();
                    for (int j = 0; j < seqLen; ++j) {
                        seq.getEntry(j, p);
                        ah.atomize(p, ppool, tempTVP);
                        sb.addItem(tempTVP);
                    }
                } else {
                    ah.atomize(tvp, ppool, tempTVP);
                    sb.addItem(tempTVP);
                }
                sb.finish();
                result.set(abvs);
            } catch (SystemException e) {
                throw e;
            } catch (IOException e) {
                throw new SystemException(ErrorCode.SYSE0001);
            }
        }
    }
}
