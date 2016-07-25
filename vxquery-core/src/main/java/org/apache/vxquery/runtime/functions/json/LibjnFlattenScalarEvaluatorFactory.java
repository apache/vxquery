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
package org.apache.vxquery.runtime.functions.json;

import java.io.IOException;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.AbstractSequencePointable;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.jsonitem.ArrayPointable;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;

public class LibjnFlattenScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    public LibjnFlattenScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final SequencePointable sp = (SequencePointable) SequencePointable.FACTORY.createPointable();
        final AbstractSequencePointable asp1 = new AbstractSequencePointable();
        final AbstractSequencePointable asp2 = new AbstractSequencePointable();
        final ArrayPointable ap = (ArrayPointable) ArrayPointable.FACTORY.createPointable();
        final ArrayPointable ap1 = (ArrayPointable) ArrayPointable.FACTORY.createPointable();
        final SequenceBuilder sb = new SequenceBuilder();
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {

            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                TaggedValuePointable tvp = args[0];
                TaggedValuePointable tempTvp = ppool.takeOne(TaggedValuePointable.class);
                tvp.getValue(sp);
                abvs.reset();
                sb.reset(abvs);
                int size = sp.getEntryCount();
                for (int i = 0; i < size; i++) {
                    sp.getEntry(i, tempTvp);
                    if (tempTvp.getTag() == ValueTag.ARRAY_TAG) {
                        flatten(tempTvp);
                    } else {
                        try {
                            sb.addItem(tempTvp);
                        } catch (IOException e) {
                            throw new SystemException(ErrorCode.SYSE0001, e);
                        }
                    }
                }
                ppool.giveBack(tempTvp);
                try {
                    sb.finish();
                    result.set(abvs);
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
            }

            public void flatten(TaggedValuePointable tvp) throws SystemException {
                TaggedValuePointable tvp1 = ppool.takeOne(TaggedValuePointable.class);
                ArrayPointable ap=ppool.takeOne(ArrayPointable.class);
                tvp.getValue(ap);
                int size = ap.getEntryCount();
                for (int i = 0; i < size; i++) {
                    ap.getEntry(i, tvp1);
                    if (tvp1.getTag() == ValueTag.ARRAY_TAG) {
                        flatten(tvp1);
                    } else {
                        try {
                            sb.addItem(tvp1);
                        } catch (IOException e) {
                            throw new SystemException(ErrorCode.SYSE0001, e);
                        }
                    }
                }
                ppool.giveBack(tvp1);
                ppool.giveBack(ap);
            }
        };
    }

}
