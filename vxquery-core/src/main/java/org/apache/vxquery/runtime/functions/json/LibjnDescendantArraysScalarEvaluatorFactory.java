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
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.jsonitem.ArrayPointable;
import org.apache.vxquery.datamodel.accessors.jsonitem.ObjectPointable;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;

public class LibjnDescendantArraysScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    public LibjnDescendantArraysScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final SequencePointable sp = (SequencePointable) SequencePointable.FACTORY.createPointable();
        final ArrayPointable ap = (ArrayPointable) ArrayPointable.FACTORY.createPointable();
        final ObjectPointable op = (ObjectPointable) ObjectPointable.FACTORY.createPointable();
        final UTF8StringPointable stringp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        final SequenceBuilder sb = new SequenceBuilder();
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {

            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                abvs.reset();
                sb.reset(abvs);
                TaggedValuePointable tvp = args[0];
                if (tvp.getTag() == ValueTag.SEQUENCE_TAG) {
                    TaggedValuePointable tempTvp = ppool.takeOne(TaggedValuePointable.class);
                    tvp.getValue(sp);
                    int size = sp.getEntryCount();
                    for (int i = 0; i < size; i++) {
                        sp.getEntry(i, tempTvp);
                        if (tempTvp.getTag() == ValueTag.ARRAY_TAG) {
                            nested(tempTvp, ap);
                        }
                        if (tempTvp.getTag() == ValueTag.OBJECT_TAG) {
                            insideObject(tempTvp);
                        }
                    }
                    ppool.giveBack(tempTvp);
                } else if (tvp.getTag() == ValueTag.ARRAY_TAG) {
                    nested(tvp, ap);
                } else if (tvp.getTag() == ValueTag.OBJECT_TAG) {
                    insideObject(tvp);
                } else {
                    XDMConstants.setEmptySequence(tvp);
                }
                try {
                    sb.finish();
                    result.set(abvs);
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
            }

            public void nested(TaggedValuePointable tvp, ArrayPointable ap) throws SystemException {
                TaggedValuePointable tempTvp = ppool.takeOne(TaggedValuePointable.class);
                ArrayPointable tempAp = ppool.takeOne(ArrayPointable.class);
                appendSequence(tvp, ap);
                int size = ap.getEntryCount();
                for (int i = 0; i < size; i++) {

                    ap.getEntry(i, tempTvp);
                    if (tempTvp.getTag() == ValueTag.ARRAY_TAG) {
                        nested(tempTvp, tempAp);
                    }
                }
                ppool.giveBack(tempTvp);
                ppool.giveBack(tempAp);
            }

            public void appendSequence(TaggedValuePointable tvp, ArrayPointable ap) throws SystemException {
                tvp.getValue(ap);
                try {
                    sb.addItem(tvp);
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
            }

            public void insideObject(TaggedValuePointable tvp) throws SystemException {
                boolean inObject = false;
                tvp.getValue(op);
                TaggedValuePointable tempTvp = ppool.takeOne(TaggedValuePointable.class);
                try {
                    op.getKeys(tvp);
                    tvp.getValue(stringp);
                    op.getValue(stringp, tempTvp);
                } catch (IOException e1) {
                    throw new SystemException(ErrorCode.SYSE0001, e1);
                }
                if (tempTvp.getTag() == ValueTag.OBJECT_TAG) {
                    inObject = true;
                }
                if (inObject) {
                    insideObject(tempTvp);
                } else {
                    nested(tempTvp, ap);
                }
                ppool.giveBack(tempTvp);
            }
        };
    }

}
