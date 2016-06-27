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

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.jsonitem.ArrayPointable;
import org.apache.vxquery.datamodel.accessors.jsonitem.ObjectPointable;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;

public class ValueScalarEvaluator extends AbstractTaggedValueArgumentScalarEvaluator {
    protected final IHyracksTaskContext ctx;
    private final ArrayBackedValueStorage mvs;
    private final ArrayPointable ap;
    private final LongPointable lp;
    private final SequenceBuilder sb;
    private final ObjectPointable op;
    private final UTF8StringPointable sp;
    protected DataOutput out;

    public ValueScalarEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args) {
        super(args);
        this.ctx = ctx;
        ap = (ArrayPointable) ArrayPointable.FACTORY.createPointable();
        op = (ObjectPointable) ObjectPointable.FACTORY.createPointable();
        sp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        lp = (LongPointable) LongPointable.FACTORY.createPointable();
        mvs = new ArrayBackedValueStorage();
        sb = new SequenceBuilder();
    }

    @Override
    protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
        TaggedValuePointable tvp1 = args[0];
        TaggedValuePointable tvp2 = args[1];
        if (!(tvp1.getTag() == ValueTag.ARRAY_TAG || tvp1.getTag() == ValueTag.OBJECT_TAG)) {
            throw new SystemException(ErrorCode.FORG0006);
        }
        if (tvp1.getTag() == ValueTag.ARRAY_TAG) {
            if (tvp2.getTag() != ValueTag.XS_INTEGER_TAG) {
                throw new SystemException(ErrorCode.FORG0006);
            }
            TaggedValuePointable tempTvp = ppool.takeOne(TaggedValuePointable.class);
            mvs.reset();
            try {
                sb.reset(mvs);
                tvp1.getValue(ap);
                tvp2.getValue(lp);
                if ((int) lp.getLong() > ap.getEntryCount()) {
                    XDMConstants.setEmptySequence(result);
                    return;
                }
                ap.getEntry((int) lp.getLong() - 1, tempTvp);
                sb.addItem(ap.getEntryCount() != 0 ? tempTvp : tvp1);
                sb.finish();
                result.set(mvs);
            } catch (IOException e) {
                throw new SystemException(ErrorCode.SYSE0001, e);
            } finally {
                ppool.giveBack(tempTvp);
            }
        } else if (tvp1.getTag() == ValueTag.OBJECT_TAG) {
            if (tvp2.getTag() != ValueTag.XS_STRING_TAG) {
                throw new SystemException(ErrorCode.FORG0006);
            }
            TaggedValuePointable tempTvp = ppool.takeOne(TaggedValuePointable.class);
            try {
                mvs.reset();
                sb.reset(mvs);
                tvp1.getValue(op);
                tvp2.getValue(sp);
                if (op.getValue(sp, tempTvp)) {
                    sb.addItem(tempTvp);
                }
                sb.finish();
                result.set(mvs);
            } catch (IOException e) {
                throw new SystemException(ErrorCode.SYSE0001, e);

            }finally {
                ppool.giveBack(tempTvp);
            }
        }
    }

}
