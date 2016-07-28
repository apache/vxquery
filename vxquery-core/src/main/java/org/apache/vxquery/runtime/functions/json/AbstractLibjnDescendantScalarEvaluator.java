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

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
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
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;

import java.io.IOException;

public abstract class AbstractLibjnDescendantScalarEvaluator extends AbstractTaggedValueArgumentScalarEvaluator {
    protected final IHyracksTaskContext ctx;
    protected final UTF8StringPointable stringp;
    protected final SequencePointable sp, sp1;
    protected final SequenceBuilder sb;
    protected final ArrayBackedValueStorage abvs;

    public AbstractLibjnDescendantScalarEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args) {
        super(args);
        this.ctx = ctx;
        stringp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        sp = (SequencePointable) SequencePointable.FACTORY.createPointable();
        sp1 = (SequencePointable) SequencePointable.FACTORY.createPointable();
        sb = new SequenceBuilder();
        abvs = new ArrayBackedValueStorage();
    }

    @Override
    protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
        try {
            abvs.reset();
            sb.reset(abvs);
            TaggedValuePointable tvp = args[0];
            if (tvp.getTag() == ValueTag.SEQUENCE_TAG) {
                TaggedValuePointable tempTvp = ppool.takeOne(TaggedValuePointable.class);
                tvp.getValue(sp);
                int size = sp.getEntryCount();
                for (int i = 0; i < size; i++) {
                    sp.getEntry(i, tempTvp);
                    process(tempTvp);
                }
                ppool.giveBack(tempTvp);
            } else {
                process(tvp);
            }
            sb.finish();
            result.set(abvs);
        } catch (IOException e) {
            throw new SystemException(ErrorCode.SYSE0001, e);
        }
    }

    protected void processObject(TaggedValuePointable tvp) throws IOException {
        TaggedValuePointable tempTvp = ppool.takeOne(TaggedValuePointable.class);
        ObjectPointable tempOp = ppool.takeOne(ObjectPointable.class);
        tvp.getValue(tempOp);
        tempOp.getKeys(tempTvp);
        if (tempTvp.getTag() == ValueTag.XS_STRING_TAG) {
            processPair(tempTvp, tempOp);
            process(tempTvp);
        } else if (tempTvp.getTag() == ValueTag.SEQUENCE_TAG) {
            tempTvp.getValue(sp1);
            int size = sp1.getEntryCount();
            for (int i = 0; i < size; i++) {
                sp1.getEntry(i, tempTvp);
                processPair(tempTvp, tempOp);
                process(tempTvp);
            }
        }
        ppool.giveBack(tempOp);
        ppool.giveBack(tempTvp);
    }

    protected abstract void processPair(TaggedValuePointable tempTvp, ObjectPointable tempOp) throws IOException;

    protected void process(TaggedValuePointable tvp) throws IOException {
        if (tvp.getTag() == ValueTag.OBJECT_TAG) {
            processObject(tvp);
        } else if (tvp.getTag() == ValueTag.ARRAY_TAG) {
            processArray(tvp);
        }
    }

    protected void processArray(TaggedValuePointable tvp) throws IOException {
        ArrayPointable ap = ppool.takeOne(ArrayPointable.class);
        tvp.getValue(ap);
        TaggedValuePointable tempTvp = ppool.takeOne(TaggedValuePointable.class);
        for (int i = 0; i < ap.getEntryCount(); i++) {
            ap.getEntry(i, tempTvp);
            process(tempTvp);
        }
        ppool.giveBack(ap);
        ppool.giveBack(tempTvp);
    }
}
