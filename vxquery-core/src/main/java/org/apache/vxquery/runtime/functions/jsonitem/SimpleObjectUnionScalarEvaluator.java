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
package org.apache.vxquery.runtime.functions.jsonitem;

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.jsonitem.ObjectPointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;

import java.io.IOException;

public class SimpleObjectUnionScalarEvaluator extends AbstractObjectConstructorScalarEvaluator {

    private final SequencePointable sp, sp1;
    private ObjectPointable op;
    private TaggedValuePointable key;
    private final UTF8StringPointable stringKey;

    public SimpleObjectUnionScalarEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args) {
        super(ctx, args);
        sp = (SequencePointable) SequencePointable.FACTORY.createPointable();
        sp1 = (SequencePointable) SequencePointable.FACTORY.createPointable();
        stringKey = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
    }

    @Override
    protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
        TaggedValuePointable arg = args[0];
        if (!(arg.getTag() == ValueTag.SEQUENCE_TAG || arg.getTag() == ValueTag.OBJECT_TAG)) {
            throw new SystemException(ErrorCode.FORG0006);
        }
        TaggedValuePointable tempTvp = ppool.takeOne(TaggedValuePointable.class);
        TaggedValuePointable tempValue = ppool.takeOne(TaggedValuePointable.class);
        try {
            abvs.reset();
            ob.reset(abvs);
            tvps.clear();
            if (arg.getTag() == ValueTag.SEQUENCE_TAG) {
                arg.getValue(sp);
                for (int i = 0; i < sp.getEntryCount(); ++i) {
                    op = (ObjectPointable) ObjectPointable.FACTORY.createPointable();
                    sp.getEntry(i, tempTvp);
                    tempTvp.getValue(op);
                    op.getKeys(tempTvp);
                    addPairs(tempTvp, tempValue);
                }
            } else if (arg.getTag() == ValueTag.OBJECT_TAG) {
                op = (ObjectPointable) ObjectPointable.FACTORY.createPointable();
                arg.getValue(op);
                addPairs(tempTvp, tempValue);
            }
            ob.finish();
            result.set(abvs);
        } catch (IOException e) {
            throw new SystemException(ErrorCode.SYSE0001, e);
        } finally {
            ppool.giveBack(tempTvp);
            for (TaggedValuePointable pointable : tvps) {
                ppool.giveBack(pointable);
            }
        }
    }

    private void addPair(TaggedValuePointable tempTvp, TaggedValuePointable tempValue)
            throws IOException, SystemException {
        if (!isDuplicateKeys(tempTvp, tvps)) {
            key = ppool.takeOne(TaggedValuePointable.class);
            key.set(tempTvp);
            tvps.add(key);
            tempTvp.getValue(stringKey);
            op.getValue(stringKey, tempValue);
            ob.addItem(stringKey, tempValue);
        } else {
            throw new SystemException(ErrorCode.JNDY0003);
        }
    }

    private void addPairs(TaggedValuePointable tempTvp, TaggedValuePointable tempValue)
            throws IOException, SystemException {
        op.getKeys(tempTvp);
        if (tempTvp.getTag() == ValueTag.XS_STRING_TAG) {
            addPair(tempTvp, tempValue);
        } else if (tempTvp.getTag() == ValueTag.SEQUENCE_TAG) {
            tempTvp.getValue(sp1);
            for (int j = 0; j < sp1.getEntryCount(); ++j) {
                key = ppool.takeOne(TaggedValuePointable.class);
                sp1.getEntry(j, tempTvp);
                addPair(tempTvp, tempValue);
            }
        }
    }
}
