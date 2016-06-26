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
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.jsonitem.ObjectPointable;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JnKeysScalarEvaluator extends AbstractTaggedValueArgumentScalarEvaluator {
    protected final IHyracksTaskContext ctx;
    private final SequencePointable sp1, sp2;
    private final List<TaggedValuePointable> pointables;
    private final SequenceBuilder sb;
    private final ArrayBackedValueStorage abvs;

    public JnKeysScalarEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args) {
        super(args);
        this.ctx = ctx;
        sp1 = (SequencePointable) SequencePointable.FACTORY.createPointable();
        sp2 = (SequencePointable) SequencePointable.FACTORY.createPointable();
        pointables = new ArrayList<>();
        sb = new SequenceBuilder();
        abvs = new ArrayBackedValueStorage();
    }

    @Override
    protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
        TaggedValuePointable tvp1 = args[0];
        ObjectPointable op;
        if (tvp1.getTag() == ValueTag.SEQUENCE_TAG) {
            TaggedValuePointable temptvp = ppool.takeOne(TaggedValuePointable.class);
            try {
                tvp1.getValue(sp1);
                int size1 = sp1.getEntryCount();
                int size2;
                for (int i = 0; i < size1; i++) {
                    sp1.getEntry(i, temptvp);
                    if (temptvp.getTag() == ValueTag.OBJECT_TAG) {
                        op = (ObjectPointable) ObjectPointable.FACTORY.createPointable();
                        temptvp.getValue(op);
                        op.getKeys(temptvp);
                        temptvp.getValue(sp2);
                        size2 = sp2.getEntryCount();
                        for (int j = 0; j < size2; j++) {
                            TaggedValuePointable tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY
                                    .createPointable();
                            sp2.getEntry(j, tvp);
                            pointables.add(tvp);
                        }
                    }
                }
                removeDuplicates(pointables);
                abvs.reset();
                sb.reset(abvs);
                for (TaggedValuePointable tvp : pointables) {
                    sb.addItem(tvp);
                }
                sb.finish();
                result.set(abvs);
            } catch (IOException e) {
                throw new SystemException(ErrorCode.SYSE0001, e);
            } finally {
                ppool.giveBack(temptvp);
            }
        } else if (tvp1.getTag() == ValueTag.OBJECT_TAG) {
            try {
                op = (ObjectPointable) ObjectPointable.FACTORY.createPointable();
                tvp1.getValue(op);
                op.getKeys(result);
            } catch (IOException e) {
                throw new SystemException(ErrorCode.SYSE0001, e);
            }
        } else {
            try {
                abvs.reset();
                sb.reset(abvs);
                sb.finish();
                result.set(abvs);
            } catch (IOException e) {
                throw new SystemException(ErrorCode.SYSE0001, e);

            }
        }
    }

    private void removeDuplicates(List<TaggedValuePointable> pointables) {
        int size = pointables.size();
        for (int i = 0; i < size - 1; i++) {
            for (int j = i + 1; j < size; j++) {
                if (!FunctionHelper.arraysEqual(pointables.get(j), pointables.get(i)))
                    continue;
                pointables.remove(j);
                j--;
                size--;
            }
        }
    }
}
