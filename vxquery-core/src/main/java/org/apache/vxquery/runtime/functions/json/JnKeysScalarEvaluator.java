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
import org.apache.vxquery.datamodel.values.XDMConstants;
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
    private final SequenceBuilder sb;
    private List<TaggedValuePointable> pointables;
    private final ObjectPointable op;

    public JnKeysScalarEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args) {
        super(args);
        this.ctx = ctx;
        sp1 = (SequencePointable) SequencePointable.FACTORY.createPointable();
        sp2 = (SequencePointable) SequencePointable.FACTORY.createPointable();
        sb = new SequenceBuilder();
        pointables = new ArrayList<>();
        op = (ObjectPointable) ObjectPointable.FACTORY.createPointable();
    }

    @Override
    protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
        TaggedValuePointable tvp1 = args[0];
        pointables.clear();
        if (tvp1.getTag() == ValueTag.SEQUENCE_TAG) {
            List<ArrayBackedValueStorage> abvsList = new ArrayList<>();
            TaggedValuePointable temptvp = ppool.takeOne(TaggedValuePointable.class);
            ArrayBackedValueStorage abvsResult = abvsPool.takeOne();
            abvsList.add(abvsResult);
            try {
                tvp1.getValue(sp1);
                int size1 = sp1.getEntryCount();
                int size2;
                for (int i = 0; i < size1; i++) {
                    sp1.getEntry(i, temptvp);
                    if (temptvp.getTag() == ValueTag.OBJECT_TAG) {
                        temptvp.getValue(op);
                        ArrayBackedValueStorage abvsKeys = new ArrayBackedValueStorage();
                        abvsList.add(abvsKeys);
                        op.getKeys(abvsKeys);
                        temptvp.set(abvsKeys);
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
                FunctionHelper.removeDuplicates(pointables);
                abvsResult.reset();
                sb.reset(abvsResult);
                for (TaggedValuePointable tvp : pointables) {
                    sb.addItem(tvp);
                }
                sb.finish();
                result.set(abvsResult);
            } catch (IOException e) {
                throw new SystemException(ErrorCode.SYSE0001, e);
            } finally {
                for (ArrayBackedValueStorage arrayBackedValueStorage : abvsList) {
                    abvsPool.giveBack(arrayBackedValueStorage);
                }
                ppool.giveBack(temptvp);
            }
        } else if (tvp1.getTag() == ValueTag.OBJECT_TAG) {
            ArrayBackedValueStorage abvsResult = abvsPool.takeOne();
            try {
                tvp1.getValue(op);
                op.getKeys(abvsResult);
                result.set(abvsResult);
            } catch (IOException e) {
                throw new SystemException(ErrorCode.SYSE0001, e);
            } finally {
                abvsPool.giveBack(abvsResult);
            }
        } else {
            XDMConstants.setEmptySequence(result);
        }
    }
}
