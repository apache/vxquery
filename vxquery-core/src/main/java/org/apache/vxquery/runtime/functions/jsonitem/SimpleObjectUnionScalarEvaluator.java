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
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.SystemException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SimpleObjectUnionScalarEvaluator extends ObjectConstructorScalarEvaluator {

    private final SequencePointable sp, sp1;

    public SimpleObjectUnionScalarEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args) {
        super(ctx, args);
        sp = (SequencePointable) SequencePointable.FACTORY.createPointable();
        sp1 = (SequencePointable) SequencePointable.FACTORY.createPointable();
    }

    @Override
    protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
        List<TaggedValuePointable> tvps = new ArrayList<>();

        ObjectPointable op;
        TaggedValuePointable key, value;
        TaggedValuePointable arg = args[0];
        if (arg.getTag() == ValueTag.SEQUENCE_TAG) {
            arg.getValue(sp);
            TaggedValuePointable tempTvp = ppool.takeOne(TaggedValuePointable.class);
            TaggedValuePointable boolTvp = ppool.takeOne(TaggedValuePointable.class);
            UTF8StringPointable tempKey = ppool.takeOne(UTF8StringPointable.class);
            XDMConstants.setFalse(boolTvp);
            try {
                for (int i = 0; i < sp.getEntryCount(); ++i) {
                    op = (ObjectPointable) ObjectPointable.FACTORY.createPointable();
                    sp.getEntry(i, tempTvp);
                    tempTvp.getValue(op);
                    op.getKeys(tempTvp);
                    if (tempTvp.getTag() == ValueTag.XS_STRING_TAG) {
                        key = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
                        value = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
                        tempTvp.getValue(tempKey);
                        op.getValue(tempKey, value);
                        key.set(tempTvp);
                        tvps.add(key);
                        tvps.add(value);
                        tvps.add(boolTvp);

                    } else if (tempTvp.getTag() == ValueTag.SEQUENCE_TAG) {
                        tempTvp.getValue(sp1);
                        for (int j = 0; j < sp1.getEntryCount(); ++j) {
                            key = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
                            value = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
                            sp1.getEntry(j, tempTvp);
                            tempTvp.getValue(tempKey);
                            op.getValue(tempKey, value);
                            key.set(tempTvp);
                            tvps.add(key);
                            tvps.add(value);
                            tvps.add(boolTvp);
                        }

                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                ppool.giveBack(tempKey);
                ppool.giveBack(tempTvp);
                ppool.giveBack(boolTvp);
            }
        }
        super.evaluate(tvps.toArray(new TaggedValuePointable[tvps.size()]), result);
    }
}
