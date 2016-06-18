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
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.jsonitem.ArrayPointable;
import org.apache.vxquery.datamodel.builders.jsonitem.ArrayBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;

public class ValueScalarEvaluator extends AbstractTaggedValueArgumentScalarEvaluator {
    protected final IHyracksTaskContext ctx;
    private final ArrayBackedValueStorage mvs;
    private final ArrayPointable ap;
    private final LongPointable lp;
    private final ArrayBuilder ab;
    protected DataOutput out;

    public ValueScalarEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args) {
        super(args);
        this.ctx = ctx;
        ap = (ArrayPointable) ArrayPointable.FACTORY.createPointable();
        lp = (LongPointable) LongPointable.FACTORY.createPointable();
        mvs = new ArrayBackedValueStorage();
        ab = new ArrayBuilder();
    }

    @Override
    protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
        mvs.reset();
        try {
            ab.reset(mvs);
            TaggedValuePointable tvp1 = args[0];
            if (tvp1.getTag() == ValueTag.ARRAY_TAG) {
                tvp1.getValue(ap);
                TaggedValuePointable tvp2 = args[1];
                TaggedValuePointable tempTvp = ppool.takeOne(TaggedValuePointable.class);
                tvp2.getValue(lp);
                ap.getEntry((int) lp.getLong() - 1, tempTvp);
                try {
                    ab.addItem(tempTvp);
                } finally {
                    ppool.giveBack(tempTvp);
                }
                ab.finish();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        result.set(mvs);
    }

}
