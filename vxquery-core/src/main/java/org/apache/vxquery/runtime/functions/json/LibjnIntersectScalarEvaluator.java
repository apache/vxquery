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
import org.apache.vxquery.datamodel.accessors.jsonitem.ObjectPointable;
import org.apache.vxquery.datamodel.builders.jsonitem.ArrayBuilder;
import org.apache.vxquery.datamodel.builders.jsonitem.ObjectBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class LibjnIntersectScalarEvaluator extends AbstractTaggedValueArgumentScalarEvaluator {
    protected final IHyracksTaskContext ctx;
    private final SequencePointable sp, sp1;
    protected final ObjectBuilder ob;
    private ObjectPointable op;
    private final ArrayBuilder ab;
    private TaggedValuePointable key, value;
    private final UTF8StringPointable stringKey;
    protected final ArrayBackedValueStorage abvs, abvs1;
    protected final Map<TaggedValuePointable, TaggedValuePointable[]> tvps;

    public LibjnIntersectScalarEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args) {
        super(args);
        this.ctx = ctx;
        sp = (SequencePointable) SequencePointable.FACTORY.createPointable();
        sp1 = (SequencePointable) SequencePointable.FACTORY.createPointable();
        stringKey = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        abvs = new ArrayBackedValueStorage();
        abvs1 = new ArrayBackedValueStorage();
        ob = new ObjectBuilder();
        ab = new ArrayBuilder();
        tvps = new LinkedHashMap<>();
    }

    @Override
    protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
        TaggedValuePointable arg = args[0];
        if (!(arg.getTag() == ValueTag.SEQUENCE_TAG || arg.getTag() == ValueTag.OBJECT_TAG)) {
            throw new SystemException(ErrorCode.FORG0006);
        }
        TaggedValuePointable tempTvp = ppool.takeOne(TaggedValuePointable.class);
        try {
            abvs.reset();
            ob.reset(abvs);
            tvps.clear();
            if (arg.getTag() == ValueTag.SEQUENCE_TAG) {
                arg.getValue(sp);
                for (int i = 0; i < sp.getEntryCount(); ++i) {
                    sp.getEntry(i, tempTvp);
                    if (tempTvp.getTag() == ValueTag.OBJECT_TAG) {
                        op = (ObjectPointable) ObjectPointable.FACTORY.createPointable();
                        tempTvp.getValue(op);
                        addPairs(tempTvp);
                    }
                }
            } else if (arg.getTag() == ValueTag.OBJECT_TAG) {
                op = (ObjectPointable) ObjectPointable.FACTORY.createPointable();
                arg.getValue(op);
                addPairs(tempTvp);
            }
            for (TaggedValuePointable key1 : tvps.keySet()) {
                key1.getValue(stringKey);
                TaggedValuePointable[] values = tvps.get(key1);
                if (values.length > 1) {
                    abvs1.reset();
                    ab.reset(abvs1);
                    for (TaggedValuePointable value1 : values) {
                        ab.addItem(value1);
                    }
                    ab.finish();
                    ob.addItem(stringKey, abvs1);
                }
            }
            ob.finish();
            result.set(abvs);
        } catch (IOException e) {
            throw new SystemException(ErrorCode.SYSE0001, e);
        } finally {
            ppool.giveBack(tempTvp);
            for (TaggedValuePointable key1 : tvps.keySet()) {
                for (TaggedValuePointable value1 : tvps.get(key1)) {
                    ppool.giveBack(value1);
                }
                ppool.giveBack(key1);
            }
        }
    }

    private void addPair(TaggedValuePointable tempTvp) throws IOException, SystemException {
        TaggedValuePointable tvp = isDuplicateKeys(tempTvp, tvps.keySet());
        value = ppool.takeOne(TaggedValuePointable.class);
        tempTvp.getValue(stringKey);
        op.getValue(stringKey, value);
        if (tvp == null) {
            key = ppool.takeOne(TaggedValuePointable.class);
            key.set(tempTvp);
            TaggedValuePointable[] values = { value };
            tvps.put(key, values);
        } else {
            TaggedValuePointable[] values = tvps.get(tvp);
            TaggedValuePointable[] values1 = new TaggedValuePointable[values.length + 1];
            for (int i = 0; i < values.length; i++) {
                values1[i] = values[i];
            }
            values1[values.length] = value;
            tvps.replace(tvp, values1);
        }
    }

    private void addPairs(TaggedValuePointable tempTvp) throws IOException, SystemException {
        op.getKeys(tempTvp);
        if (tempTvp.getTag() == ValueTag.XS_STRING_TAG) {
            addPair(tempTvp);
        } else if (tempTvp.getTag() == ValueTag.SEQUENCE_TAG) {
            tempTvp.getValue(sp1);
            for (int j = 0; j < sp1.getEntryCount(); ++j) {
                key = ppool.takeOne(TaggedValuePointable.class);
                sp1.getEntry(j, tempTvp);
                addPair(tempTvp);
            }
        }
    }

    protected TaggedValuePointable isDuplicateKeys(TaggedValuePointable key, Set<TaggedValuePointable> pointables) {
        for (TaggedValuePointable tvp : pointables) {
            if (tvp != null && FunctionHelper.arraysEqual(tvp, key)) {
                return tvp;
            }
        }
        return null;
    }
}