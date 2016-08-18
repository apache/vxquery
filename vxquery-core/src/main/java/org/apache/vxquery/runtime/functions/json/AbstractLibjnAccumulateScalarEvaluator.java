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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class AbstractLibjnAccumulateScalarEvaluator extends AbstractTaggedValueArgumentScalarEvaluator {
    protected final IHyracksTaskContext ctx;
    private final SequencePointable sp, sp1;
    protected final ObjectBuilder ob;
    private final ArrayBuilder ab;
    protected TaggedValuePointable key, value;
    private final UTF8StringPointable stringKey;
    private final List<ArrayBackedValueStorage> abvsList;
    private final Map<TaggedValuePointable, List<TaggedValuePointable>> tvps;
    protected List<TaggedValuePointable> values;
    protected final ObjectPointable op;

    public AbstractLibjnAccumulateScalarEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args) {
        super(args);
        this.ctx = ctx;
        sp = (SequencePointable) SequencePointable.FACTORY.createPointable();
        sp1 = (SequencePointable) SequencePointable.FACTORY.createPointable();
        stringKey = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        ob = new ObjectBuilder();
        ab = new ArrayBuilder();
        tvps = new LinkedHashMap<>();
        op = (ObjectPointable) ObjectPointable.FACTORY.createPointable();
        abvsList = new ArrayList<>();
    }

    @Override
    protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
        TaggedValuePointable arg = args[0];
        if (!(arg.getTag() == ValueTag.SEQUENCE_TAG || arg.getTag() == ValueTag.OBJECT_TAG)) {
            throw new SystemException(ErrorCode.FORG0006);
        }
        TaggedValuePointable tempTvp = ppool.takeOne(TaggedValuePointable.class);
        ArrayBackedValueStorage abvsResult = abvsPool.takeOne();
        abvsList.add(abvsResult);
        ArrayBackedValueStorage abvsItem = abvsPool.takeOne();
        abvsList.add(abvsItem);
        try {
            abvsResult.reset();
            ob.reset(abvsResult);
            tvps.clear();
            if (arg.getTag() == ValueTag.SEQUENCE_TAG) {
                arg.getValue(sp);
                for (int i = 0; i < sp.getEntryCount(); ++i) {
                    sp.getEntry(i, tempTvp);
                    if (tempTvp.getTag() == ValueTag.OBJECT_TAG) {
                        addPairs(tempTvp);
                    }
                }
            } else if (arg.getTag() == ValueTag.OBJECT_TAG) {
                addPairs(arg);
            }
            for (TaggedValuePointable key1 : tvps.keySet()) {
                key1.getValue(stringKey);
                values = tvps.get(key1);
                if (values.size() > 1) {
                    FunctionHelper.removeDuplicates(values);
                    if (values.size() > 1) {
                        abvsItem.reset();
                        ab.reset(abvsItem);
                        for (TaggedValuePointable pointable : values) {
                            ab.addItem(pointable);
                        }
                        ab.finish();
                        ob.addItem(stringKey, abvsItem);
                    } else {
                        ob.addItem(stringKey, values.get(0));
                    }
                } else {
                    addKeyValue(stringKey, values.get(0));
                }
            }
            ob.finish();
            result.set(abvsResult);
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
            for (ArrayBackedValueStorage arrayBackedValueStorage : abvsList) {
                abvsPool.giveBack(arrayBackedValueStorage);
            }
        }
    }

    protected abstract void addKeyValue(UTF8StringPointable stringKey, TaggedValuePointable taggedValuePointable)
            throws IOException;

    private void addPair(TaggedValuePointable tvp1, ObjectPointable op) throws IOException, SystemException {
        TaggedValuePointable tvp = isDuplicateKeys(tvp1, tvps.keySet());
        value = ppool.takeOne(TaggedValuePointable.class);
        tvp1.getValue(stringKey);
        op.getValue(stringKey, value);
        if (tvp == null) {
            key = ppool.takeOne(TaggedValuePointable.class);
            key.set(tvp1);
            values = new ArrayList<>();
            values.add(value);
            tvps.put(key, values);
        } else {
            tvps.get(tvp).add(value);
        }
    }

    private void addPairs(TaggedValuePointable tvp1) throws IOException, SystemException {
        ArrayBackedValueStorage mvs = abvsPool.takeOne();
        abvsList.add(mvs);
        tvp1.getValue(op);
        op.getKeys(mvs);
        tvp1.set(mvs);
        if (tvp1.getTag() == ValueTag.XS_STRING_TAG) {
            addPair(tvp1, op);
        } else if (tvp1.getTag() == ValueTag.SEQUENCE_TAG) {
            tvp1.getValue(sp1);
            for (int j = 0; j < sp1.getEntryCount(); ++j) {
                sp1.getEntry(j, tvp1);
                addPair(tvp1, op);
            }
        }
    }

    private TaggedValuePointable isDuplicateKeys(TaggedValuePointable key, Set<TaggedValuePointable> pointables) {
        for (TaggedValuePointable tvp : pointables) {
            if (tvp != null && FunctionHelper.arraysEqual(tvp, key)) {
                return tvp;
            }
        }
        return null;
    }
}
