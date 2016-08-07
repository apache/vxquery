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
import org.apache.vxquery.datamodel.builders.jsonitem.ObjectBuilder;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;

import java.io.IOException;

public class LibjnRemoveKeysScalarEvaluator extends AbstractTaggedValueArgumentScalarEvaluator {
    protected final IHyracksTaskContext ctx;
    private final ObjectPointable op;
    private TaggedValuePointable key;
    private final UTF8StringPointable stringKey;
    private final ObjectBuilder ob;
    private final ArrayBackedValueStorage abvs, abvs1;
    private final SequenceBuilder sb;

    public LibjnRemoveKeysScalarEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args) {
        super(args);
        this.ctx = ctx;
        stringKey = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        ob = new ObjectBuilder();
        abvs = new ArrayBackedValueStorage();
        abvs1 = new ArrayBackedValueStorage();
        sb = new SequenceBuilder();
        op = (ObjectPointable) ObjectPointable.FACTORY.createPointable();
    }

    @Override
    protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
        TaggedValuePointable sequence = args[0];
        TaggedValuePointable keys = args[1];

        TaggedValuePointable tempTvp = ppool.takeOne(TaggedValuePointable.class);
        SequencePointable sp = ppool.takeOne(SequencePointable.class);
        try {
            abvs1.reset();
            sb.reset(abvs1);
            if (sequence.getTag() == ValueTag.SEQUENCE_TAG) {
                sequence.getValue(sp);
                for (int i = 0; i < sp.getEntryCount(); ++i) {
                    sp.getEntry(i, tempTvp);
                    if (tempTvp.getTag() == ValueTag.OBJECT_TAG) {
                        tempTvp.getValue(op);
                        op.getKeys(tempTvp);
                        addPairs(tempTvp, keys);
                    } else {
                        sb.addItem(tempTvp);
                    }
                }
            } else if (sequence.getTag() == ValueTag.OBJECT_TAG) {
                sequence.getValue(op);
                addPairs(tempTvp, keys);
            } else {
                sb.addItem(sequence);
            }
            sb.finish();
            result.set(abvs1);
        } catch (IOException e) {
            throw new SystemException(ErrorCode.SYSE0001, e);
        } finally {
            ppool.giveBack(tempTvp);
            ppool.giveBack(sp);
        }
    }

    private void addPair(TaggedValuePointable tempTvp, TaggedValuePointable tempValue)
            throws IOException, SystemException {
        tempTvp.getValue(stringKey);
        op.getValue(stringKey, tempValue);
        ob.addItem(stringKey, tempValue);
    }

    private void addPairs(TaggedValuePointable objTvp, TaggedValuePointable keys) throws IOException, SystemException {
        SequencePointable sp1 = ppool.takeOne(SequencePointable.class);
        TaggedValuePointable tempValue = ppool.takeOne(TaggedValuePointable.class);
        try {
            op.getKeys(objTvp);
            if (objTvp.getTag() == ValueTag.XS_STRING_TAG) {
                if (!isKeyFound(objTvp, keys)) {
                    abvs.reset();
                    ob.reset(abvs);
                    addPair(objTvp, tempValue);
                    ob.finish();
                    sb.addItem(abvs);
                }
            } else if (objTvp.getTag() == ValueTag.SEQUENCE_TAG) {
                objTvp.getValue(sp1);
                boolean found = false;
                for (int j = 0; j < sp1.getEntryCount(); ++j) {
                    sp1.getEntry(j, objTvp);
                    if (!isKeyFound(objTvp, keys)) {
                        if (!found) {
                            abvs.reset();
                            ob.reset(abvs);
                            found = true;
                        }
                        addPair(objTvp, tempValue);
                    }
                }
                if (found) {
                    ob.finish();
                    sb.addItem(abvs);
                }
            }
        } finally {
            ppool.giveBack(sp1);
            ppool.giveBack(tempValue);
        }
    }

    private boolean isKeyFound(TaggedValuePointable tvp, TaggedValuePointable keys) throws SystemException {
        TaggedValuePointable tempTvp = ppool.takeOne(TaggedValuePointable.class);
        SequencePointable sp2 = ppool.takeOne(SequencePointable.class);
        try {
            if (keys.getTag() != ValueTag.SEQUENCE_TAG && keys.getTag() != ValueTag.XS_STRING_TAG) {
                throw new SystemException(ErrorCode.FORG0006);
            }
            if (keys.getTag() == ValueTag.SEQUENCE_TAG) {
                keys.getValue(sp2);
                for (int i = 0; i < sp2.getEntryCount(); i++) {
                    sp2.getEntry(i, tempTvp);
                    if (tempTvp.getTag() != ValueTag.XS_STRING_TAG) {
                        throw new SystemException(ErrorCode.FORG0006);
                    }
                    if (FunctionHelper.arraysEqual(tempTvp, tvp)) {
                        return true;
                    }
                }
            } else if (keys.getTag() == ValueTag.XS_STRING_TAG) {
                if (FunctionHelper.arraysEqual(tvp, keys)) {
                    return true;
                }
            }
        } finally {
            ppool.giveBack(tempTvp);
            ppool.giveBack(sp2);
        }
        return false;
    }
}
