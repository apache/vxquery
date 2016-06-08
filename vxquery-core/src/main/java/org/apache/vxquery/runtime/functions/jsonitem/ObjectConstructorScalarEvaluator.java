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

import java.io.IOException;

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.builders.jsonitem.ObjectBuilder;
import org.apache.vxquery.datamodel.builders.nodes.DictionaryBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.node.AbstractNodeConstructorScalarEvaluator;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;

public class ObjectConstructorScalarEvaluator extends AbstractNodeConstructorScalarEvaluator {
    private ObjectBuilder ob;
    private TaggedValuePointable[] pointables;
    private IPointable vp;
    private UTF8StringPointable sp;
    private SequencePointable seqp;

    public ObjectConstructorScalarEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args) {
        super(ctx, args);
        ob = new ObjectBuilder();
        vp = VoidPointable.FACTORY.createPointable();
        sp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        seqp = (SequencePointable) SequencePointable.FACTORY.createPointable();
    }

    @Override
    protected void constructNode(DictionaryBuilder db, TaggedValuePointable[] args, IMutableValueStorage mvs)
            throws IOException, SystemException {
        ob.reset(mvs);
        TaggedValuePointable tvp;
        TaggedValuePointable tempKey = ppool.takeOne(TaggedValuePointable.class);
        TaggedValuePointable tempValue = ppool.takeOne(TaggedValuePointable.class);

        tvp = args[0];
        if (tvp.getTag() == ValueTag.SEQUENCE_TAG) {
            tvp.getValue(seqp);
            int len = seqp.getEntryCount();
            pointables = new TaggedValuePointable[len / 2];
            for (int i = 0; i < len; i += 2) {
                seqp.getEntry(i, tempKey);
                seqp.getEntry(i + 1, tempValue);
                if (!isDuplicate(tempKey)) {
                    pointables[i / 2] = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
                    tempKey.getValue(pointables[i / 2]);
                    sp.set(vp);
                    ob.addItem(sp, tempValue);
                } else {
                    //Todo: throw the required exception
                }
            }
            ppool.giveBack(tempKey);
            ppool.giveBack(tempValue);
        }
        ob.finish();
    }

    private boolean isDuplicate(TaggedValuePointable tempKey) {
        for (TaggedValuePointable tvp : pointables) {
            tempKey.getValue(vp);
            if (tvp != null && FunctionHelper.arraysEqual(tvp, vp)) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected boolean createsDictionary() {
        return false;
    }
}
