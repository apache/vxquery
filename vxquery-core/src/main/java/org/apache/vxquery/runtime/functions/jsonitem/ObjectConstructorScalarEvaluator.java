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
import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.builders.jsonitem.ArrayBuilder;
import org.apache.vxquery.datamodel.builders.jsonitem.ObjectBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;

import java.io.IOException;

public class ObjectConstructorScalarEvaluator extends AbstractTaggedValueArgumentScalarEvaluator {
    private ObjectBuilder ob;
    private TaggedValuePointable[] pointables;
    private IPointable vp;
    private UTF8StringPointable sp;
    private SequencePointable seqp;
    protected final IHyracksTaskContext ctx;
    private final ArrayBackedValueStorage abvs;
    private final ArrayBackedValueStorage abvs1;
    private final BooleanPointable bp;
    private final ArrayBuilder ab;

    public ObjectConstructorScalarEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args) {
        super(args);
        this.ctx = ctx;
        abvs = new ArrayBackedValueStorage();
        abvs1 = new ArrayBackedValueStorage();
        ob = new ObjectBuilder();
        vp = VoidPointable.FACTORY.createPointable();
        sp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        seqp = (SequencePointable) SequencePointable.FACTORY.createPointable();
        bp = (BooleanPointable) BooleanPointable.FACTORY.createPointable();
        ab = new ArrayBuilder();
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
    protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
        TaggedValuePointable key, value, qmc;
        try {
            abvs.reset();
            ob.reset(abvs);

            int len = args.length;
            pointables = new TaggedValuePointable[len / 3];
            for (int i = 0; i < len; i += 3) {
                key = args[i];
                value = args[i + 1];
                qmc = args[i + 2];
                if (!isDuplicate(key)) {
                    pointables[i / 3] = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
                    key.getValue(pointables[i / 3]);
                    sp.set(vp);
                    if (value.getTag() == ValueTag.SEQUENCE_TAG) {
                        qmc.getValue(bp);
                        value.getValue(seqp);
                        if (seqp.getEntryCount() == 0) {
                            if (bp.getBoolean()) {
                                continue;
                            }
                            XDMConstants.setJsNull(value);
                            ob.addItem(sp, value);
                        } else {
                            abvs1.reset();
                            ab.reset(abvs1);
                            int l = seqp.getEntryCount();
                            for (int j = 0; j < l; j++) {
                                seqp.getEntry(j, value);
                                ab.addItem(value);
                            }
                            ab.finish();
                            vp.set(abvs1);
                            ob.addItem(sp, vp);
                        }
                    } else {
                        ob.addItem(sp, value);
                    }
                } else {
                    throw new SystemException(ErrorCode.JNDY0003);
                }
            }

            ob.finish();
            result.set(abvs);
        } catch (IOException e) {
            throw new SystemException(ErrorCode.SYSE0001, e);
        }
    }
}
