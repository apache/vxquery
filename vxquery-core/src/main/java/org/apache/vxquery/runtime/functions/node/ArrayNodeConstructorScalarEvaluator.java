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
package org.apache.vxquery.runtime.functions.node;

import java.io.IOException;

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.builders.jsonitem.ArrayBuilder;
import org.apache.vxquery.datamodel.builders.nodes.DictionaryBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.SystemException;

public class ArrayNodeConstructorScalarEvaluator extends AbstractNodeConstructorScalarEvaluator {
    private final ArrayBuilder ab;

    private final SequencePointable sp;

    public ArrayNodeConstructorScalarEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args) {
        super(ctx, args);
        ab = new ArrayBuilder();
        sp = (SequencePointable) SequencePointable.FACTORY.createPointable();
    }

    @Override
    protected void constructNode(DictionaryBuilder db, TaggedValuePointable[] args, IMutableValueStorage mvs)
            throws IOException, SystemException {
        ab.reset(mvs);
        TaggedValuePointable arg = args[0];
        if (arg.getTag() == ValueTag.SEQUENCE_TAG) {
            TaggedValuePointable tempTvp = ppool.takeOne(TaggedValuePointable.class);
            try {
                arg.getValue(sp);
                for (int i = 0; i < sp.getEntryCount(); ++i) {
                    sp.getEntry(i, tempTvp);
                    ab.addItem(tempTvp);
                }
            } finally {
                ppool.giveBack(tempTvp);
            }
        } else {
            ab.addItem(arg);
        }
        ab.finish();
    }

    @Override
    protected boolean createsDictionary() {
        return false;
    }

}
