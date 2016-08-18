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
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.jsonitem.ArrayPointable;
import org.apache.vxquery.datamodel.accessors.jsonitem.ObjectPointable;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;

import java.io.IOException;

public class KeysOrMembersScalarEvaluator extends AbstractTaggedValueArgumentScalarEvaluator {
    protected final IHyracksTaskContext ctx;
    private final ObjectPointable op;
    private final ArrayPointable ap;
    private final SequenceBuilder sb;
    private final TaggedValuePointable tempTvp;

    public KeysOrMembersScalarEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args) {
        super(args);
        this.ctx = ctx;
        op = (ObjectPointable) ObjectPointable.FACTORY.createPointable();
        ap = (ArrayPointable) ArrayPointable.FACTORY.createPointable();
        sb = new SequenceBuilder();
        tempTvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    }

    @Override
    protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
        final TaggedValuePointable tvp = args[0];
        ArrayBackedValueStorage abvs = abvsPool.takeOne();
        try {
            switch (tvp.getTag()) {
                case ValueTag.OBJECT_TAG:
                    tvp.getValue(op);
                    op.getKeys(abvs);
                    result.set(abvs);
                    break;
                case ValueTag.ARRAY_TAG:
                    abvs.reset();
                    sb.reset(abvs);
                    tvp.getValue(ap);
                    ap.appendItems(sb);
                    sb.finish();
                    result.set(abvs);
                    break;
                default:
                    throw new SystemException(ErrorCode.FORG0006);
            }
        } catch (IOException e) {
            throw new SystemException(ErrorCode.SYSE0001, e);
        } finally {
            abvsPool.giveBack(abvs);
        }
    }
}
