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

import java.io.IOException;

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.jsonitem.ArrayPointable;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;

public class JnMembersScalarEvaluator extends AbstractTaggedValueArgumentScalarEvaluator {
    protected final IHyracksTaskContext ctx;
    private final SequencePointable sp;
    private final ArrayBackedValueStorage abvs;
    private final SequenceBuilder sb;
    private ArrayPointable ap;
    private TaggedValuePointable tempTvp;

    public JnMembersScalarEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args) {
        super(args);
        this.ctx = ctx;
        sp = (SequencePointable) SequencePointable.FACTORY.createPointable();
        abvs = new ArrayBackedValueStorage();
        sb = new SequenceBuilder();
        ap = (ArrayPointable) ArrayPointable.FACTORY.createPointable();
        tempTvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    }

    @Override
    protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
        TaggedValuePointable tvp = args[0];
        abvs.reset();
        sb.reset(abvs);
        try {
            if (tvp.getTag() == ValueTag.SEQUENCE_TAG) {
                tvp.getValue(sp);
                int size1 = sp.getEntryCount();
                for (int i = 0; i < size1; i++) {
                    sp.getEntry(i, tempTvp);
                    if (tempTvp.getTag() == ValueTag.ARRAY_TAG) {
                        tempTvp.getValue(ap);
                        ap.appendSequence(sb);
                    } else {
                        XDMConstants.setEmptySequence(result);
                    }
                }
            } else if (tvp.getTag() == ValueTag.ARRAY_TAG) {
                tvp.getValue(ap);
                ap.appendSequence(sb);
            } else {
                XDMConstants.setEmptySequence(result);
            }
            sb.finish();
            result.set(abvs);
        } catch (IOException e) {
            throw new SystemException(ErrorCode.SYSE0001, e);
        }
    }
}
