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
package org.apache.vxquery.runtime.functions.type;

import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.types.SequenceType;

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;

public abstract class AbstractTypeScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public AbstractTypeScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    protected static abstract class AbstractTypeScalarEvaluator extends AbstractTaggedValueArgumentScalarEvaluator {
        protected final DynamicContext dCtx;

        private final IntegerPointable ip;

        private boolean first;

        protected AbstractTypeScalarEvaluator(IScalarEvaluator[] args, IHyracksTaskContext ctx) {
            super(args);
            dCtx = (DynamicContext) ctx.getJobletContext().getGlobalJobData();
            ip = (IntegerPointable) IntegerPointable.FACTORY.createPointable();
            first = true;
        }

        protected abstract void setSequenceType(SequenceType sType);

        protected abstract void evaluate(TaggedValuePointable tvp, IPointable result) throws SystemException;

        @Override
        protected final void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
            if (first) {
                if (args[1].getTag() != ValueTag.XS_INT_TAG) {
                    throw new IllegalArgumentException("Expected int value tag, got: " + args[1].getTag());
                }
                args[1].getValue(ip);
                int typeCode = ip.getInteger();
                SequenceType sType = dCtx.getStaticContext().lookupSequenceType(typeCode);
                setSequenceType(sType);
                first = false;
            }
            evaluate(args[0], result);
        }
    }
}
