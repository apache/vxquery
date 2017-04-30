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
package org.apache.vxquery.runtime.functions.base;

import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.vxquery.context.DynamicContext;

public abstract class AbstractTaggedValueArgumentAggregateEvaluatorFactory implements IAggregateEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    private final IScalarEvaluatorFactory[] args;

    public static DynamicContext dCtx;

    public AbstractTaggedValueArgumentAggregateEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        this.args = args;
    }

    @Override
    public final IAggregateEvaluator createAggregateEvaluator(IHyracksTaskContext ctx) throws HyracksDataException {
        dCtx = (DynamicContext) ctx.getJobletContext().getGlobalJobData();
        IScalarEvaluator[] es = new IScalarEvaluator[args.length];
        for (int i = 0; i < es.length; ++i) {
            es[i] = args[i].createScalarEvaluator(ctx);
        }
        return createEvaluator(es);
    }

    protected abstract IAggregateEvaluator createEvaluator(IScalarEvaluator[] args) throws HyracksDataException;
}
