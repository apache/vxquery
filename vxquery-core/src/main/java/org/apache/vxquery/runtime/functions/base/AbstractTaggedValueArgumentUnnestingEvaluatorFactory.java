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

import org.apache.vxquery.context.DynamicContext;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IUnnestingEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IUnnestingEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public abstract class AbstractTaggedValueArgumentUnnestingEvaluatorFactory implements IUnnestingEvaluatorFactory {
    private static final long serialVersionUID = 1L;
    
    protected DynamicContext dCtx;

    private final IScalarEvaluatorFactory[] args;

    public AbstractTaggedValueArgumentUnnestingEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        this.args = args;
        this.dCtx = null;
    }

    @Override
    public final IUnnestingEvaluator createUnnestingEvaluator(IHyracksTaskContext ctx) throws AlgebricksException {
        dCtx = (DynamicContext) ctx.getJobletContext().getGlobalJobData();

        IScalarEvaluator[] es = new IScalarEvaluator[args.length];
        for (int i = 0; i < es.length; ++i) {
            es[i] = args[i].createScalarEvaluator(ctx);
        }
        return createEvaluator(es);
    }

    protected abstract IUnnestingEvaluator createEvaluator(IScalarEvaluator[] args) throws AlgebricksException;
}