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
package org.apache.vxquery.runtime.functions.step;

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentUnnestingEvaluator;

public class ChildPathStepUnnestingEvaluator extends AbstractTaggedValueArgumentUnnestingEvaluator {
    final ChildPathStepUnnesting childPathStep;

    public ChildPathStepUnnestingEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args) {
        super(args);
        childPathStep = new ChildPathStepUnnesting(ctx, ppool);
    }

    public boolean step(IPointable result) throws HyracksDataException {
        return childPathStep.step(result);
    }

    @Override
    protected void init(TaggedValuePointable[] args) throws HyracksDataException {
        childPathStep.init(args);
    }
}
