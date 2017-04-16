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
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.vxquery.datamodel.accessors.PointablePool;
import org.apache.vxquery.datamodel.accessors.PointablePoolFactory;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;

public abstract class AbstractTaggedValueArgumentAggregateEvaluator implements IAggregateEvaluator {
    private final IScalarEvaluator[] args;

    private final TaggedValuePointable[] tvps;

    protected final PointablePool ppool = PointablePoolFactory.INSTANCE.createPointablePool();

    public AbstractTaggedValueArgumentAggregateEvaluator(IScalarEvaluator[] args) {
        this.args = args;
        tvps = new TaggedValuePointable[args.length];
        for (int i = 0; i < tvps.length; ++i) {
            tvps[i] = new TaggedValuePointable();
        }
    }

    @Override
    public void step(IFrameTupleReference tuple) throws HyracksDataException {
        for (int i = 0; i < args.length; ++i) {
            args[i].evaluate(tuple, tvps[i]);
        }
        step(tvps);
    }

    protected abstract void step(TaggedValuePointable[] args) throws HyracksDataException;
}
