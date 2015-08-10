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
package org.apache.vxquery.runtime.functions.conditional;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.BooleanPointable;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class IfThenElseScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;
    private static final int CONDITION = 0;
    private static final int TRUE_CONDITION = 1;
    private static final int FALSE_CONDITION = 2;

    public IfThenElseScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, final IScalarEvaluator[] args)
            throws AlgebricksException {
        final TaggedValuePointable conditionTvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final BooleanPointable bp = (BooleanPointable) BooleanPointable.FACTORY.createPointable();

        return new IScalarEvaluator() {
            @Override
            public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
                args[CONDITION].evaluate(tuple, conditionTvp);

                try {
                    if (conditionTvp.getTag() != ValueTag.XS_BOOLEAN_TAG) {
                        throw new SystemException(ErrorCode.FORG0006);
                    }
                    conditionTvp.getValue(bp);

                    if (bp.getBoolean()) {
                        args[TRUE_CONDITION].evaluate(tuple, result);
                    } else {
                        args[FALSE_CONDITION].evaluate(tuple, result);
                    }
                } catch (SystemException e) {
                    throw new AlgebricksException(e);
                }
            }
        };
    }
}