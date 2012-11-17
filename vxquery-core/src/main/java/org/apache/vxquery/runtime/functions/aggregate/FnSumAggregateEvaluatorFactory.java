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
package org.apache.vxquery.runtime.functions.aggregate;

import java.io.DataOutput;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.arithmetic.AddOperation;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentAggregateEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentAggregateEvaluatorFactory;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class FnSumAggregateEvaluatorFactory extends AbstractTaggedValueArgumentAggregateEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnSumAggregateEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IAggregateEvaluator createEvaluator(IScalarEvaluator[] args) throws AlgebricksException {
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final DataOutput dOut = abvs.getDataOutput();
        final AddOperation aOp = new AddOperation();

        return new AbstractTaggedValueArgumentAggregateEvaluator(args) {
            long count;
            TaggedValuePointable tvpSum = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();

            @Override
            public void init() throws AlgebricksException {
                count = 0;
            }

            @Override
            public void finish(IPointable result) throws AlgebricksException {
                // TODO What is returned when step is never called. Since the second argument is the zero value.
                if (count == 0) {
                    // No argument return an integer.
                    try {
                        abvs.reset();
                        dOut.write(ValueTag.XS_INTEGER_TAG);
                        dOut.writeLong(0);
                        result.set(abvs);
                    } catch (Exception e) {
                        throw new AlgebricksException(e);
                    }
                } else {
                    result.set(tvpSum);
                }
            }

            @Override
            protected void step(TaggedValuePointable[] args) throws SystemException {
                TaggedValuePointable tvp = args[0];
                if (count == 0) {
                    // Init.
                    tvpSum.set(tvp);
                } else {
                    FunctionHelper.arithmeticOperation(aOp, dCtx, tvp, tvpSum, tvpSum);
                }
                count++;
            }
        };
    }
}