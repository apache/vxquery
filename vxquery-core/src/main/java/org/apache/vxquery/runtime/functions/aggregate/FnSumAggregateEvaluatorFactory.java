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
import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.arithmetic.AddOperation;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentAggregateEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentAggregateEvaluatorFactory;
import org.apache.vxquery.runtime.functions.util.ArithmeticHelper;

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
        final ArrayBackedValueStorage abvsSum = new ArrayBackedValueStorage();
        final DataOutput dOutSum = abvsSum.getDataOutput();
        final AddOperation aOp = new AddOperation();
        final ArithmeticHelper add = new ArithmeticHelper(aOp, dCtx);

        return new AbstractTaggedValueArgumentAggregateEvaluator(args) {
            TaggedValuePointable tvpSum = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();

            // TODO Check if the second argument is supplied as the zero value.

            @Override
            public void init() throws AlgebricksException {
                try {
                    abvsSum.reset();
                    dOutSum.write(ValueTag.XS_INTEGER_TAG);
                    dOutSum.writeLong(0);
                    tvpSum.set(abvsSum);
                } catch (IOException e) {
                    throw new AlgebricksException(e.toString());
                }
            }

            @Override
            public void finishPartial(IPointable result) throws AlgebricksException {
                finish(result);
            }

            @Override
            public void finish(IPointable result) throws AlgebricksException {
                result.set(tvpSum);
            }

            @Override
            protected void step(TaggedValuePointable[] args) throws SystemException {
                TaggedValuePointable tvp = args[0];
                add.compute(tvp, tvpSum, tvpSum);
            }
        };
    }
}