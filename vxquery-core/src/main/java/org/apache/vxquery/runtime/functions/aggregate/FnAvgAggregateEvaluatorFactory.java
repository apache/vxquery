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
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.arithmetic.AddOperation;
import org.apache.vxquery.runtime.functions.arithmetic.DivideOperation;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentAggregateEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentAggregateEvaluatorFactory;
import org.apache.vxquery.runtime.functions.util.ArithmeticHelper;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class FnAvgAggregateEvaluatorFactory extends AbstractTaggedValueArgumentAggregateEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnAvgAggregateEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IAggregateEvaluator createEvaluator(IScalarEvaluator[] args) throws AlgebricksException {
        final TaggedValuePointable tvpCount = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final ArrayBackedValueStorage abvsSum = new ArrayBackedValueStorage();
        final DataOutput dOutSum = abvsSum.getDataOutput();
        final ArrayBackedValueStorage abvsCount = new ArrayBackedValueStorage();
        final DataOutput dOutCount = abvsCount.getDataOutput();
        final AddOperation aOp = new AddOperation();
        final ArithmeticHelper add = new ArithmeticHelper(aOp, dCtx);
        final DivideOperation aOpDivide = new DivideOperation();
        final ArithmeticHelper divide = new ArithmeticHelper(aOpDivide, dCtx);

        return new AbstractTaggedValueArgumentAggregateEvaluator(args) {
            long count;
            TaggedValuePointable tvpSum = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();

            @Override
            public void init() throws AlgebricksException {
                count = 0;
            }

            @Override
            public void finishPartial(IPointable result) throws AlgebricksException {
                finish(result);
            }

            @Override
            public void finish(IPointable result) throws AlgebricksException {
                if (count == 0) {
                    XDMConstants.setEmptySequence(result);
                } else {
                    // Set count as a TaggedValuePointable.
                    try {
                        abvsCount.reset();
                        dOutCount.write(ValueTag.XS_INTEGER_TAG);
                        dOutCount.writeLong(count);
                        tvpCount.set(abvsCount);

                        divide.compute(tvpSum, tvpCount, tvpSum);
                        result.set(tvpSum);
                    } catch (Exception e) {
                        throw new AlgebricksException(e);
                    }
                }
            }

            @Override
            protected void step(TaggedValuePointable[] args) throws SystemException {
                TaggedValuePointable tvp = args[0];
                if (count == 0) {
                    // Init.
                    try {
                        abvsSum.reset();
                        dOutSum.write(tvp.getByteArray(), tvp.getStartOffset(), tvp.getLength());
                        tvpSum.set(abvsSum);
                    } catch (IOException e) {
                        throw new SystemException(ErrorCode.SYSE0001, e.toString());
                    }
                } else {
                    add.compute(tvp, tvpSum, tvpSum);
                }
                count++;
            }
        };
    }
}