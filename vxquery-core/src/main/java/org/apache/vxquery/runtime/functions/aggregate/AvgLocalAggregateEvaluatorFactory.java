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

import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.runtime.functions.arithmetic.AddOperation;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentAggregateEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentAggregateEvaluatorFactory;
import org.apache.vxquery.runtime.functions.util.ArithmeticHelper;

public class AvgLocalAggregateEvaluatorFactory extends AbstractTaggedValueArgumentAggregateEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public AvgLocalAggregateEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IAggregateEvaluator createEvaluator(IScalarEvaluator[] args) throws HyracksDataException {
        final TaggedValuePointable tvpCount = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final ArrayBackedValueStorage abvsCount = new ArrayBackedValueStorage();
        final DataOutput dOutCount = abvsCount.getDataOutput();
        final ArrayBackedValueStorage abvsSum = new ArrayBackedValueStorage();
        final DataOutput dOutSum = abvsSum.getDataOutput();
        final ArrayBackedValueStorage abvsSeq = new ArrayBackedValueStorage();
        final SequenceBuilder sb = new SequenceBuilder();
        final AddOperation aOpAdd = new AddOperation();
        final ArithmeticHelper add = new ArithmeticHelper(aOpAdd, dCtx);

        return new AbstractTaggedValueArgumentAggregateEvaluator(args) {
            long count;
            TaggedValuePointable tvpSum = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();

            @Override
            public void init() throws HyracksDataException {
                count = 0;
                try {
                    abvsSum.reset();
                    dOutSum.write(ValueTag.XS_INTEGER_TAG);
                    dOutSum.writeLong(0);
                    tvpSum.set(abvsSum);
                } catch (Exception e) {
                    throw new HyracksDataException(e);
                }
            }

            @Override
            public void finishPartial(IPointable result) throws HyracksDataException {
                finish(result);
            }

            @Override
            public void finish(IPointable result) throws HyracksDataException {
                if (count == 0) {
                    XDMConstants.setEmptySequence(result);
                } else {
                    // Set count as a TaggedValuePointable.
                    try {
                        abvsCount.reset();
                        dOutCount.write(ValueTag.XS_INTEGER_TAG);
                        dOutCount.writeLong(count);
                        tvpCount.set(abvsCount);

                        // Save intermediate result.
                        abvsSeq.reset();
                        sb.reset(abvsSeq);
                        sb.addItem(tvpCount);
                        sb.addItem(tvpSum);
                        sb.finish();
                        result.set(abvsSeq);
                    } catch (Exception e) {
                        throw new HyracksDataException(e);
                    }
                }
            }

            @Override
            protected void step(TaggedValuePointable[] args) throws HyracksDataException {
                TaggedValuePointable tvp = args[0];
                add.compute(tvp, tvpSum, tvpSum);
                count++;
            }
        };
    }
}
