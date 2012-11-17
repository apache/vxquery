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
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentAggregateEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentAggregateEvaluatorFactory;
import org.apache.vxquery.runtime.functions.comparison.AbstractValueComparisonOperation;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public abstract class AbstractMaxMinAggregateEvaluatorFactory extends
        AbstractTaggedValueArgumentAggregateEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public AbstractMaxMinAggregateEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    protected IAggregateEvaluator createEvaluator(IScalarEvaluator[] args) throws AlgebricksException {
        final AbstractValueComparisonOperation aOp = createValueComparisonOperation();
        final TaggedValuePointable tvp2 = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();

        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final DataOutput dOut = abvs.getDataOutput();

        return new AbstractTaggedValueArgumentAggregateEvaluator(args) {
            long count;

            @Override
            public void init() throws AlgebricksException {
                count = 0;
            }

            @Override
            public void finish(IPointable result) throws AlgebricksException {
                result.set(abvs);
            }

            @Override
            protected void step(TaggedValuePointable[] args) throws SystemException {
                TaggedValuePointable tvp1 = args[0];
                if (count != 0) {
                    tvp2.set(abvs.getByteArray(), abvs.getStartOffset(), abvs.getLength());
                }
                if (count == 0 || FunctionHelper.transformThenCompareMinMaxTaggedValues(aOp, tvp1, tvp2, dCtx)) {
                    try {
                        abvs.reset();
                        dOut.write(tvp1.getByteArray(), tvp1.getStartOffset(), tvp1.getLength());
                    } catch (IOException e) {
                        throw new SystemException(ErrorCode.SYSE0001, e);
                    }
                }
                count++;
            }

        };
    }

    protected abstract AbstractValueComparisonOperation createValueComparisonOperation();
}