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
package org.apache.vxquery.runtime.functions.comparison;

import java.io.DataOutput;

import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public abstract class AbstractValueComparisonScalarEvaluatorFactory extends
        AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public AbstractValueComparisonScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final DataOutput dOut = abvs.getDataOutput();
        final AbstractValueComparisonOperation aOp = createValueComparisonOperation();
        final DynamicContext dCtx = (DynamicContext) ctx.getJobletContext().getGlobalJobData();

        final ArrayBackedValueStorage abvsInteger1 = new ArrayBackedValueStorage();
        final DataOutput dOutInteger1 = abvsInteger1.getDataOutput();
        final ArrayBackedValueStorage abvsInteger2 = new ArrayBackedValueStorage();
        final DataOutput dOutInteger2 = abvsInteger2.getDataOutput();

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                TaggedValuePointable tvp1 = args[0];
                TaggedValuePointable tvp2 = args[1];
                boolean booleanResult = transformThenCompareTaggedValues(aOp, tvp1, tvp2, dCtx);

                try {
                    abvs.reset();
                    dOut.write(ValueTag.XS_BOOLEAN_TAG);
                    dOut.write(booleanResult ? 1 : 0);
                    result.set(abvs);
                } catch (Exception e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
            }

            protected boolean transformThenCompareTaggedValues(AbstractValueComparisonOperation aOp,
                    TaggedValuePointable tvp1, TaggedValuePointable tvp2, DynamicContext dCtx) throws SystemException {
                TaggedValuePointable tvp1new = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
                TaggedValuePointable tvp2new = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();

                try {
                    if (FunctionHelper.isDerivedFromInteger(tvp1.getTag())) {
                        abvsInteger1.reset();
                        FunctionHelper.getIntegerPointable(tvp1, dOutInteger1);
                        tvp1new.set(abvsInteger1.getByteArray(), abvsInteger1.getStartOffset(),
                                LongPointable.TYPE_TRAITS.getFixedLength() + 1);
                    } else {
                        tvp1new = tvp1;
                    }
                    if (FunctionHelper.isDerivedFromInteger(tvp2.getTag())) {
                        abvsInteger2.reset();
                        FunctionHelper.getIntegerPointable(tvp2, dOutInteger2);
                        tvp2new.set(abvsInteger2.getByteArray(), abvsInteger2.getStartOffset(),
                                LongPointable.TYPE_TRAITS.getFixedLength() + 1);
                    } else {
                        tvp2new = tvp2;
                    }

                    return FunctionHelper.compareTaggedValues(aOp, tvp1new, tvp2new, dCtx);
                } catch (SystemException se) {
                    throw se;
                } catch (Exception e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
            }
        };
    }

    protected abstract AbstractValueComparisonOperation createValueComparisonOperation();
}