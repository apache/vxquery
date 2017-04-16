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

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.TypedPointables;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;

public abstract class AbstractValueComparisonScalarEvaluatorFactory extends
        AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public AbstractValueComparisonScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws HyracksDataException {
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final DataOutput dOut = abvs.getDataOutput();
        final AbstractValueComparisonOperation aOp = createValueComparisonOperation();
        final DynamicContext dCtx = (DynamicContext) ctx.getJobletContext().getGlobalJobData();
        final SequencePointable seqp = (SequencePointable) SequencePointable.FACTORY.createPointable();

        final ArrayBackedValueStorage abvsInteger1 = new ArrayBackedValueStorage();
        final DataOutput dOutInteger1 = abvsInteger1.getDataOutput();
        final ArrayBackedValueStorage abvsInteger2 = new ArrayBackedValueStorage();
        final DataOutput dOutInteger2 = abvsInteger2.getDataOutput();

        final TaggedValuePointable tvp1new = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final TaggedValuePointable tvp2new = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final TypedPointables tp1 = new TypedPointables();
        final TypedPointables tp2 = new TypedPointables();

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                TaggedValuePointable tvp1 = args[0];
                TaggedValuePointable tvp2 = args[1];

                if (tvp1.getTag() == ValueTag.SEQUENCE_TAG) {
                    tvp1.getValue(seqp);
                    if (seqp.getEntryCount() == 0) {
                        XDMConstants.setEmptySequence(result);
                        return;
                    }
                    throw new SystemException(ErrorCode.XPTY0004);
                }
                if (tvp2.getTag() == ValueTag.SEQUENCE_TAG) {
                    tvp2.getValue(seqp);
                    if (seqp.getEntryCount() == 0) {
                        XDMConstants.setEmptySequence(result);
                        return;
                    }
                    throw new SystemException(ErrorCode.XPTY0004);
                }

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
                try {
                    if (tvp1.getTag() == ValueTag.XS_UNTYPED_ATOMIC_TAG) {
                        // Only need to change tag since the storage is the same for untyped atomic and string.
                        tvp1.getByteArray()[0] = ValueTag.XS_STRING_TAG;
                        tvp1new.set(tvp1);
                    } else if (FunctionHelper.isDerivedFromInteger(tvp1.getTag())) {
                        abvsInteger1.reset();
                        FunctionHelper.getIntegerPointable(tvp1, dOutInteger1, tp1);
                        tvp1new.set(abvsInteger1.getByteArray(), abvsInteger1.getStartOffset(),
                                LongPointable.TYPE_TRAITS.getFixedLength() + 1);
                    } else {
                        tvp1new.set(tvp1);
                    }
                    if (tvp2.getTag() == ValueTag.XS_UNTYPED_ATOMIC_TAG) {
                        // Only need to change tag since the storage is the same for untyped atomic and string.
                        tvp2.getByteArray()[0] = ValueTag.XS_STRING_TAG;
                        tvp2new.set(tvp2);
                    } else if (FunctionHelper.isDerivedFromInteger(tvp2.getTag())) {
                        abvsInteger2.reset();
                        FunctionHelper.getIntegerPointable(tvp2, dOutInteger2, tp2);
                        tvp2new.set(abvsInteger2.getByteArray(), abvsInteger2.getStartOffset(),
                                LongPointable.TYPE_TRAITS.getFixedLength() + 1);
                    } else {
                        tvp2new.set(tvp2);
                    }

                    return FunctionHelper.compareTaggedValues(aOp, tvp1new, tvp2new, dCtx, tp1, tp2);
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
