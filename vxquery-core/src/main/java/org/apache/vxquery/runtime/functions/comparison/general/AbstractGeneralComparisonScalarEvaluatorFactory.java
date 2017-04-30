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
package org.apache.vxquery.runtime.functions.comparison.general;

import java.io.DataOutput;

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.TypedPointables;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.runtime.functions.cast.AbstractCastToOperation;
import org.apache.vxquery.runtime.functions.cast.CastToAnyURIOperation;
import org.apache.vxquery.runtime.functions.cast.CastToBase64BinaryOperation;
import org.apache.vxquery.runtime.functions.cast.CastToBooleanOperation;
import org.apache.vxquery.runtime.functions.cast.CastToByteOperation;
import org.apache.vxquery.runtime.functions.cast.CastToDTDurationOperation;
import org.apache.vxquery.runtime.functions.cast.CastToDateOperation;
import org.apache.vxquery.runtime.functions.cast.CastToDateTimeOperation;
import org.apache.vxquery.runtime.functions.cast.CastToDecimalOperation;
import org.apache.vxquery.runtime.functions.cast.CastToDoubleOperation;
import org.apache.vxquery.runtime.functions.cast.CastToDurationOperation;
import org.apache.vxquery.runtime.functions.cast.CastToFloatOperation;
import org.apache.vxquery.runtime.functions.cast.CastToGDayOperation;
import org.apache.vxquery.runtime.functions.cast.CastToGMonthDayOperation;
import org.apache.vxquery.runtime.functions.cast.CastToGMonthOperation;
import org.apache.vxquery.runtime.functions.cast.CastToGYearMonthOperation;
import org.apache.vxquery.runtime.functions.cast.CastToGYearOperation;
import org.apache.vxquery.runtime.functions.cast.CastToHexBinaryOperation;
import org.apache.vxquery.runtime.functions.cast.CastToIntOperation;
import org.apache.vxquery.runtime.functions.cast.CastToIntegerOperation;
import org.apache.vxquery.runtime.functions.cast.CastToLongOperation;
import org.apache.vxquery.runtime.functions.cast.CastToNegativeIntegerOperation;
import org.apache.vxquery.runtime.functions.cast.CastToNonNegativeIntegerOperation;
import org.apache.vxquery.runtime.functions.cast.CastToNonPositiveIntegerOperation;
import org.apache.vxquery.runtime.functions.cast.CastToPositiveIntegerOperation;
import org.apache.vxquery.runtime.functions.cast.CastToQNameOperation;
import org.apache.vxquery.runtime.functions.cast.CastToShortOperation;
import org.apache.vxquery.runtime.functions.cast.CastToStringOperation;
import org.apache.vxquery.runtime.functions.cast.CastToTimeOperation;
import org.apache.vxquery.runtime.functions.cast.CastToUnsignedByteOperation;
import org.apache.vxquery.runtime.functions.cast.CastToUnsignedIntOperation;
import org.apache.vxquery.runtime.functions.cast.CastToUnsignedLongOperation;
import org.apache.vxquery.runtime.functions.cast.CastToUnsignedShortOperation;
import org.apache.vxquery.runtime.functions.cast.CastToUntypedAtomicOperation;
import org.apache.vxquery.runtime.functions.cast.CastToYMDurationOperation;
import org.apache.vxquery.runtime.functions.comparison.AbstractValueComparisonOperation;
import org.apache.vxquery.runtime.functions.util.AtomizeHelper;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;

public abstract class AbstractGeneralComparisonScalarEvaluatorFactory extends
        AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public AbstractGeneralComparisonScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws HyracksDataException {
        final AbstractValueComparisonOperation aOp = createValueComparisonOperation();
        final ArrayBackedValueStorage abvsInner1 = new ArrayBackedValueStorage();
        final DataOutput dOutInner1 = abvsInner1.getDataOutput();
        final ArrayBackedValueStorage abvsInner2 = new ArrayBackedValueStorage();
        final DataOutput dOutInner2 = abvsInner2.getDataOutput();

        final AtomizeHelper ah = new AtomizeHelper();
        final TypedPointables tp1 = new TypedPointables();
        final TypedPointables tp2 = new TypedPointables();
        final DynamicContext dCtx = (DynamicContext) ctx.getJobletContext().getGlobalJobData();
        final SequencePointable seqp1 = (SequencePointable) SequencePointable.FACTORY.createPointable();
        final SequencePointable seqp2 = (SequencePointable) SequencePointable.FACTORY.createPointable();
        final VoidPointable p1 = (VoidPointable) VoidPointable.FACTORY.createPointable();
        final VoidPointable p2 = (VoidPointable) VoidPointable.FACTORY.createPointable();
        final TaggedValuePointable tvpSeq1 = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final TaggedValuePointable tvpSeq2 = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final TaggedValuePointable tvpTransform1 = (TaggedValuePointable) TaggedValuePointable.FACTORY
                .createPointable();
        final TaggedValuePointable tvpTransform2 = (TaggedValuePointable) TaggedValuePointable.FACTORY
                .createPointable();
        final TaggedValuePointable tvpCompare1 = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final TaggedValuePointable tvpCompare2 = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            AbstractCastToOperation aCastToOp = new CastToStringOperation();

            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                boolean booleanResult = false;
                TaggedValuePointable tvpArg1 = args[0];
                TaggedValuePointable tvpArg2 = args[1];
                try {
                    if (tvpArg1.getTag() == ValueTag.SEQUENCE_TAG) {
                        tvpArg1.getValue(seqp1);
                        int seqLen = seqp1.getEntryCount();
                        for (int j = 0; j < seqLen; ++j) {
                            seqp1.getEntry(j, p1);
                            tvpSeq1.set(p1.getByteArray(), p1.getStartOffset(), p1.getLength());
                            if (evaluateTaggedValueArgument2(aOp, tvpSeq1, tvpArg2, dCtx)) {
                                booleanResult = true;
                                break;
                            }
                        }
                    } else {
                        booleanResult = evaluateTaggedValueArgument2(aOp, tvpArg1, tvpArg2, dCtx);
                    }

                    byte[] byteResult = new byte[2];
                    byteResult[0] = ValueTag.XS_BOOLEAN_TAG;
                    byteResult[1] = (byte) (booleanResult ? 1 : 0);
                    result.set(byteResult, 0, 2);
                } catch (SystemException se) {
                    throw se;
                } catch (Exception e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
            }

            /**
             * Check the second argument for a sequence and loop if required.
             *
             * @param aOp
             * @param tvpArg1
             * @param tvpArg2
             * @param dCtx
             * @return
             * @throws SystemException
             */
            protected boolean evaluateTaggedValueArgument2(AbstractValueComparisonOperation aOp,
                    TaggedValuePointable tvpArg1, TaggedValuePointable tvpArg2, DynamicContext dCtx)
                    throws SystemException {
                try {
                    if (tvpArg2.getTag() == ValueTag.SEQUENCE_TAG) {
                        tvpArg2.getValue(seqp2);
                        int seqLen = seqp2.getEntryCount();
                        for (int j = 0; j < seqLen; ++j) {
                            seqp2.getEntry(j, p2);
                            tvpSeq2.set(p2.getByteArray(), p2.getStartOffset(), p2.getLength());
                            if (transformThenCompareTaggedValues(aOp, tvpArg1, tvpSeq2, dCtx)) {
                                return true;
                            }
                        }
                    } else {
                        return transformThenCompareTaggedValues(aOp, tvpArg1, tvpArg2, dCtx);
                    }
                } catch (SystemException se) {
                    throw se;
                } catch (Exception e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
                return false;
            }

            /**
             * Transform the values into values supported for general comparison.
             *
             * @param aOp
             * @param tvpArg1
             * @param tvpArg2
             * @param dCtx
             * @return
             * @throws SystemException
             */
            protected boolean transformThenCompareTaggedValues(AbstractValueComparisonOperation aOp,
                    TaggedValuePointable tvpArg1, TaggedValuePointable tvpArg2, DynamicContext dCtx)
                    throws SystemException {
                boolean tagTransformed1 = false, tagTransformed2 = false;
                abvsInner1.reset();
                abvsInner2.reset();
                tvpTransform1.set(tvpArg1);
                tvpTransform2.set(tvpArg2);
                int tid1 = FunctionHelper.getBaseTypeForGeneralComparisons(tvpTransform1.getTag());
                int tid2 = FunctionHelper.getBaseTypeForGeneralComparisons(tvpTransform2.getTag());

                // Converted tags
                try {
                    // Converts node tree's into untyped atomic values that can then be compared as atomic items.
                    if (tid1 == ValueTag.NODE_TREE_TAG && tid2 == ValueTag.NODE_TREE_TAG) {
                        ah.atomize(tvpArg1, ppool, tvpTransform1);
                        ah.atomize(tvpArg2, ppool, tvpTransform2);
                        tid1 = FunctionHelper.getBaseTypeForGeneralComparisons(tvpTransform1.getTag());
                        tid2 = FunctionHelper.getBaseTypeForGeneralComparisons(tvpTransform2.getTag());
                    } else if (tid1 == ValueTag.NODE_TREE_TAG) {
                        ah.atomize(tvpArg1, ppool, tvpTransform1);
                        tid1 = FunctionHelper.getBaseTypeForGeneralComparisons(tvpTransform1.getTag());
                    } else if (tid2 == ValueTag.NODE_TREE_TAG) {
                        ah.atomize(tvpArg2, ppool, tvpTransform2);
                        tid2 = FunctionHelper.getBaseTypeForGeneralComparisons(tvpTransform2.getTag());
                    }

                    // Set up value comparison tagged value pointables.
                    if (tid1 == ValueTag.XS_UNTYPED_ATOMIC_TAG && tid2 == ValueTag.XS_UNTYPED_ATOMIC_TAG) {
                        // Only need to change tag since the storage is the same for untyped atomic and string.
                        dOutInner1.write(tvpTransform1.getByteArray(), tvpTransform1.getStartOffset(),
                                tvpTransform1.getLength());
                        tvpCompare1.set(abvsInner1.getByteArray(), abvsInner1.getStartOffset(), abvsInner1.getLength());
                        tvpCompare1.getByteArray()[0] = ValueTag.XS_STRING_TAG;
                        tagTransformed1 = true;
                        dOutInner2.write(tvpTransform2.getByteArray(), tvpTransform2.getStartOffset(),
                                tvpTransform2.getLength());
                        tvpCompare2.set(abvsInner2.getByteArray(), abvsInner2.getStartOffset(), abvsInner2.getLength());
                        tvpCompare2.getByteArray()[0] = ValueTag.XS_STRING_TAG;
                        tagTransformed2 = true;
                    } else if (tid1 == ValueTag.XS_UNTYPED_ATOMIC_TAG) {
                        tid1 = tid2;
                        getCastToOperator(tid2);
                        tvpTransform1.getValue(tp1.utf8sp);
                        aCastToOp.convertUntypedAtomic(tp1.utf8sp, dOutInner1);
                        tvpCompare1.set(abvsInner1.getByteArray(), abvsInner1.getStartOffset(), abvsInner1.getLength());
                        tagTransformed1 = true;
                    } else if (tid2 == ValueTag.XS_UNTYPED_ATOMIC_TAG) {
                        tid2 = tid1;
                        getCastToOperator(tid1);
                        tvpTransform2.getValue(tp2.utf8sp);
                        aCastToOp.convertUntypedAtomic(tp2.utf8sp, dOutInner2);
                        tvpCompare2.set(abvsInner2.getByteArray(), abvsInner2.getStartOffset(), abvsInner2.getLength());
                        tagTransformed2 = true;
                    }
                    // Copy over the values not changed and upgrade numeric values to double.
                    if (!tagTransformed1) {
                        tvpCompare1.set(tvpTransform1);
                        if (FunctionHelper.isDerivedFromDouble(tvpCompare1.getTag())) {
                            FunctionHelper.getDoublePointable(tvpTransform1, dOutInner1, tp1);
                            tvpCompare1.set(abvsInner1.getByteArray(), abvsInner1.getStartOffset(),
                                    DoublePointable.TYPE_TRAITS.getFixedLength() + 1);
                            tagTransformed1 = true;
                        }
                    }
                    if (!tagTransformed2) {
                        tvpCompare2.set(tvpTransform2);
                        if (FunctionHelper.isDerivedFromDouble(tvpCompare2.getTag())) {
                            FunctionHelper.getDoublePointable(tvpTransform2, dOutInner2, tp2);
                            tvpCompare2.set(abvsInner2.getByteArray(), abvsInner2.getStartOffset(),
                                    DoublePointable.TYPE_TRAITS.getFixedLength() + 1);
                            tagTransformed2 = true;
                        }
                    }
                } catch (SystemException se) {
                    throw se;
                } catch (Exception e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
                return FunctionHelper.compareTaggedValues(aOp, tvpCompare1, tvpCompare2, dCtx, tp1, tp2);
            }

            private void getCastToOperator(int tid) {
                switch (tid) {
                    case ValueTag.XS_ANY_URI_TAG:
                        aCastToOp = new CastToAnyURIOperation();
                        break;
                    case ValueTag.XS_BASE64_BINARY_TAG:
                        aCastToOp = new CastToBase64BinaryOperation();
                        break;
                    case ValueTag.XS_BOOLEAN_TAG:
                        aCastToOp = new CastToBooleanOperation();
                        break;
                    case ValueTag.XS_DATE_TAG:
                        aCastToOp = new CastToDateOperation();
                        break;
                    case ValueTag.XS_DATETIME_TAG:
                        aCastToOp = new CastToDateTimeOperation();
                        break;
                    case ValueTag.XS_DAY_TIME_DURATION_TAG:
                        aCastToOp = new CastToDTDurationOperation();
                        break;
                    case ValueTag.XS_DURATION_TAG:
                        aCastToOp = new CastToDurationOperation();
                        break;
                    case ValueTag.XS_HEX_BINARY_TAG:
                        aCastToOp = new CastToHexBinaryOperation();
                        break;
                    case ValueTag.XS_G_DAY_TAG:
                        aCastToOp = new CastToGDayOperation();
                        break;
                    case ValueTag.XS_G_MONTH_DAY_TAG:
                        aCastToOp = new CastToGMonthDayOperation();
                        break;
                    case ValueTag.XS_G_MONTH_TAG:
                        aCastToOp = new CastToGMonthOperation();
                        break;
                    case ValueTag.XS_G_YEAR_MONTH_TAG:
                        aCastToOp = new CastToGYearMonthOperation();
                        break;
                    case ValueTag.XS_G_YEAR_TAG:
                        aCastToOp = new CastToGYearOperation();
                        break;
                    case ValueTag.XS_QNAME_TAG:
                        aCastToOp = new CastToQNameOperation();
                        break;
                    case ValueTag.XS_STRING_TAG:
                        aCastToOp = new CastToStringOperation();
                        break;
                    case ValueTag.XS_TIME_TAG:
                        aCastToOp = new CastToTimeOperation();
                        break;
                    case ValueTag.XS_UNTYPED_ATOMIC_TAG:
                        aCastToOp = new CastToUntypedAtomicOperation();
                        break;
                    case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                        aCastToOp = new CastToYMDurationOperation();
                        break;
                    case ValueTag.XS_DECIMAL_TAG:
                        aCastToOp = new CastToDecimalOperation();
                        break;
                    case ValueTag.XS_DOUBLE_TAG:
                        aCastToOp = new CastToDoubleOperation();
                        break;
                    case ValueTag.XS_FLOAT_TAG:
                        aCastToOp = new CastToFloatOperation();
                        break;
                    case ValueTag.XS_INTEGER_TAG:
                        aCastToOp = new CastToIntegerOperation();
                        break;
                    case ValueTag.XS_NON_POSITIVE_INTEGER_TAG:
                        aCastToOp = new CastToNonPositiveIntegerOperation();
                        break;
                    case ValueTag.XS_NEGATIVE_INTEGER_TAG:
                        aCastToOp = new CastToNegativeIntegerOperation();
                        break;
                    case ValueTag.XS_LONG_TAG:
                        aCastToOp = new CastToLongOperation();
                        break;
                    case ValueTag.XS_NON_NEGATIVE_INTEGER_TAG:
                        aCastToOp = new CastToNonNegativeIntegerOperation();
                        break;
                    case ValueTag.XS_UNSIGNED_LONG_TAG:
                        aCastToOp = new CastToUnsignedLongOperation();
                        break;
                    case ValueTag.XS_POSITIVE_INTEGER_TAG:
                        aCastToOp = new CastToPositiveIntegerOperation();
                        break;
                    case ValueTag.XS_INT_TAG:
                        aCastToOp = new CastToIntOperation();
                        break;
                    case ValueTag.XS_UNSIGNED_INT_TAG:
                        aCastToOp = new CastToUnsignedIntOperation();
                        break;
                    case ValueTag.XS_SHORT_TAG:
                        aCastToOp = new CastToShortOperation();
                        break;
                    case ValueTag.XS_UNSIGNED_SHORT_TAG:
                        aCastToOp = new CastToUnsignedShortOperation();
                        break;
                    case ValueTag.XS_BYTE_TAG:
                        aCastToOp = new CastToByteOperation();
                        break;
                    case ValueTag.XS_UNSIGNED_BYTE_TAG:
                        aCastToOp = new CastToUnsignedByteOperation();
                        break;
                    default:
                        aCastToOp = new CastToUntypedAtomicOperation();
                }
            }

        };
    }

    protected abstract AbstractValueComparisonOperation createValueComparisonOperation();
}
