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
import java.io.IOException;

import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSBinaryPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDatePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDateTimePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDurationPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSQNamePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSTimePointable;
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
import org.apache.vxquery.types.BuiltinTypeRegistry;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.BooleanPointable;
import edu.uci.ics.hyracks.data.std.primitive.BytePointable;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.primitive.ShortPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public abstract class AbstractGeneralComparisonScalarEvaluatorFactory extends
        AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public AbstractGeneralComparisonScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final AbstractValueComparisonOperation aOp = createValueComparisonOperation();
        final ArrayBackedValueStorage abvsInner1 = new ArrayBackedValueStorage();
        final DataOutput dOutInner1 = abvsInner1.getDataOutput();
        final ArrayBackedValueStorage abvsInner2 = new ArrayBackedValueStorage();
        final DataOutput dOutInner2 = abvsInner2.getDataOutput();
        final TypedPointables tp = new TypedPointables();
        final TypedPointables tp1 = new TypedPointables();
        final TypedPointables tp2 = new TypedPointables();
        final DynamicContext dCtx = (DynamicContext) ctx.getJobletContext().getGlobalJobData();

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            AbstractCastToOperation aCastToOp = new CastToStringOperation();

            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                boolean booleanResult = false;
                boolean tagTransformed1 = false, tagTransformed2 = false;
                TaggedValuePointable tvpArg1 = args[0];
                TaggedValuePointable tvpArg2 = args[1];
                int tid1 = getBaseTypeForComparisons(tvpArg1.getTag());
                int tid2 = getBaseTypeForComparisons(tvpArg2.getTag());
                abvsInner1.reset();
                abvsInner2.reset();
                // Converted tags
                TaggedValuePointable tvp1 = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
                TaggedValuePointable tvp2 = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
                try {
                    // Set up value comparison tagged value pointables.
                    if (tid1 == ValueTag.XS_UNTYPED_ATOMIC_TAG && tid2 == ValueTag.XS_UNTYPED_ATOMIC_TAG) {
                        // Only need to change tag since the storage is the same for untyped atomic and string.
                        tid1 = ValueTag.XS_STRING_TAG;
                        tid2 = ValueTag.XS_STRING_TAG;
                    } else if (tid1 == ValueTag.XS_UNTYPED_ATOMIC_TAG) {
                        tid1 = tid2;
                        getCastToOperator(tid2);
                        tvpArg1.getValue(tp1.utf8sp);
                        aCastToOp.convertUntypedAtomic(tp1.utf8sp, dOutInner1);
                        tvp1.set(abvsInner1.getByteArray(), abvsInner1.getStartOffset(), abvsInner1.getLength());
                        tagTransformed1 = true;
                    } else if (tid2 == ValueTag.XS_UNTYPED_ATOMIC_TAG) {
                        tid2 = tid1;
                        getCastToOperator(tid1);
                        tvpArg2.getValue(tp2.utf8sp);
                        aCastToOp.convertUntypedAtomic(tp2.utf8sp, dOutInner2);
                        tvp2.set(abvsInner2.getByteArray(), abvsInner2.getStartOffset(), abvsInner2.getLength());
                        tagTransformed2 = true;
                    }
                    // Copy over the values not changed and upgrade numeric values to double.
                    if (!tagTransformed1) {
                        tvp1 = tvpArg1;
                        switch (tvp1.getTag()) {
                            case ValueTag.XS_DECIMAL_TAG:
                            case ValueTag.XS_DOUBLE_TAG:
                            case ValueTag.XS_FLOAT_TAG:
                            case ValueTag.XS_INTEGER_TAG:
                            case ValueTag.XS_NON_POSITIVE_INTEGER_TAG:
                            case ValueTag.XS_NEGATIVE_INTEGER_TAG:
                            case ValueTag.XS_LONG_TAG:
                            case ValueTag.XS_NON_NEGATIVE_INTEGER_TAG:
                            case ValueTag.XS_UNSIGNED_LONG_TAG:
                            case ValueTag.XS_POSITIVE_INTEGER_TAG:
                            case ValueTag.XS_INT_TAG:
                            case ValueTag.XS_UNSIGNED_INT_TAG:
                            case ValueTag.XS_SHORT_TAG:
                            case ValueTag.XS_UNSIGNED_SHORT_TAG:
                            case ValueTag.XS_BYTE_TAG:
                            case ValueTag.XS_UNSIGNED_BYTE_TAG:
                                getDoublePointable(tvpArg1, dOutInner1);
                                tvp1.set(abvsInner1.getByteArray(), abvsInner1.getStartOffset(),
                                        DoublePointable.TYPE_TRAITS.getFixedLength() + 1);
                                tagTransformed1 = true;
                        }
                    }
                    if (!tagTransformed2) {
                        tvp2 = tvpArg2;
                        switch (tvp2.getTag()) {
                            case ValueTag.XS_DECIMAL_TAG:
                            case ValueTag.XS_DOUBLE_TAG:
                            case ValueTag.XS_FLOAT_TAG:
                            case ValueTag.XS_INTEGER_TAG:
                            case ValueTag.XS_NON_POSITIVE_INTEGER_TAG:
                            case ValueTag.XS_NEGATIVE_INTEGER_TAG:
                            case ValueTag.XS_LONG_TAG:
                            case ValueTag.XS_NON_NEGATIVE_INTEGER_TAG:
                            case ValueTag.XS_UNSIGNED_LONG_TAG:
                            case ValueTag.XS_POSITIVE_INTEGER_TAG:
                            case ValueTag.XS_INT_TAG:
                            case ValueTag.XS_UNSIGNED_INT_TAG:
                            case ValueTag.XS_SHORT_TAG:
                            case ValueTag.XS_UNSIGNED_SHORT_TAG:
                            case ValueTag.XS_BYTE_TAG:
                            case ValueTag.XS_UNSIGNED_BYTE_TAG:
                                getDoublePointable(tvpArg1, dOutInner2);
                                tvp2.set(abvsInner1.getByteArray(), abvsInner1.getStartOffset(),
                                        DoublePointable.TYPE_TRAITS.getFixedLength() + 1);
                                tagTransformed2 = true;
                        }
                    }

                    // Run the value comparison.
                    switch (tid1) {

                        case ValueTag.XS_DOUBLE_TAG:
                            tvp1.getValue(tp1.doublep);
                            switch (tid2) {
                                case ValueTag.XS_DOUBLE_TAG:
                                    tvp2.getValue(tp2.doublep);
                                    booleanResult = aOp.operateDoubleDouble(tp1.doublep, tp2.doublep);
                                    break;
                            }
                            break;

                        case ValueTag.XS_BOOLEAN_TAG:
                            tvp1.getValue(tp1.boolp);
                            switch (tid2) {
                                case ValueTag.XS_BOOLEAN_TAG:
                                    tvp2.getValue(tp2.boolp);
                                    booleanResult = aOp.operateBooleanBoolean(tp1.boolp, tp2.boolp);
                                    break;
                            }
                            break;

                        case ValueTag.XS_STRING_TAG:
                            tvp1.getValue(tp1.utf8sp);
                            switch (tid2) {
                                case ValueTag.XS_STRING_TAG:
                                    tvp2.getValue(tp2.utf8sp);
                                    booleanResult = aOp.operateStringString(tp1.utf8sp, tp2.utf8sp);
                                    break;
                            }
                            break;

                        case ValueTag.XS_DATE_TAG:
                            tvp1.getValue(tp1.datep);
                            switch (tid2) {
                                case ValueTag.XS_DATE_TAG:
                                    tvp2.getValue(tp2.datep);
                                    booleanResult = aOp.operateDateDate(tp1.datep, tp2.datep, dCtx);
                                    break;
                                default:
                                    // Cross comparisons between DateTime, Date and Time are not supported.
                                    throw new SystemException(ErrorCode.XPTY0004);
                            }
                            break;

                        case ValueTag.XS_DATETIME_TAG:
                            tvp1.getValue(tp1.datetimep);
                            switch (tid2) {
                                case ValueTag.XS_DATETIME_TAG:
                                    tvp2.getValue(tp2.datetimep);
                                    booleanResult = aOp.operateDatetimeDatetime(tp1.datetimep, tp2.datetimep, dCtx);
                                    break;
                                default:
                                    // Cross comparisons between DateTime, Date and Time are not supported.
                                    throw new SystemException(ErrorCode.XPTY0004);
                            }
                            break;

                        case ValueTag.XS_TIME_TAG:
                            tvp1.getValue(tp1.timep);
                            switch (tid2) {
                                case ValueTag.XS_TIME_TAG:
                                    tvp2.getValue(tp2.timep);
                                    booleanResult = aOp.operateTimeTime(tp1.timep, tp2.timep, dCtx);
                                    break;
                                default:
                                    // Cross comparisons between DateTime, Date and Time are not supported.
                                    throw new SystemException(ErrorCode.XPTY0004);
                            }
                            break;

                        case ValueTag.XS_DURATION_TAG:
                            tvp1.getValue(tp1.durationp);
                            switch (tid2) {
                                case ValueTag.XS_DAY_TIME_DURATION_TAG:
                                    tvp2.getValue(tp2.longp);
                                    booleanResult = aOp.operateDurationDTDuration(tp1.durationp, tp2.longp);
                                    break;
                                case ValueTag.XS_DURATION_TAG:
                                    tvp2.getValue(tp2.durationp);
                                    booleanResult = aOp.operateDurationDuration(tp1.durationp, tp2.durationp);
                                    break;
                                case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                                    tvp2.getValue(tp2.intp);
                                    booleanResult = aOp.operateDurationYMDuration(tp1.durationp, tp2.intp);
                                    break;
                            }
                            break;

                        case ValueTag.XS_DAY_TIME_DURATION_TAG:
                            tvp1.getValue(tp1.longp);
                            switch (tid2) {
                                case ValueTag.XS_DAY_TIME_DURATION_TAG:
                                    tvp2.getValue(tp2.longp);
                                    booleanResult = aOp.operateDTDurationDTDuration(tp1.longp, tp2.longp);
                                    break;
                                case ValueTag.XS_DURATION_TAG:
                                    tvp2.getValue(tp2.durationp);
                                    booleanResult = aOp.operateDTDurationDuration(tp1.longp, tp2.durationp);
                                    break;
                            }
                            break;

                        case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                            tvp1.getValue(tp1.intp);
                            switch (tid2) {
                                case ValueTag.XS_DURATION_TAG:
                                    tvp2.getValue(tp2.durationp);
                                    booleanResult = aOp.operateYMDurationDuration(tp1.intp, tp2.durationp);
                                    break;
                                case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                                    tvp2.getValue(tp2.intp);
                                    booleanResult = aOp.operateYMDurationYMDuration(tp1.intp, tp2.intp);
                                    break;
                            }
                            break;

                        case ValueTag.XS_G_DAY_TAG:
                            tvp1.getValue(tp1.datep);
                            switch (tid2) {
                                case ValueTag.XS_G_DAY_TAG:
                                    tvp2.getValue(tp2.datep);
                                    booleanResult = aOp.operateGDayGDay(tp1.datep, tp2.datep, dCtx);
                                    break;
                            }
                            break;

                        case ValueTag.XS_G_MONTH_DAY_TAG:
                            tvp1.getValue(tp1.datep);
                            switch (tid2) {
                                case ValueTag.XS_G_MONTH_DAY_TAG:
                                    tvp2.getValue(tp2.datep);
                                    booleanResult = aOp.operateGMonthDayGMonthDay(tp1.datep, tp2.datep, dCtx);
                                    break;
                            }
                            break;

                        case ValueTag.XS_G_MONTH_TAG:
                            tvp1.getValue(tp1.datep);
                            switch (tid2) {
                                case ValueTag.XS_G_MONTH_TAG:
                                    tvp2.getValue(tp2.datep);
                                    booleanResult = aOp.operateGMonthGMonth(tp1.datep, tp2.datep, dCtx);
                                    break;
                            }
                            break;

                        case ValueTag.XS_G_YEAR_MONTH_TAG:
                            tvp1.getValue(tp1.datep);
                            switch (tid2) {
                                case ValueTag.XS_G_YEAR_MONTH_TAG:
                                    tvp2.getValue(tp2.datep);
                                    booleanResult = aOp.operateGYearMonthGYearMonth(tp1.datep, tp2.datep, dCtx);
                                    break;
                            }
                            break;

                        case ValueTag.XS_G_YEAR_TAG:
                            tvp1.getValue(tp1.datep);
                            switch (tid2) {
                                case ValueTag.XS_G_YEAR_TAG:
                                    tvp2.getValue(tp2.datep);
                                    booleanResult = aOp.operateGYearGYear(tp1.datep, tp2.datep, dCtx);
                                    break;
                            }
                            break;

                        case ValueTag.XS_HEX_BINARY_TAG:
                            tvp1.getValue(tp1.binaryp);
                            switch (tid2) {
                                case ValueTag.XS_HEX_BINARY_TAG:
                                    tvp2.getValue(tp2.binaryp);
                                    booleanResult = aOp.operateHexBinaryHexBinary(tp1.binaryp, tp2.binaryp);
                                    break;
                            }
                            break;

                        case ValueTag.XS_BASE64_BINARY_TAG:
                            tvp1.getValue(tp1.binaryp);
                            switch (tid2) {
                                case ValueTag.XS_BASE64_BINARY_TAG:
                                    tvp2.getValue(tp2.binaryp);
                                    booleanResult = aOp.operateBase64BinaryBase64Binary(tp1.binaryp, tp2.binaryp);
                                    break;
                            }
                            break;

                        case ValueTag.XS_ANY_URI_TAG:
                            tvp1.getValue(tp1.utf8sp);
                            switch (tid2) {
                                case ValueTag.XS_ANY_URI_TAG:
                                    tvp2.getValue(tp2.utf8sp);
                                    booleanResult = aOp.operateAnyURIAnyURI(tp1.utf8sp, tp2.utf8sp);
                                    break;
                            }
                            break;

                        case ValueTag.XS_QNAME_TAG:
                            tvp1.getValue(tp1.qnamep);
                            switch (tid2) {
                                case ValueTag.XS_QNAME_TAG:
                                    tvp2.getValue(tp2.qnamep);
                                    booleanResult = aOp.operateQNameQName(tp1.qnamep, tp2.qnamep);
                                    break;
                            }
                            break;

                        case ValueTag.XS_NOTATION_TAG:
                            tvp1.getValue(tp1.utf8sp);
                            switch (tid2) {
                                case ValueTag.XS_NOTATION_TAG:
                                    tvp2.getValue(tp2.utf8sp);
                                    booleanResult = aOp.operateNotationNotation(tp1.utf8sp, tp2.utf8sp);
                                    break;
                            }
                            break;
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

            private void getDoublePointable(TaggedValuePointable tvp, DataOutput dOut) throws SystemException,
                    IOException {
                double value;
                switch (tvp.getTag()) {
                    case ValueTag.XS_DECIMAL_TAG:
                        tvp.getValue(tp.decp);
                        value = tp.decp.doubleValue();
                        break;

                    case ValueTag.XS_DOUBLE_TAG:
                        tvp.getValue(tp.doublep);
                        value = tp.doublep.doubleValue();
                        break;

                    case ValueTag.XS_FLOAT_TAG:
                        tvp.getValue(tp.floatp);
                        value = tp.floatp.doubleValue();
                        break;

                    case ValueTag.XS_INTEGER_TAG:
                    case ValueTag.XS_LONG_TAG:
                    case ValueTag.XS_NEGATIVE_INTEGER_TAG:
                    case ValueTag.XS_NON_POSITIVE_INTEGER_TAG:
                    case ValueTag.XS_NON_NEGATIVE_INTEGER_TAG:
                    case ValueTag.XS_POSITIVE_INTEGER_TAG:
                    case ValueTag.XS_UNSIGNED_INT_TAG:
                    case ValueTag.XS_UNSIGNED_LONG_TAG:
                        tvp.getValue(tp.longp);
                        value = tp.longp.doubleValue();
                        break;

                    case ValueTag.XS_INT_TAG:
                    case ValueTag.XS_UNSIGNED_SHORT_TAG:
                        tvp.getValue(tp.intp);
                        value = tp.intp.doubleValue();
                        break;

                    case ValueTag.XS_SHORT_TAG:
                    case ValueTag.XS_UNSIGNED_BYTE_TAG:
                        tvp.getValue(tp.shortp);
                        value = tp.shortp.doubleValue();
                        break;

                    case ValueTag.XS_BYTE_TAG:
                        tvp.getValue(tp.bytep);
                        value = tp.bytep.doubleValue();
                        break;

                    default:
                        value = 0;
                }
                dOut.write(ValueTag.XS_DOUBLE_TAG);
                dOut.writeDouble(value);
            }

            private int getBaseTypeForComparisons(int tid) throws SystemException {
                while (true) {
                    switch (tid) {
                        case ValueTag.XS_ANY_URI_TAG:
                        case ValueTag.XS_BASE64_BINARY_TAG:
                        case ValueTag.XS_BOOLEAN_TAG:
                        case ValueTag.XS_DATE_TAG:
                        case ValueTag.XS_DATETIME_TAG:
                        case ValueTag.XS_DAY_TIME_DURATION_TAG:
                        case ValueTag.XS_DURATION_TAG:
                        case ValueTag.XS_HEX_BINARY_TAG:
                        case ValueTag.XS_G_DAY_TAG:
                        case ValueTag.XS_G_MONTH_DAY_TAG:
                        case ValueTag.XS_G_MONTH_TAG:
                        case ValueTag.XS_G_YEAR_MONTH_TAG:
                        case ValueTag.XS_G_YEAR_TAG:
                        case ValueTag.XS_QNAME_TAG:
                        case ValueTag.XS_STRING_TAG:
                        case ValueTag.XS_TIME_TAG:
                        case ValueTag.XS_UNTYPED_ATOMIC_TAG:
                        case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                            return tid;
                        case ValueTag.XS_DECIMAL_TAG:
                        case ValueTag.XS_DOUBLE_TAG:
                        case ValueTag.XS_FLOAT_TAG:
                        case ValueTag.XS_INTEGER_TAG:
                        case ValueTag.XS_NON_POSITIVE_INTEGER_TAG:
                        case ValueTag.XS_NEGATIVE_INTEGER_TAG:
                        case ValueTag.XS_LONG_TAG:
                        case ValueTag.XS_NON_NEGATIVE_INTEGER_TAG:
                        case ValueTag.XS_UNSIGNED_LONG_TAG:
                        case ValueTag.XS_POSITIVE_INTEGER_TAG:
                        case ValueTag.XS_INT_TAG:
                        case ValueTag.XS_UNSIGNED_INT_TAG:
                        case ValueTag.XS_SHORT_TAG:
                        case ValueTag.XS_UNSIGNED_SHORT_TAG:
                        case ValueTag.XS_BYTE_TAG:
                        case ValueTag.XS_UNSIGNED_BYTE_TAG:
                            return ValueTag.XS_DOUBLE_TAG;

                        case ValueTag.XS_ANY_ATOMIC_TAG:
                            throw new SystemException(ErrorCode.XPTY0004);

                        default:
                            tid = BuiltinTypeRegistry.INSTANCE.getSchemaTypeById(tid).getBaseType().getTypeId();
                    }
                }
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

    private static class TypedPointables {
        BooleanPointable boolp = (BooleanPointable) BooleanPointable.FACTORY.createPointable();
        BytePointable bytep = (BytePointable) BytePointable.FACTORY.createPointable();
        DoublePointable doublep = (DoublePointable) DoublePointable.FACTORY.createPointable();
        FloatPointable floatp = (FloatPointable) FloatPointable.FACTORY.createPointable();
        IntegerPointable intp = (IntegerPointable) IntegerPointable.FACTORY.createPointable();
        LongPointable longp = (LongPointable) LongPointable.FACTORY.createPointable();
        ShortPointable shortp = (ShortPointable) ShortPointable.FACTORY.createPointable();
        UTF8StringPointable utf8sp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        XSBinaryPointable binaryp = (XSBinaryPointable) XSBinaryPointable.FACTORY.createPointable();
        XSDatePointable datep = (XSDatePointable) XSDatePointable.FACTORY.createPointable();
        XSDateTimePointable datetimep = (XSDateTimePointable) XSDateTimePointable.FACTORY.createPointable();
        XSDecimalPointable decp = (XSDecimalPointable) XSDecimalPointable.FACTORY.createPointable();
        XSDurationPointable durationp = (XSDurationPointable) XSDurationPointable.FACTORY.createPointable();
        XSTimePointable timep = (XSTimePointable) XSTimePointable.FACTORY.createPointable();
        XSQNamePointable qnamep = (XSQNamePointable) XSQNamePointable.FACTORY.createPointable();
    }

    protected abstract AbstractValueComparisonOperation createValueComparisonOperation();
}