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
package org.apache.vxquery.runtime.functions.castable;

import java.io.DataOutput;

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.TypedPointables;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.type.AbstractTypeScalarEvaluatorFactory;
import org.apache.vxquery.types.BuiltinTypeRegistry;
import org.apache.vxquery.types.SequenceType;

public class CastableScalarEvaluatorFactory extends AbstractTypeScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public CastableScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws HyracksDataException {
        return new AbstractTypeScalarEvaluator(args, ctx) {
            final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
            final DataOutput dOut = abvs.getDataOutput();
            final TypedPointables tp = new TypedPointables();
            AbstractCastableAsOperation aOp = new CastableAsStringOperation();

            @Override
            protected void evaluate(TaggedValuePointable tvp, IPointable result) throws SystemException {
                abvs.reset();
                int tid = tvp.getTag();
                try {
                    switch (tid) {
                        case ValueTag.XS_ANY_URI_TAG:
                            tvp.getValue(tp.utf8sp);
                            aOp.convertAnyURI(tp.utf8sp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_BASE64_BINARY_TAG:
                            tvp.getValue(tp.binaryp);
                            aOp.convertBase64Binary(tp.binaryp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_BOOLEAN_TAG:
                            tvp.getValue(tp.boolp);
                            aOp.convertBoolean(tp.boolp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_BYTE_TAG:
                            tvp.getValue(tp.bytep);
                            aOp.convertByte(tp.bytep, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_DATE_TAG:
                            tvp.getValue(tp.datep);
                            aOp.convertDate(tp.datep, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_DATETIME_TAG:
                            tvp.getValue(tp.datetimep);
                            aOp.convertDatetime(tp.datetimep, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_DAY_TIME_DURATION_TAG:
                            tvp.getValue(tp.longp);
                            aOp.convertDTDuration(tp.longp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_DECIMAL_TAG:
                            tvp.getValue(tp.decp);
                            aOp.convertDecimal(tp.decp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_DOUBLE_TAG:
                            tvp.getValue(tp.doublep);
                            aOp.convertDouble(tp.doublep, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_DURATION_TAG:
                            tvp.getValue(tp.durationp);
                            aOp.convertDuration(tp.durationp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_FLOAT_TAG:
                            tvp.getValue(tp.floatp);
                            aOp.convertFloat(tp.floatp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_G_DAY_TAG:
                            tvp.getValue(tp.datep);
                            aOp.convertGDay(tp.datep, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_G_MONTH_TAG:
                            tvp.getValue(tp.datep);
                            aOp.convertGMonth(tp.datep, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_G_MONTH_DAY_TAG:
                            tvp.getValue(tp.datep);
                            aOp.convertGMonthDay(tp.datep, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_G_YEAR_TAG:
                            tvp.getValue(tp.datep);
                            aOp.convertGYear(tp.datep, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_G_YEAR_MONTH_TAG:
                            tvp.getValue(tp.datep);
                            aOp.convertGYearMonth(tp.datep, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_HEX_BINARY_TAG:
                            tvp.getValue(tp.binaryp);
                            aOp.convertHexBinary(tp.binaryp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_INT_TAG:
                            tvp.getValue(tp.intp);
                            aOp.convertInt(tp.intp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_INTEGER_TAG:
                            tvp.getValue(tp.longp);
                            aOp.convertInteger(tp.longp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_LONG_TAG:
                            tvp.getValue(tp.longp);
                            aOp.convertLong(tp.longp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_NEGATIVE_INTEGER_TAG:
                            tvp.getValue(tp.longp);
                            aOp.convertNegativeInteger(tp.longp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_NON_NEGATIVE_INTEGER_TAG:
                            tvp.getValue(tp.longp);
                            aOp.convertNonNegativeInteger(tp.longp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_NON_POSITIVE_INTEGER_TAG:
                            tvp.getValue(tp.longp);
                            aOp.convertNonPositiveInteger(tp.longp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_NOTATION_TAG:
                            tvp.getValue(tp.utf8sp);
                            aOp.convertNotation(tp.utf8sp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_POSITIVE_INTEGER_TAG:
                            tvp.getValue(tp.longp);
                            aOp.convertPositiveInteger(tp.longp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_QNAME_TAG:
                            tvp.getValue(tp.qnamep);
                            aOp.convertQName(tp.qnamep, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.SEQUENCE_TAG:
                            XDMConstants.setFalse(result);
                            return;

                        case ValueTag.XS_SHORT_TAG:
                            tvp.getValue(tp.shortp);
                            aOp.convertShort(tp.shortp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_STRING_TAG:
                            tvp.getValue(tp.utf8sp);
                            aOp.convertString(tp.utf8sp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_TIME_TAG:
                            tvp.getValue(tp.timep);
                            aOp.convertTime(tp.timep, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_UNSIGNED_BYTE_TAG:
                            tvp.getValue(tp.shortp);
                            aOp.convertShort(tp.shortp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_UNSIGNED_INT_TAG:
                            tvp.getValue(tp.longp);
                            aOp.convertUnsignedInt(tp.longp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_UNSIGNED_LONG_TAG:
                            tvp.getValue(tp.longp);
                            aOp.convertUnsignedLong(tp.longp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_UNSIGNED_SHORT_TAG:
                            tvp.getValue(tp.intp);
                            aOp.convertUnsignedShort(tp.intp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_UNTYPED_ATOMIC_TAG:
                            tvp.getValue(tp.utf8sp);
                            aOp.convertUntypedAtomic(tp.utf8sp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                            tvp.getValue(tp.intp);
                            aOp.convertYMDuration(tp.intp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.JS_NULL_TAG:
                            aOp.convertNull(tvp, dOut);
                            result.set(abvs);
                            return;
                    }
                } catch (SystemException se) {
                    throw se;
                } catch (Exception e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
                throw new SystemException(ErrorCode.XPTY0004);
            }

            @Override
            protected void setSequenceType(SequenceType sType) {
                if (sType.getItemType() == BuiltinTypeRegistry.XS_ANY_URI) {
                    aOp = new CastableAsAnyURIOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_BASE64_BINARY) {
                    aOp = new CastableAsBase64BinaryOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_BOOLEAN) {
                    aOp = new CastableAsBooleanOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_BYTE) {
                    aOp = new CastableAsByteOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_DATE) {
                    aOp = new CastableAsDateOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_DATETIME) {
                    aOp = new CastableAsDateTimeOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_DAY_TIME_DURATION) {
                    aOp = new CastableAsDTDurationOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_DECIMAL) {
                    aOp = new CastableAsDecimalOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_DOUBLE) {
                    aOp = new CastableAsDoubleOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_DURATION) {
                    aOp = new CastableAsDurationOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_FLOAT) {
                    aOp = new CastableAsFloatOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_G_DAY) {
                    aOp = new CastableAsGDayOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_G_MONTH) {
                    aOp = new CastableAsGMonthOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_G_MONTH_DAY) {
                    aOp = new CastableAsGMonthDayOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_G_YEAR) {
                    aOp = new CastableAsGYearOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_G_YEAR_MONTH) {
                    aOp = new CastableAsGYearMonthOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_HEX_BINARY) {
                    aOp = new CastableAsHexBinaryOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_INT) {
                    aOp = new CastableAsIntOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_INTEGER) {
                    aOp = new CastableAsIntegerOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_LONG) {
                    aOp = new CastableAsLongOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_NEGATIVE_INTEGER) {
                    aOp = new CastableAsNegativeIntegerOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_NON_NEGATIVE_INTEGER) {
                    aOp = new CastableAsNonNegativeIntegerOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_NON_POSITIVE_INTEGER) {
                    aOp = new CastableAsNonPositiveIntegerOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_NOTATION) {
                    aOp = new CastableAsNotationOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_QNAME) {
                    aOp = new CastableAsQNameOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_SHORT) {
                    aOp = new CastableAsShortOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_STRING) {
                    aOp = new CastableAsStringOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_TIME) {
                    aOp = new CastableAsTimeOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_UNSIGNED_BYTE) {
                    aOp = new CastableAsUnsignedByteOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_UNSIGNED_INT) {
                    aOp = new CastableAsUnsignedIntOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_UNSIGNED_LONG) {
                    aOp = new CastableAsUnsignedLongOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_UNSIGNED_SHORT) {
                    aOp = new CastableAsUnsignedShortOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_UNTYPED_ATOMIC) {
                    aOp = new CastableAsUntypedAtomicOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_YEAR_MONTH_DURATION) {
                    aOp = new CastableAsYMDurationOperation();
                } else {
                    aOp = new CastableAsUntypedAtomicOperation();
                }
            }

        };
    }

}
