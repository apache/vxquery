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
package org.apache.vxquery.runtime.functions.cast;

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
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.type.AbstractTypeScalarEvaluatorFactory;
import org.apache.vxquery.types.BuiltinTypeRegistry;
import org.apache.vxquery.types.SequenceType;

public class CastScalarEvaluatorFactory extends AbstractTypeScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public CastScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws HyracksDataException {
        return new AbstractTypeScalarEvaluator(args, ctx) {
            final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
            final DataOutput dOut = abvs.getDataOutput();
            final TypedPointables tp = new TypedPointables();
            AbstractCastToOperation aOp = new CastToStringOperation();

            @Override
            protected void evaluate(TaggedValuePointable tvp, IPointable result) throws SystemException {
                abvs.reset();
                int tid = tvp.getTag();
                try {
                    switch (tid) {
                        /**
                         * Primitive Datatypes (Alphabetical)
                         */
                        case ValueTag.XS_ANY_URI_TAG:
                            tvp.getValue(tp.utf8sp);
                            aOp.convertAnyURI(tp.utf8sp, dOut);
                            break;

                        case ValueTag.XS_BASE64_BINARY_TAG:
                            tvp.getValue(tp.binaryp);
                            aOp.convertBase64Binary(tp.binaryp, dOut);
                            break;

                        case ValueTag.XS_BOOLEAN_TAG:
                            tvp.getValue(tp.boolp);
                            aOp.convertBoolean(tp.boolp, dOut);
                            break;

                        case ValueTag.XS_DATE_TAG:
                            tvp.getValue(tp.datep);
                            aOp.convertDate(tp.datep, dOut);
                            break;

                        case ValueTag.XS_DATETIME_TAG:
                            tvp.getValue(tp.datetimep);
                            aOp.convertDatetime(tp.datetimep, dOut);
                            break;

                        case ValueTag.XS_DAY_TIME_DURATION_TAG:
                            tvp.getValue(tp.longp);
                            aOp.convertDTDuration(tp.longp, dOut);
                            break;

                        case ValueTag.XS_DECIMAL_TAG:
                            tvp.getValue(tp.decp);
                            aOp.convertDecimal(tp.decp, dOut);
                            break;

                        case ValueTag.XS_DOUBLE_TAG:
                            tvp.getValue(tp.doublep);
                            aOp.convertDouble(tp.doublep, dOut);
                            break;

                        case ValueTag.XS_DURATION_TAG:
                            tvp.getValue(tp.durationp);
                            aOp.convertDuration(tp.durationp, dOut);
                            break;

                        case ValueTag.XS_FLOAT_TAG:
                            tvp.getValue(tp.floatp);
                            aOp.convertFloat(tp.floatp, dOut);
                            break;

                        case ValueTag.XS_G_DAY_TAG:
                            tvp.getValue(tp.datep);
                            aOp.convertGDay(tp.datep, dOut);
                            break;

                        case ValueTag.XS_G_MONTH_TAG:
                            tvp.getValue(tp.datep);
                            aOp.convertGMonth(tp.datep, dOut);
                            break;

                        case ValueTag.XS_G_MONTH_DAY_TAG:
                            tvp.getValue(tp.datep);
                            aOp.convertGMonthDay(tp.datep, dOut);
                            break;

                        case ValueTag.XS_G_YEAR_TAG:
                            tvp.getValue(tp.datep);
                            aOp.convertGYear(tp.datep, dOut);
                            break;

                        case ValueTag.XS_G_YEAR_MONTH_TAG:
                            tvp.getValue(tp.datep);
                            aOp.convertGYearMonth(tp.datep, dOut);
                            break;

                        case ValueTag.XS_HEX_BINARY_TAG:
                            tvp.getValue(tp.binaryp);
                            aOp.convertHexBinary(tp.binaryp, dOut);
                            break;

                        case ValueTag.XS_INTEGER_TAG:
                            tvp.getValue(tp.longp);
                            aOp.convertInteger(tp.longp, dOut);
                            break;

                        case ValueTag.XS_NOTATION_TAG:
                            tvp.getValue(tp.utf8sp);
                            aOp.convertNotation(tp.utf8sp, dOut);
                            break;

                        case ValueTag.XS_QNAME_TAG:
                            tvp.getValue(tp.qnamep);
                            aOp.convertQName(tp.qnamep, dOut);
                            break;

                        case ValueTag.XS_STRING_TAG:
                            tvp.getValue(tp.utf8sp);
                            aOp.convertString(tp.utf8sp, dOut);
                            break;

                        case ValueTag.XS_TIME_TAG:
                            tvp.getValue(tp.timep);
                            aOp.convertTime(tp.timep, dOut);
                            break;

                        case ValueTag.XS_UNTYPED_ATOMIC_TAG:
                            tvp.getValue(tp.utf8sp);
                            aOp.convertUntypedAtomic(tp.utf8sp, dOut);
                            break;

                        case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                            tvp.getValue(tp.intp);
                            aOp.convertYMDuration(tp.intp, dOut);
                            break;

                        /**
                         * Derived Numeric Datatypes (Alphabetical)
                         */
                        case ValueTag.XS_BYTE_TAG:
                            tvp.getValue(tp.bytep);
                            aOp.convertByte(tp.bytep, dOut);
                            break;

                        case ValueTag.XS_INT_TAG:
                            tvp.getValue(tp.intp);
                            aOp.convertInt(tp.intp, dOut);
                            break;

                        case ValueTag.XS_LONG_TAG:
                            tvp.getValue(tp.longp);
                            aOp.convertLong(tp.longp, dOut);
                            break;

                        case ValueTag.XS_NEGATIVE_INTEGER_TAG:
                            tvp.getValue(tp.longp);
                            aOp.convertNegativeInteger(tp.longp, dOut);
                            break;

                        case ValueTag.XS_NON_NEGATIVE_INTEGER_TAG:
                            tvp.getValue(tp.longp);
                            aOp.convertNonNegativeInteger(tp.longp, dOut);
                            break;

                        case ValueTag.XS_NON_POSITIVE_INTEGER_TAG:
                            tvp.getValue(tp.longp);
                            aOp.convertNonPositiveInteger(tp.longp, dOut);
                            break;

                        case ValueTag.XS_POSITIVE_INTEGER_TAG:
                            tvp.getValue(tp.longp);
                            aOp.convertPositiveInteger(tp.longp, dOut);
                            break;

                        case ValueTag.XS_SHORT_TAG:
                            tvp.getValue(tp.shortp);
                            aOp.convertShort(tp.shortp, dOut);
                            break;

                        case ValueTag.XS_UNSIGNED_BYTE_TAG:
                            tvp.getValue(tp.shortp);
                            aOp.convertUnsignedByte(tp.shortp, dOut);
                            break;

                        case ValueTag.XS_UNSIGNED_INT_TAG:
                            tvp.getValue(tp.longp);
                            aOp.convertUnsignedInt(tp.longp, dOut);
                            break;

                        case ValueTag.XS_UNSIGNED_LONG_TAG:
                            tvp.getValue(tp.longp);
                            aOp.convertUnsignedLong(tp.longp, dOut);
                            break;

                        case ValueTag.XS_UNSIGNED_SHORT_TAG:
                            tvp.getValue(tp.intp);
                            aOp.convertUnsignedShort(tp.intp, dOut);
                            break;

                        /**
                         * Derived String Datatypes (Alphabetical)
                         */
                        case ValueTag.XS_ENTITY_TAG:
                            tvp.getValue(tp.utf8sp);
                            aOp.convertEntity(tp.utf8sp, dOut);
                            break;

                        case ValueTag.XS_ID_TAG:
                            tvp.getValue(tp.utf8sp);
                            aOp.convertID(tp.utf8sp, dOut);
                            break;

                        case ValueTag.XS_IDREF_TAG:
                            tvp.getValue(tp.utf8sp);
                            aOp.convertIDREF(tp.utf8sp, dOut);
                            break;

                        case ValueTag.XS_LANGUAGE_TAG:
                            tvp.getValue(tp.utf8sp);
                            aOp.convertIDREF(tp.utf8sp, dOut);
                            break;

                        case ValueTag.XS_NAME_TAG:
                            tvp.getValue(tp.utf8sp);
                            aOp.convertName(tp.utf8sp, dOut);
                            break;

                        case ValueTag.XS_NCNAME_TAG:
                            tvp.getValue(tp.utf8sp);
                            aOp.convertNCName(tp.utf8sp, dOut);
                            break;

                        case ValueTag.XS_NMTOKEN_TAG:
                            tvp.getValue(tp.utf8sp);
                            aOp.convertNMToken(tp.utf8sp, dOut);
                            break;

                        case ValueTag.XS_NORMALIZED_STRING_TAG:
                            tvp.getValue(tp.utf8sp);
                            aOp.convertNormalizedString(tp.utf8sp, dOut);
                            break;

                        case ValueTag.XS_TOKEN_TAG:
                            tvp.getValue(tp.utf8sp);
                            aOp.convertToken(tp.utf8sp, dOut);
                            break;

                        /**
                         * JSON null
                         */
                        case ValueTag.JS_NULL_TAG:
                            aOp.convertNull(dOut);
                            break;

                        default:
                            throw new SystemException(ErrorCode.XPTY0004);
                    }
                    result.set(abvs.getByteArray(), abvs.getStartOffset(), abvs.getLength());
                } catch (SystemException se) {
                    throw se;
                } catch (Exception e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }

            }

            @Override
            protected void setSequenceType(SequenceType sType) {
                if (sType.getItemType() == BuiltinTypeRegistry.XS_ANY_URI) {
                    aOp = new CastToAnyURIOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_BASE64_BINARY) {
                    aOp = new CastToBase64BinaryOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_BOOLEAN) {
                    aOp = new CastToBooleanOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_BYTE) {
                    aOp = new CastToByteOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_DATE) {
                    aOp = new CastToDateOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_DATETIME) {
                    aOp = new CastToDateTimeOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_DAY_TIME_DURATION) {
                    aOp = new CastToDTDurationOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_DECIMAL) {
                    aOp = new CastToDecimalOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_DOUBLE) {
                    aOp = new CastToDoubleOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_DURATION) {
                    aOp = new CastToDurationOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_ENTITY) {
                    aOp = new CastToEntityOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_FLOAT) {
                    aOp = new CastToFloatOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_G_DAY) {
                    aOp = new CastToGDayOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_G_MONTH) {
                    aOp = new CastToGMonthOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_G_MONTH_DAY) {
                    aOp = new CastToGMonthDayOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_G_YEAR) {
                    aOp = new CastToGYearOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_G_YEAR_MONTH) {
                    aOp = new CastToGYearMonthOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_HEX_BINARY) {
                    aOp = new CastToHexBinaryOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_ID) {
                    aOp = new CastToIDOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_IDREF) {
                    aOp = new CastToIDREFOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_INT) {
                    aOp = new CastToIntOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_INTEGER) {
                    aOp = new CastToIntegerOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_LANGUAGE) {
                    aOp = new CastToLanguageOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_LONG) {
                    aOp = new CastToLongOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_NAME) {
                    aOp = new CastToNameOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_NCNAME) {
                    aOp = new CastToNCNameOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_NEGATIVE_INTEGER) {
                    aOp = new CastToNegativeIntegerOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_NMTOKEN) {
                    aOp = new CastToNMTokenOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_NON_NEGATIVE_INTEGER) {
                    aOp = new CastToNonNegativeIntegerOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_NON_POSITIVE_INTEGER) {
                    aOp = new CastToNonPositiveIntegerOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_NORMALIZED_STRING) {
                    aOp = new CastToNormalizedStringOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_NOTATION) {
                    aOp = new CastToNotationOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_POSITIVE_INTEGER) {
                    aOp = new CastToPositiveIntegerOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_QNAME) {
                    aOp = new CastToQNameOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_SHORT) {
                    aOp = new CastToShortOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_STRING) {
                    aOp = new CastToStringOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_TIME) {
                    aOp = new CastToTimeOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_TOKEN) {
                    aOp = new CastToTokenOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_UNTYPED_ATOMIC) {
                    aOp = new CastToUntypedAtomicOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_UNSIGNED_BYTE) {
                    aOp = new CastToUnsignedByteOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_UNSIGNED_INT) {
                    aOp = new CastToUnsignedIntOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_UNSIGNED_LONG) {
                    aOp = new CastToUnsignedLongOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_UNSIGNED_SHORT) {
                    aOp = new CastToUnsignedShortOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_YEAR_MONTH_DURATION) {
                    aOp = new CastToYMDurationOperation();
                } else {
                    aOp = new CastToUntypedAtomicOperation();
                }
            }

        };
    }

}
