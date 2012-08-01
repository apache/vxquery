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
import org.apache.vxquery.runtime.functions.type.AbstractTypeScalarEvaluatorFactory;
import org.apache.vxquery.types.BuiltinTypeRegistry;
import org.apache.vxquery.types.SequenceType;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.BooleanPointable;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class CastScalarEvaluatorFactory extends AbstractTypeScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public CastScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        return new AbstractTypeScalarEvaluator(args, ctx) {
            final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
            final DataOutput dOut = abvs.getDataOutput();
            final TypedPointables tp = new TypedPointables();
            AbstractCastToOperation aOp = new CastToStringOperation();

            @Override
            protected void evaluate(TaggedValuePointable tvp, IPointable result) throws SystemException {
                int tid = getBaseTypeForCasts(tvp.getTag());
                if (tid == ValueTag.XS_UNTYPED_ATOMIC_TAG) {
                    // TODO Convert to double
                    tid = ValueTag.XS_DOUBLE_TAG;
                    throw new UnsupportedOperationException();
                }

                abvs.reset();
                try {
                    switch (tid) {
                        case ValueTag.XS_ANY_URI_TAG:
                            tvp.getValue(tp.utf8sp);
                            aOp.convertString(tp.utf8sp, dOut);
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
                            tvp.getValue(tp.intp);
                            aOp.convertDTDuration(tp.intp, dOut);
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

                        case ValueTag.XS_INTEGER_TAG:
                            tvp.getValue(tp.longp);
                            aOp.convertInteger(tp.longp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_NOTATION_TAG:
                            tvp.getValue(tp.utf8sp);
                            aOp.convertNotation(tp.utf8sp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_QNAME_TAG:
                            tvp.getValue(tp.qnamep);
                            aOp.convertQName(tp.qnamep, dOut);
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
                    aOp = new CastToAnyURIOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_BASE64_BINARY) {
                    aOp = new CastToBase64BinaryOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_BOOLEAN) {
                    aOp = new CastToBooleanOperation();
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
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_INTEGER) {
                    aOp = new CastToIntegerOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_HEX_BINARY) {
                    aOp = new CastToHexBinaryOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_NOTATION) {
                    aOp = new CastToNotationOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_QNAME) {
                    aOp = new CastToQNameOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_STRING) {
                    aOp = new CastToStringOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_TIME) {
                    aOp = new CastToTimeOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_UNTYPED_ATOMIC) {
                    aOp = new CastToUntypedAtomicOperation();
                } else if (sType.getItemType() == BuiltinTypeRegistry.XS_YEAR_MONTH_DURATION) {
                    aOp = new CastToYMDurationOperation();
                } else {
                    aOp = new CastToUntypedAtomicOperation();
                }
            }

            private int getBaseTypeForCasts(int tid) throws SystemException {
                while (true) {
                    switch (tid) {
                        case ValueTag.XS_STRING_TAG:
                        case ValueTag.XS_DECIMAL_TAG:
                        case ValueTag.XS_INTEGER_TAG:
                        case ValueTag.XS_FLOAT_TAG:
                        case ValueTag.XS_DOUBLE_TAG:
                        case ValueTag.XS_ANY_URI_TAG:
                        case ValueTag.XS_BOOLEAN_TAG:
                        case ValueTag.XS_DATE_TAG:
                        case ValueTag.XS_DATETIME_TAG:
                        case ValueTag.XS_TIME_TAG:
                        case ValueTag.XS_DAY_TIME_DURATION_TAG:
                        case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                        case ValueTag.XS_BASE64_BINARY_TAG:
                        case ValueTag.XS_HEX_BINARY_TAG:
                        case ValueTag.XS_QNAME_TAG:
                        case ValueTag.XS_G_DAY_TAG:
                        case ValueTag.XS_G_MONTH_DAY_TAG:
                        case ValueTag.XS_G_MONTH_TAG:
                        case ValueTag.XS_G_YEAR_MONTH_TAG:
                        case ValueTag.XS_G_YEAR_TAG:
                        case ValueTag.XS_UNTYPED_ATOMIC_TAG:
                            return tid;

                        case ValueTag.XS_ANY_ATOMIC_TAG:
                            throw new SystemException(ErrorCode.XPTY0004);

                        default:
                            tid = BuiltinTypeRegistry.INSTANCE.getSchemaTypeById(tid).getBaseType().getTypeId();
                    }
                }
            }

        };
    }

    private static class TypedPointables {
        BooleanPointable boolp = (BooleanPointable) BooleanPointable.FACTORY.createPointable();
        IntegerPointable intp = (IntegerPointable) IntegerPointable.FACTORY.createPointable();
        LongPointable longp = (LongPointable) LongPointable.FACTORY.createPointable();
        FloatPointable floatp = (FloatPointable) FloatPointable.FACTORY.createPointable();
        DoublePointable doublep = (DoublePointable) DoublePointable.FACTORY.createPointable();
        UTF8StringPointable utf8sp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        XSBinaryPointable binaryp = (XSBinaryPointable) XSBinaryPointable.FACTORY.createPointable();
        XSDecimalPointable decp = (XSDecimalPointable) XSDecimalPointable.FACTORY.createPointable();
        XSDateTimePointable datetimep = (XSDateTimePointable) XSDateTimePointable.FACTORY.createPointable();
        XSDatePointable datep = (XSDatePointable) XSDatePointable.FACTORY.createPointable();
        XSDurationPointable durationp = (XSDurationPointable) XSDurationPointable.FACTORY.createPointable();
        XSTimePointable timep = (XSTimePointable) XSTimePointable.FACTORY.createPointable();
        XSQNamePointable qnamep = (XSQNamePointable) XSQNamePointable.FACTORY.createPointable();
    }

}