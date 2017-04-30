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
package org.apache.vxquery.runtime.functions.strings;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.data.std.util.UTF8StringBuilder;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.TypedPointables;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.runtime.functions.cast.CastToStringOperation;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;

public class FnConcatEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnConcatEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws HyracksDataException {
        final UTF8StringPointable stringp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final ArrayBackedValueStorage abvsInner = new ArrayBackedValueStorage();
        final DataOutput dOutInner = abvsInner.getDataOutput();
        final CastToStringOperation castToString = new CastToStringOperation();
        final TypedPointables tp = new TypedPointables();
        final UTF8StringBuilder builder = new UTF8StringBuilder();
        final GrowableArray ga = new GrowableArray();

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                abvs.reset();

                try {
                    // append each string to abvsBuilder
                    ga.reset();
                    builder.reset(ga, 300);

                    for (int i = 0; i < args.length; i++) {
                        TaggedValuePointable tvp = args[i];

                        // TODO Update function to support cast to a string from any atomic value.
                        if (!FunctionHelper.isDerivedFromString(tvp.getTag())) {

                            try {
                                abvsInner.reset();
                                switch (tvp.getTag()) {
                                    case ValueTag.XS_ANY_URI_TAG:
                                        tvp.getValue(tp.utf8sp);
                                        castToString.convertAnyURI(tp.utf8sp, dOutInner);
                                        break;
                                    case ValueTag.XS_UNTYPED_ATOMIC_TAG:
                                        tvp.getValue(tp.utf8sp);
                                        castToString.convertUntypedAtomic(tp.utf8sp, dOutInner);
                                        break;
                                    case ValueTag.XS_BASE64_BINARY_TAG:
                                        tvp.getValue(tp.binaryp);
                                        castToString.convertBase64Binary(tp.binaryp, dOutInner);
                                        break;
                                    case ValueTag.XS_HEX_BINARY_TAG:
                                        tvp.getValue(tp.binaryp);
                                        castToString.convertHexBinary(tp.binaryp, dOutInner);
                                        break;
                                    case ValueTag.XS_BOOLEAN_TAG:
                                        tvp.getValue(tp.boolp);
                                        castToString.convertBoolean(tp.boolp, dOutInner);
                                        break;
                                    case ValueTag.XS_DATETIME_TAG:
                                        tvp.getValue(tp.datetimep);
                                        castToString.convertDatetime(tp.datetimep, dOutInner);
                                        break;
                                    case ValueTag.XS_DAY_TIME_DURATION_TAG:
                                        tvp.getValue(tp.longp);
                                        castToString.convertDTDuration(tp.longp, dOutInner);
                                        break;
                                    case ValueTag.XS_INTEGER_TAG:
                                    case ValueTag.XS_NON_POSITIVE_INTEGER_TAG:
                                    case ValueTag.XS_NEGATIVE_INTEGER_TAG:
                                    case ValueTag.XS_LONG_TAG:
                                    case ValueTag.XS_NON_NEGATIVE_INTEGER_TAG:
                                    case ValueTag.XS_UNSIGNED_LONG_TAG:
                                    case ValueTag.XS_POSITIVE_INTEGER_TAG:
                                    case ValueTag.XS_UNSIGNED_INT_TAG:
                                        tvp.getValue(tp.longp);
                                        castToString.convertInteger(tp.longp, dOutInner);
                                        break;
                                    case ValueTag.XS_DURATION_TAG:
                                        tvp.getValue(tp.durationp);
                                        castToString.convertDuration(tp.durationp, dOutInner);
                                        break;
                                    case ValueTag.XS_DATE_TAG:
                                        tvp.getValue(tp.datep);
                                        castToString.convertDate(tp.datep, dOutInner);
                                        break;
                                    case ValueTag.XS_G_DAY_TAG:
                                        tvp.getValue(tp.datep);
                                        castToString.convertGDay(tp.datep, dOutInner);
                                        break;
                                    case ValueTag.XS_G_MONTH_DAY_TAG:
                                        tvp.getValue(tp.datep);
                                        castToString.convertGMonthDay(tp.datep, dOutInner);
                                        break;
                                    case ValueTag.XS_G_MONTH_TAG:
                                        tvp.getValue(tp.datep);
                                        castToString.convertGMonth(tp.datep, dOutInner);
                                        break;
                                    case ValueTag.XS_G_YEAR_MONTH_TAG:
                                        tvp.getValue(tp.datep);
                                        castToString.convertGYearMonth(tp.datep, dOutInner);
                                        break;
                                    case ValueTag.XS_G_YEAR_TAG:
                                        tvp.getValue(tp.datep);
                                        castToString.convertGYear(tp.datep, dOutInner);
                                        break;
                                    case ValueTag.XS_QNAME_TAG:
                                        tvp.getValue(tp.qnamep);
                                        castToString.convertQName(tp.qnamep, dOutInner);
                                        break;
                                    case ValueTag.XS_TIME_TAG:
                                        tvp.getValue(tp.timep);
                                        castToString.convertTime(tp.timep, dOutInner);
                                        break;
                                    case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                                        tvp.getValue(tp.intp);
                                        castToString.convertYMDuration(tp.intp, dOutInner);
                                        break;
                                    case ValueTag.XS_INT_TAG:
                                    case ValueTag.XS_UNSIGNED_SHORT_TAG:
                                        tvp.getValue(tp.intp);
                                        castToString.convertInt(tp.intp, dOutInner);
                                        break;
                                    case ValueTag.XS_DECIMAL_TAG:
                                        tvp.getValue(tp.decp);
                                        castToString.convertDecimal(tp.decp, dOutInner);
                                        break;
                                    case ValueTag.XS_DOUBLE_TAG:
                                        tvp.getValue(tp.doublep);
                                        castToString.convertDouble(tp.doublep, dOutInner);
                                        break;
                                    case ValueTag.XS_FLOAT_TAG:
                                        tvp.getValue(tp.floatp);
                                        castToString.convertFloat(tp.floatp, dOutInner);
                                        break;
                                    case ValueTag.XS_SHORT_TAG:
                                    case ValueTag.XS_UNSIGNED_BYTE_TAG:
                                        tvp.getValue(tp.shortp);
                                        castToString.convertShort(tp.shortp, dOutInner);
                                        break;
                                    case ValueTag.XS_BYTE_TAG:
                                        tvp.getValue(tp.bytep);
                                        castToString.convertByte(tp.bytep, dOutInner);
                                        break;
                                    case ValueTag.SEQUENCE_TAG:
                                        tvp.getValue(tp.seqp);
                                        if (tp.seqp.getEntryCount() == 0) {
                                            // Byte Format: Type (1 byte) + String Length (2 bytes) + String.
                                            dOutInner.write(ValueTag.XS_STRING_TAG);
                                            dOutInner.write(0);
                                            dOutInner.write(0);
                                            break;
                                        }
                                        // Pass through if not empty sequence.
                                    default:
                                        throw new SystemException(ErrorCode.XPTY0004);
                                }

                                // Remove tag.
                                stringp.set(abvsInner.getByteArray(), abvsInner.getStartOffset() + 1,
                                        abvsInner.getLength() - 1);
                            } catch (IOException e) {
                                throw new SystemException(ErrorCode.SYSE0001, e);
                            }
                        } else {
                            tvp.getValue(stringp);
                        }

                        // If its an empty string do nothing.
                        if (stringp.getStringLength() > 0) {
                            builder.appendUtf8StringPointable(stringp);
                        }
                    }
                    builder.finish();

                    // Add tag to string and write out.
                    DataOutput out = abvs.getDataOutput();
                    out.write(ValueTag.XS_STRING_TAG);
                    out.write(ga.getByteArray(), 0, ga.getLength());
                    result.set(abvs.getByteArray(), abvs.getStartOffset(), abvs.getLength());
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
            }
        };
    }

}
