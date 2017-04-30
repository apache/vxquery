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
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.TypedPointables;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.runtime.functions.cast.CastToStringOperation;
import org.apache.vxquery.runtime.functions.util.AtomizeHelper;

public class FnStringScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnStringScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws HyracksDataException {
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final DataOutput dOut = abvs.getDataOutput();
        final CastToStringOperation castToString = new CastToStringOperation();
        final TypedPointables tp = new TypedPointables();
        final AtomizeHelper ah = new AtomizeHelper();
        final UTF8StringPointable stringNode = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        final TaggedValuePointable tvpNode = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                TaggedValuePointable tvp1 = args[0];
                try {
                    abvs.reset();

                    switch (tvp1.getTag()) {
                        case ValueTag.XS_ANY_URI_TAG:
                            tvp1.getValue(tp.utf8sp);
                            castToString.convertAnyURI(tp.utf8sp, dOut);
                            break;
                        case ValueTag.XS_STRING_TAG:
                        case ValueTag.XS_NORMALIZED_STRING_TAG:
                        case ValueTag.XS_TOKEN_TAG:
                        case ValueTag.XS_LANGUAGE_TAG:
                        case ValueTag.XS_NMTOKEN_TAG:
                        case ValueTag.XS_NAME_TAG:
                        case ValueTag.XS_NCNAME_TAG:
                        case ValueTag.XS_ID_TAG:
                        case ValueTag.XS_IDREF_TAG:
                        case ValueTag.XS_ENTITY_TAG:
                            tvp1.getValue(tp.utf8sp);
                            castToString.convertString(tp.utf8sp, dOut);
                            break;
                        case ValueTag.XS_UNTYPED_ATOMIC_TAG:
                            tvp1.getValue(tp.utf8sp);
                            castToString.convertUntypedAtomic(tp.utf8sp, dOut);
                            break;
                        case ValueTag.XS_BASE64_BINARY_TAG:
                            tvp1.getValue(tp.binaryp);
                            castToString.convertBase64Binary(tp.binaryp, dOut);
                            break;
                        case ValueTag.XS_HEX_BINARY_TAG:
                            tvp1.getValue(tp.binaryp);
                            castToString.convertHexBinary(tp.binaryp, dOut);
                            break;
                        case ValueTag.XS_BOOLEAN_TAG:
                            tvp1.getValue(tp.boolp);
                            castToString.convertBoolean(tp.boolp, dOut);
                            break;
                        case ValueTag.XS_DATETIME_TAG:
                            tvp1.getValue(tp.datetimep);
                            castToString.convertDatetime(tp.datetimep, dOut);
                            break;
                        case ValueTag.XS_DAY_TIME_DURATION_TAG:
                            tvp1.getValue(tp.longp);
                            castToString.convertDTDuration(tp.longp, dOut);
                            break;
                        case ValueTag.XS_INTEGER_TAG:
                        case ValueTag.XS_NON_POSITIVE_INTEGER_TAG:
                        case ValueTag.XS_NEGATIVE_INTEGER_TAG:
                        case ValueTag.XS_LONG_TAG:
                        case ValueTag.XS_NON_NEGATIVE_INTEGER_TAG:
                        case ValueTag.XS_UNSIGNED_LONG_TAG:
                        case ValueTag.XS_POSITIVE_INTEGER_TAG:
                        case ValueTag.XS_UNSIGNED_INT_TAG:
                            tvp1.getValue(tp.longp);
                            castToString.convertInteger(tp.longp, dOut);
                            break;
                        case ValueTag.XS_DURATION_TAG:
                            tvp1.getValue(tp.durationp);
                            castToString.convertDuration(tp.durationp, dOut);
                            break;
                        case ValueTag.XS_DATE_TAG:
                            tvp1.getValue(tp.datep);
                            castToString.convertDate(tp.datep, dOut);
                            break;
                        case ValueTag.XS_G_DAY_TAG:
                            tvp1.getValue(tp.datep);
                            castToString.convertGDay(tp.datep, dOut);
                            break;
                        case ValueTag.XS_G_MONTH_DAY_TAG:
                            tvp1.getValue(tp.datep);
                            castToString.convertGMonthDay(tp.datep, dOut);
                            break;
                        case ValueTag.XS_G_MONTH_TAG:
                            tvp1.getValue(tp.datep);
                            castToString.convertGMonth(tp.datep, dOut);
                            break;
                        case ValueTag.XS_G_YEAR_MONTH_TAG:
                            tvp1.getValue(tp.datep);
                            castToString.convertGYearMonth(tp.datep, dOut);
                            break;
                        case ValueTag.XS_G_YEAR_TAG:
                            tvp1.getValue(tp.datep);
                            castToString.convertGYear(tp.datep, dOut);
                            break;
                        case ValueTag.XS_QNAME_TAG:
                            tvp1.getValue(tp.qnamep);
                            castToString.convertQName(tp.qnamep, dOut);
                            break;
                        case ValueTag.XS_TIME_TAG:
                            tvp1.getValue(tp.timep);
                            castToString.convertTime(tp.timep, dOut);
                            break;
                        case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                            tvp1.getValue(tp.intp);
                            castToString.convertYMDuration(tp.intp, dOut);
                            break;
                        case ValueTag.XS_INT_TAG:
                        case ValueTag.XS_UNSIGNED_SHORT_TAG:
                            tvp1.getValue(tp.intp);
                            castToString.convertInt(tp.intp, dOut);
                            break;
                        case ValueTag.XS_DECIMAL_TAG:
                            tvp1.getValue(tp.decp);
                            castToString.convertDecimal(tp.decp, dOut);
                            break;
                        case ValueTag.XS_DOUBLE_TAG:
                            tvp1.getValue(tp.doublep);
                            castToString.convertDouble(tp.doublep, dOut);
                            break;
                        case ValueTag.XS_FLOAT_TAG:
                            tvp1.getValue(tp.floatp);
                            castToString.convertFloat(tp.floatp, dOut);
                            break;
                        case ValueTag.XS_SHORT_TAG:
                        case ValueTag.XS_UNSIGNED_BYTE_TAG:
                            tvp1.getValue(tp.shortp);
                            castToString.convertShort(tp.shortp, dOut);
                            break;
                        case ValueTag.XS_BYTE_TAG:
                            tvp1.getValue(tp.bytep);
                            castToString.convertByte(tp.bytep, dOut);
                            break;
                        case ValueTag.SEQUENCE_TAG:
                            tvp1.getValue(tp.seqp);
                            if (tp.seqp.getEntryCount() == 0) {
                                XDMConstants.setEmptyString(result);
                                return;
                            }
                        case ValueTag.NODE_TREE_TAG:
                            ah.atomize(tvp1, ppool, tvpNode);
                            tvpNode.getValue(stringNode);
                            castToString.convertUntypedAtomic(stringNode, dOut);
                            break;
                        case ValueTag.JS_NULL_TAG:
                            castToString.convertNull(dOut);
                            break;
                        case ValueTag.ARRAY_TAG:
                        case ValueTag.OBJECT_TAG:
                            throw new SystemException(ErrorCode.JNTY0024);
                            // Pass through if not empty sequence.
                        default:
                            throw new SystemException(ErrorCode.XPDY0002);
                    }
                    result.set(abvs);
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
            }

        };
    }
}
