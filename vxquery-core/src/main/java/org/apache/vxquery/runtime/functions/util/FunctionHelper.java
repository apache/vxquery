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
package org.apache.vxquery.runtime.functions.util;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.htrace.fasterxml.jackson.core.JsonParseException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.TypedPointables;
import org.apache.vxquery.datamodel.api.IDate;
import org.apache.vxquery.datamodel.api.ITime;
import org.apache.vxquery.datamodel.api.ITimezone;
import org.apache.vxquery.datamodel.util.DateTime;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.hdfs2.HDFSFunctions;
import org.apache.vxquery.runtime.functions.arithmetic.AbstractArithmeticOperation;
import org.apache.vxquery.runtime.functions.cast.CastToDoubleOperation;
import org.apache.vxquery.runtime.functions.comparison.AbstractValueComparisonOperation;
import org.apache.vxquery.runtime.functions.strings.ICharacterIterator;
import org.apache.vxquery.runtime.functions.strings.UTF8StringCharacterIterator;
import org.apache.vxquery.types.BuiltinTypeConstants;
import org.apache.vxquery.types.BuiltinTypeRegistry;
import org.apache.vxquery.xmlparser.IParser;

public class FunctionHelper {

    private FunctionHelper() {
    }

    public static void arithmeticOperation(AbstractArithmeticOperation aOp, DynamicContext dCtx,
            TaggedValuePointable tvp1, TaggedValuePointable tvp2, IPointable result, TypedPointables tp1,
            TypedPointables tp2) throws SystemException {
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final DataOutput dOut = abvs.getDataOutput();
        final ArrayBackedValueStorage abvsArgument1 = new ArrayBackedValueStorage();
        final DataOutput dOutArgument1 = abvsArgument1.getDataOutput();
        final ArrayBackedValueStorage abvsArgument2 = new ArrayBackedValueStorage();
        final DataOutput dOutArgument2 = abvsArgument2.getDataOutput();
        final CastToDoubleOperation castToDouble = new CastToDoubleOperation();

        abvs.reset();
        try {
            int tid1 = getBaseTypeForArithmetics(tvp1.getTag());
            int tid2 = getBaseTypeForArithmetics(tvp2.getTag());
            LongPointable longp1 = (LongPointable) LongPointable.FACTORY.createPointable();
            DoublePointable doublep1 = (DoublePointable) DoublePointable.FACTORY.createPointable();
            switch (tvp1.getTag()) {
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
                    abvsArgument1.reset();
                    getIntegerPointable(tvp1, dOutArgument1, tp1);
                    longp1.set(abvsArgument1.getByteArray(), abvsArgument1.getStartOffset() + 1,
                            LongPointable.TYPE_TRAITS.getFixedLength());
                    break;
                case ValueTag.XS_DOUBLE_TAG:
                    tvp1.getValue(doublep1);
                    break;
                case ValueTag.XS_UNTYPED_ATOMIC_TAG:
                    tid1 = ValueTag.XS_DOUBLE_TAG;
                    tvp1.getValue(tp1.utf8sp);
                    abvsArgument1.reset();
                    castToDouble.convertUntypedAtomic(tp1.utf8sp, dOutArgument1);
                    doublep1.set(abvsArgument1.getByteArray(), abvsArgument1.getStartOffset() + 1,
                            DoublePointable.TYPE_TRAITS.getFixedLength());
                    break;
            }
            LongPointable longp2 = (LongPointable) LongPointable.FACTORY.createPointable();
            DoublePointable doublep2 = (DoublePointable) DoublePointable.FACTORY.createPointable();
            switch (tvp2.getTag()) {
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
                    abvsArgument2.reset();
                    getIntegerPointable(tvp2, dOutArgument2, tp2);
                    longp2.set(abvsArgument2.getByteArray(), abvsArgument2.getStartOffset() + 1,
                            LongPointable.TYPE_TRAITS.getFixedLength());
                    break;
                case ValueTag.XS_DOUBLE_TAG:
                    tvp2.getValue(doublep2);
                    break;
                case ValueTag.XS_UNTYPED_ATOMIC_TAG:
                    tid2 = ValueTag.XS_DOUBLE_TAG;
                    tvp2.getValue(tp2.utf8sp);
                    abvsArgument2.reset();
                    castToDouble.convertUntypedAtomic(tp2.utf8sp, dOutArgument2);
                    doublep2.set(abvsArgument2.getByteArray(), abvsArgument2.getStartOffset() + 1,
                            DoublePointable.TYPE_TRAITS.getFixedLength());
                    break;
            }
            switch (tid1) {
                case ValueTag.XS_DECIMAL_TAG:
                    tvp1.getValue(tp1.decp);
                    switch (tid2) {
                        case ValueTag.XS_DECIMAL_TAG:
                            tvp2.getValue(tp2.decp);
                            aOp.operateDecimalDecimal(tp1.decp, tp2.decp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_INTEGER_TAG:
                            aOp.operateDecimalInteger(tp1.decp, longp2, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_FLOAT_TAG:
                            tvp2.getValue(tp2.floatp);
                            aOp.operateDecimalFloat(tp1.decp, tp2.floatp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_DOUBLE_TAG:
                            aOp.operateDecimalDouble(tp1.decp, doublep2, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_DAY_TIME_DURATION_TAG:
                            tvp2.getValue(tp2.longp);
                            aOp.operateDecimalDTDuration(tp1.decp, tp2.longp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                            tvp2.getValue(tp2.intp);
                            aOp.operateDecimalYMDuration(tp1.decp, tp2.intp, dOut);
                            result.set(abvs);
                            return;
                    }
                    break;

                case ValueTag.XS_INTEGER_TAG:
                    switch (tid2) {
                        case ValueTag.XS_DECIMAL_TAG:
                            tvp2.getValue(tp2.decp);
                            aOp.operateIntegerDecimal(longp1, tp2.decp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_INTEGER_TAG:
                            aOp.operateIntegerInteger(longp1, longp2, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_FLOAT_TAG:
                            tvp2.getValue(tp2.floatp);
                            aOp.operateIntegerFloat(longp1, tp2.floatp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_DOUBLE_TAG:
                            aOp.operateIntegerDouble(longp1, doublep2, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_DAY_TIME_DURATION_TAG:
                            tvp2.getValue(tp2.longp);
                            aOp.operateIntegerDTDuration(longp1, tp2.longp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                            tvp2.getValue(tp2.intp);
                            aOp.operateIntegerYMDuration(longp1, tp2.intp, dOut);
                            result.set(abvs);
                            return;
                    }
                    break;

                case ValueTag.XS_FLOAT_TAG:
                    tvp1.getValue(tp1.floatp);
                    switch (tid2) {
                        case ValueTag.XS_DECIMAL_TAG:
                            tvp2.getValue(tp2.decp);
                            aOp.operateFloatDecimal(tp1.floatp, tp2.decp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_INTEGER_TAG:
                            aOp.operateFloatInteger(tp1.floatp, longp2, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_FLOAT_TAG:
                            tvp2.getValue(tp2.floatp);
                            aOp.operateFloatFloat(tp1.floatp, tp2.floatp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_DOUBLE_TAG:
                            aOp.operateFloatDouble(tp1.floatp, doublep2, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_DAY_TIME_DURATION_TAG:
                            tvp2.getValue(tp2.longp);
                            aOp.operateFloatDTDuration(tp1.floatp, tp2.longp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                            tvp2.getValue(tp2.intp);
                            aOp.operateFloatYMDuration(tp1.floatp, tp2.intp, dOut);
                            result.set(abvs);
                            return;
                    }
                    break;

                case ValueTag.XS_DOUBLE_TAG:
                    switch (tid2) {
                        case ValueTag.XS_DECIMAL_TAG:
                            tvp2.getValue(tp2.decp);
                            aOp.operateDoubleDecimal(doublep1, tp2.decp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_INTEGER_TAG:
                            aOp.operateDoubleInteger(doublep1, longp2, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_FLOAT_TAG:
                            tvp2.getValue(tp2.floatp);
                            aOp.operateDoubleFloat(doublep1, tp2.floatp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_DOUBLE_TAG:
                            aOp.operateDoubleDouble(doublep1, doublep2, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_DAY_TIME_DURATION_TAG:
                            tvp2.getValue(tp2.longp);
                            aOp.operateDoubleDTDuration(doublep1, tp2.longp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                            tvp2.getValue(tp2.intp);
                            aOp.operateDoubleYMDuration(doublep1, tp2.intp, dOut);
                            result.set(abvs);
                            return;
                    }
                    break;

                case ValueTag.XS_DATE_TAG:
                    tvp1.getValue(tp1.datep);
                    switch (tid2) {
                        case ValueTag.XS_DATE_TAG:
                            tvp2.getValue(tp2.datep);
                            aOp.operateDateDate(tp1.datep, tp2.datep, dCtx, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_DAY_TIME_DURATION_TAG:
                            tvp2.getValue(tp2.longp);
                            aOp.operateDateDTDuration(tp1.datep, tp2.longp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                            tvp2.getValue(tp2.intp);
                            aOp.operateDateYMDuration(tp1.datep, tp2.intp, dOut);
                            result.set(abvs);
                            return;
                    }
                    break;

                case ValueTag.XS_DATETIME_TAG:
                    tvp1.getValue(tp1.datetimep);
                    switch (tid2) {
                        case ValueTag.XS_DATETIME_TAG:
                            tvp2.getValue(tp2.datetimep);
                            aOp.operateDatetimeDatetime(tp1.datetimep, tp2.datetimep, dCtx, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_DAY_TIME_DURATION_TAG:
                            tvp2.getValue(tp2.longp);
                            aOp.operateDatetimeDTDuration(tp1.datetimep, tp2.longp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                            tvp2.getValue(tp2.intp);
                            aOp.operateDatetimeYMDuration(tp1.datetimep, tp2.intp, dOut);
                            result.set(abvs);
                            return;
                    }
                    break;

                case ValueTag.XS_TIME_TAG:
                    tvp1.getValue(tp1.timep);
                    switch (tid2) {
                        case ValueTag.XS_TIME_TAG:
                            tvp2.getValue(tp2.timep);
                            aOp.operateTimeTime(tp1.timep, tp2.timep, dCtx, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_DAY_TIME_DURATION_TAG:
                            tvp2.getValue(tp2.longp);
                            aOp.operateTimeDTDuration(tp1.timep, tp2.longp, dOut);
                            result.set(abvs);
                            return;

                    }
                    break;

                case ValueTag.XS_DAY_TIME_DURATION_TAG:
                    tvp1.getValue(tp1.longp);
                    switch (tid2) {
                        case ValueTag.XS_DECIMAL_TAG:
                            tvp2.getValue(tp2.decp);
                            aOp.operateDTDurationDecimal(tp1.longp, tp2.decp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_INTEGER_TAG:
                            aOp.operateDTDurationInteger(tp1.longp, longp2, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_FLOAT_TAG:
                            tvp2.getValue(tp2.floatp);
                            aOp.operateDTDurationFloat(tp1.longp, tp2.floatp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_DOUBLE_TAG:
                            aOp.operateDTDurationDouble(tp1.longp, doublep2, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_DATE_TAG:
                            tvp2.getValue(tp2.datep);
                            aOp.operateDTDurationDate(tp1.longp, tp2.datep, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_TIME_TAG:
                            tvp2.getValue(tp2.timep);
                            aOp.operateDTDurationTime(tp1.longp, tp2.timep, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_DATETIME_TAG:
                            tvp2.getValue(tp2.datetimep);
                            aOp.operateDTDurationDatetime(tp1.longp, tp2.datetimep, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_DAY_TIME_DURATION_TAG:
                            tvp2.getValue(tp2.longp);
                            aOp.operateDTDurationDTDuration(tp1.longp, tp2.longp, dOut);
                            result.set(abvs);
                            return;
                    }
                    break;

                case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                    tvp1.getValue(tp1.intp);
                    switch (tid2) {
                        case ValueTag.XS_DECIMAL_TAG:
                            tvp2.getValue(tp2.decp);
                            aOp.operateYMDurationDecimal(tp1.intp, tp2.decp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_INTEGER_TAG:
                            aOp.operateYMDurationInteger(tp1.intp, longp2, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_FLOAT_TAG:
                            tvp2.getValue(tp2.floatp);
                            aOp.operateYMDurationFloat(tp1.intp, tp2.floatp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_DOUBLE_TAG:
                            aOp.operateYMDurationDouble(tp1.intp, doublep2, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_DATE_TAG:
                            tvp2.getValue(tp2.datep);
                            aOp.operateYMDurationDate(tp1.intp, tp2.datep, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_DATETIME_TAG:
                            tvp2.getValue(tp2.datetimep);
                            aOp.operateYMDurationDatetime(tp1.intp, tp2.datetimep, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                            tvp2.getValue(tp2.intp);
                            aOp.operateYMDurationYMDuration(tp1.intp, tp2.intp, dOut);
                            result.set(abvs);
                            return;
                    }
                    break;
            }
        } catch (SystemException se) {
            throw se;
        } catch (Exception e) {
            throw new SystemException(ErrorCode.SYSE0001, e);
        }
        throw new SystemException(ErrorCode.XPTY0004);
    }

    public static boolean arraysEqual(IPointable p1, IPointable p2) {
        return arraysEqual(p1.getByteArray(), p1.getStartOffset(), p1.getLength(), p2.getByteArray(),
                p2.getStartOffset(), p2.getLength());
    }

    public static boolean arraysEqual(byte[] bytes1, int offset1, int length1, byte[] bytes2, int offset2,
            int length2) {
        if (length1 != length2) {
            return false;
        }
        for (int i = 0; i < length1; ++i) {
            if (bytes1[offset1 + i] != bytes2[offset2 + i]) {
                return false;
            }
        }
        return true;
    }

    public static boolean compareTaggedValues(AbstractValueComparisonOperation aOp, TaggedValuePointable tvp1,
            TaggedValuePointable tvp2, DynamicContext dCtx, TypedPointables tp1, TypedPointables tp2)
                    throws SystemException {
        int tid1 = getBaseTypeForComparisons(tvp1.getTag());
        int tid2 = getBaseTypeForComparisons(tvp2.getTag());

        try {
            switch (tid1) {
                case ValueTag.XS_DECIMAL_TAG:
                    tvp1.getValue(tp1.decp);
                    switch (tid2) {
                        case ValueTag.XS_DECIMAL_TAG:
                            tvp2.getValue(tp2.decp);
                            return aOp.operateDecimalDecimal(tp1.decp, tp2.decp);

                        case ValueTag.XS_INTEGER_TAG:
                            tvp2.getValue(tp2.longp);
                            return aOp.operateDecimalInteger(tp1.decp, tp2.longp);

                        case ValueTag.XS_FLOAT_TAG:
                            tvp2.getValue(tp2.floatp);
                            return aOp.operateDecimalFloat(tp1.decp, tp2.floatp);

                        case ValueTag.XS_DOUBLE_TAG:
                            tvp2.getValue(tp2.doublep);
                            return aOp.operateDecimalDouble(tp1.decp, tp2.doublep);
                    }
                    break;

                case ValueTag.XS_INTEGER_TAG:
                    tvp1.getValue(tp1.longp);
                    switch (tid2) {
                        case ValueTag.XS_DECIMAL_TAG:
                            tvp2.getValue(tp2.decp);
                            return aOp.operateIntegerDecimal(tp1.longp, tp2.decp);

                        case ValueTag.XS_INTEGER_TAG:
                            tvp2.getValue(tp2.longp);
                            return aOp.operateIntegerInteger(tp1.longp, tp2.longp);

                        case ValueTag.XS_FLOAT_TAG:
                            tvp2.getValue(tp2.floatp);
                            return aOp.operateIntegerFloat(tp1.longp, tp2.floatp);

                        case ValueTag.XS_DOUBLE_TAG:
                            tvp2.getValue(tp2.doublep);
                            return aOp.operateIntegerDouble(tp1.longp, tp2.doublep);
                    }
                    break;

                case ValueTag.XS_FLOAT_TAG:
                    tvp1.getValue(tp1.floatp);
                    switch (tid2) {
                        case ValueTag.XS_DECIMAL_TAG:
                            tvp2.getValue(tp2.decp);
                            return aOp.operateFloatDecimal(tp1.floatp, tp2.decp);

                        case ValueTag.XS_INTEGER_TAG:
                            tvp2.getValue(tp2.longp);
                            return aOp.operateFloatInteger(tp1.floatp, tp2.longp);

                        case ValueTag.XS_FLOAT_TAG:
                            tvp2.getValue(tp2.floatp);
                            return aOp.operateFloatFloat(tp1.floatp, tp2.floatp);

                        case ValueTag.XS_DOUBLE_TAG:
                            tvp2.getValue(tp2.doublep);
                            return aOp.operateFloatDouble(tp1.floatp, tp2.doublep);
                    }
                    break;

                case ValueTag.XS_DOUBLE_TAG:
                    tvp1.getValue(tp1.doublep);
                    switch (tid2) {
                        case ValueTag.XS_DECIMAL_TAG:
                            tvp2.getValue(tp2.decp);
                            return aOp.operateDoubleDecimal(tp1.doublep, tp2.decp);

                        case ValueTag.XS_INTEGER_TAG:
                            tvp2.getValue(tp2.longp);
                            return aOp.operateDoubleInteger(tp1.doublep, tp2.longp);

                        case ValueTag.XS_FLOAT_TAG:
                            tvp2.getValue(tp2.floatp);
                            return aOp.operateDoubleFloat(tp1.doublep, tp2.floatp);

                        case ValueTag.XS_DOUBLE_TAG:
                            tvp2.getValue(tp2.doublep);
                            return aOp.operateDoubleDouble(tp1.doublep, tp2.doublep);
                    }
                    break;

                case ValueTag.XS_BOOLEAN_TAG:
                    tvp1.getValue(tp1.boolp);
                    switch (tid2) {
                        case ValueTag.XS_BOOLEAN_TAG:
                            tvp2.getValue(tp2.boolp);
                            return aOp.operateBooleanBoolean(tp1.boolp, tp2.boolp);
                    }
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
                case ValueTag.XS_UNTYPED_ATOMIC_TAG:
                    tvp1.getValue(tp1.utf8sp);
                    switch (tid2) {
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
                        case ValueTag.XS_UNTYPED_ATOMIC_TAG:
                        case ValueTag.XS_ANY_URI_TAG:
                            tvp2.getValue(tp2.utf8sp);
                            return aOp.operateStringString(tp1.utf8sp, tp2.utf8sp);
                    }
                    break;

                case ValueTag.XS_DATE_TAG:
                    tvp1.getValue(tp1.datep);
                    switch (tid2) {
                        case ValueTag.XS_DATE_TAG:
                            tvp2.getValue(tp2.datep);
                            return aOp.operateDateDate(tp1.datep, tp2.datep, dCtx);
                    }
                    break;

                case ValueTag.XS_DATETIME_TAG:
                    tvp1.getValue(tp1.datetimep);
                    switch (tid2) {
                        case ValueTag.XS_DATETIME_TAG:
                            tvp2.getValue(tp2.datetimep);
                            return aOp.operateDatetimeDatetime(tp1.datetimep, tp2.datetimep, dCtx);
                    }
                    break;

                case ValueTag.XS_TIME_TAG:
                    tvp1.getValue(tp1.timep);
                    switch (tid2) {
                        case ValueTag.XS_TIME_TAG:
                            tvp2.getValue(tp2.timep);
                            return aOp.operateTimeTime(tp1.timep, tp2.timep, dCtx);
                    }
                    break;

                case ValueTag.XS_DURATION_TAG:
                    tvp1.getValue(tp1.durationp);
                    switch (tid2) {
                        case ValueTag.XS_DAY_TIME_DURATION_TAG:
                            tvp2.getValue(tp2.longp);
                            return aOp.operateDurationDTDuration(tp1.durationp, tp2.longp);

                        case ValueTag.XS_DURATION_TAG:
                            tvp2.getValue(tp2.durationp);
                            return aOp.operateDurationDuration(tp1.durationp, tp2.durationp);

                        case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                            tvp2.getValue(tp2.intp);
                            return aOp.operateDurationYMDuration(tp1.durationp, tp2.intp);
                    }
                    break;

                case ValueTag.XS_DAY_TIME_DURATION_TAG:
                    tvp1.getValue(tp1.longp);
                    switch (tid2) {
                        case ValueTag.XS_DAY_TIME_DURATION_TAG:
                            tvp2.getValue(tp2.longp);
                            return aOp.operateDTDurationDTDuration(tp1.longp, tp2.longp);

                        case ValueTag.XS_DURATION_TAG:
                            tvp2.getValue(tp2.durationp);
                            return aOp.operateDTDurationDuration(tp1.longp, tp2.durationp);

                        case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                            tvp2.getValue(tp2.intp);
                            return aOp.operateDTDurationYMDuration(tp1.longp, tp2.intp);
                    }
                    break;

                case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                    tvp1.getValue(tp1.intp);
                    switch (tid2) {
                        case ValueTag.XS_DAY_TIME_DURATION_TAG:
                            tvp2.getValue(tp2.longp);
                            return aOp.operateYMDurationDTDuration(tp1.intp, tp2.longp);

                        case ValueTag.XS_DURATION_TAG:
                            tvp2.getValue(tp2.durationp);
                            return aOp.operateYMDurationDuration(tp1.intp, tp2.durationp);

                        case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                            tvp2.getValue(tp2.intp);
                            return aOp.operateYMDurationYMDuration(tp1.intp, tp2.intp);
                    }
                    break;

                case ValueTag.XS_G_DAY_TAG:
                    tvp1.getValue(tp1.datep);
                    switch (tid2) {
                        case ValueTag.XS_G_DAY_TAG:
                            tvp2.getValue(tp2.datep);
                            return aOp.operateGDayGDay(tp1.datep, tp2.datep, dCtx);
                    }
                    break;

                case ValueTag.XS_G_MONTH_DAY_TAG:
                    tvp1.getValue(tp1.datep);
                    switch (tid2) {
                        case ValueTag.XS_G_MONTH_DAY_TAG:
                            tvp2.getValue(tp2.datep);
                            return aOp.operateGMonthDayGMonthDay(tp1.datep, tp2.datep, dCtx);
                    }
                    break;

                case ValueTag.XS_G_MONTH_TAG:
                    tvp1.getValue(tp1.datep);
                    switch (tid2) {
                        case ValueTag.XS_G_MONTH_TAG:
                            tvp2.getValue(tp2.datep);
                            return aOp.operateGMonthGMonth(tp1.datep, tp2.datep, dCtx);
                    }
                    break;

                case ValueTag.XS_G_YEAR_MONTH_TAG:
                    tvp1.getValue(tp1.datep);
                    switch (tid2) {
                        case ValueTag.XS_G_YEAR_MONTH_TAG:
                            tvp2.getValue(tp2.datep);
                            return aOp.operateGYearMonthGYearMonth(tp1.datep, tp2.datep, dCtx);
                    }
                    break;

                case ValueTag.XS_G_YEAR_TAG:
                    tvp1.getValue(tp1.datep);
                    switch (tid2) {
                        case ValueTag.XS_G_YEAR_TAG:
                            tvp2.getValue(tp2.datep);
                            return aOp.operateGYearGYear(tp1.datep, tp2.datep, dCtx);
                    }
                    break;

                case ValueTag.XS_HEX_BINARY_TAG:
                    tvp1.getValue(tp1.binaryp);
                    switch (tid2) {
                        case ValueTag.XS_HEX_BINARY_TAG:
                            tvp2.getValue(tp2.binaryp);
                            return aOp.operateHexBinaryHexBinary(tp1.binaryp, tp2.binaryp);
                    }
                    break;

                case ValueTag.XS_BASE64_BINARY_TAG:
                    tvp1.getValue(tp1.binaryp);
                    switch (tid2) {
                        case ValueTag.XS_BASE64_BINARY_TAG:
                            tvp2.getValue(tp2.binaryp);
                            return aOp.operateBase64BinaryBase64Binary(tp1.binaryp, tp2.binaryp);
                    }
                    break;

                case ValueTag.XS_ANY_URI_TAG:
                    tvp1.getValue(tp1.utf8sp);
                    switch (tid2) {
                        case ValueTag.XS_ANY_URI_TAG:
                            tvp2.getValue(tp2.utf8sp);
                            return aOp.operateAnyURIAnyURI(tp1.utf8sp, tp2.utf8sp);

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
                        case ValueTag.XS_UNTYPED_ATOMIC_TAG:
                            tvp2.getValue(tp2.utf8sp);
                            return aOp.operateStringString(tp1.utf8sp, tp2.utf8sp);
                    }
                    break;

                case ValueTag.XS_QNAME_TAG:
                    tvp1.getValue(tp1.qnamep);
                    switch (tid2) {
                        case ValueTag.XS_QNAME_TAG:
                            tvp2.getValue(tp2.qnamep);
                            return aOp.operateQNameQName(tp1.qnamep, tp2.qnamep);
                    }
                    break;

                case ValueTag.XS_NOTATION_TAG:
                    tvp1.getValue(tp1.utf8sp);
                    switch (tid2) {
                        case ValueTag.XS_NOTATION_TAG:
                            tvp2.getValue(tp2.utf8sp);
                            return aOp.operateNotationNotation(tp1.utf8sp, tp2.utf8sp);
                    }
                    break;
            }
        } catch (SystemException se) {
            throw se;
        } catch (Exception e) {
            throw new SystemException(ErrorCode.SYSE0001, e);
        }
        throw new SystemException(ErrorCode.XPTY0004);
    }

    public static int getBaseTypeForArithmetics(int tid) throws SystemException {
        if (tid >= BuiltinTypeConstants.BUILTIN_TYPE_COUNT) {
            throw new SystemException(ErrorCode.XPTY0004);
        }
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

                case ValueTag.XS_LONG_TAG:
                case ValueTag.XS_NEGATIVE_INTEGER_TAG:
                case ValueTag.XS_NON_POSITIVE_INTEGER_TAG:
                case ValueTag.XS_NON_NEGATIVE_INTEGER_TAG:
                case ValueTag.XS_POSITIVE_INTEGER_TAG:
                case ValueTag.XS_UNSIGNED_INT_TAG:
                case ValueTag.XS_UNSIGNED_LONG_TAG:
                case ValueTag.XS_INT_TAG:
                case ValueTag.XS_UNSIGNED_SHORT_TAG:
                case ValueTag.XS_SHORT_TAG:
                case ValueTag.XS_UNSIGNED_BYTE_TAG:
                case ValueTag.XS_BYTE_TAG:
                    return ValueTag.XS_INTEGER_TAG;

                case ValueTag.XS_ANY_ATOMIC_TAG:
                    throw new SystemException(ErrorCode.XPTY0004);

                default:
                    tid = BuiltinTypeRegistry.INSTANCE.getSchemaTypeById(tid).getBaseType().getTypeId();
                    return tid;
            }
        }
    }

    public static int getBaseTypeForComparisons(int tidArg) throws SystemException {
        int tid = tidArg;
        while (true) {
            switch (tid) {
                case ValueTag.XS_ANY_URI_TAG:
                case ValueTag.XS_BASE64_BINARY_TAG:
                case ValueTag.XS_BOOLEAN_TAG:
                case ValueTag.XS_DATE_TAG:
                case ValueTag.XS_DATETIME_TAG:
                case ValueTag.XS_DAY_TIME_DURATION_TAG:
                case ValueTag.XS_DECIMAL_TAG:
                case ValueTag.XS_DOUBLE_TAG:
                case ValueTag.XS_DURATION_TAG:
                case ValueTag.XS_FLOAT_TAG:
                case ValueTag.XS_HEX_BINARY_TAG:
                case ValueTag.XS_INTEGER_TAG:
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

                case ValueTag.XS_LONG_TAG:
                case ValueTag.XS_NEGATIVE_INTEGER_TAG:
                case ValueTag.XS_NON_POSITIVE_INTEGER_TAG:
                case ValueTag.XS_NON_NEGATIVE_INTEGER_TAG:
                case ValueTag.XS_POSITIVE_INTEGER_TAG:
                case ValueTag.XS_UNSIGNED_INT_TAG:
                case ValueTag.XS_UNSIGNED_LONG_TAG:
                case ValueTag.XS_INT_TAG:
                case ValueTag.XS_UNSIGNED_SHORT_TAG:
                case ValueTag.XS_SHORT_TAG:
                case ValueTag.XS_UNSIGNED_BYTE_TAG:
                case ValueTag.XS_BYTE_TAG:
                    return ValueTag.XS_INTEGER_TAG;

                case ValueTag.XS_ANY_ATOMIC_TAG:
                case ValueTag.SEQUENCE_TAG:
                    throw new SystemException(ErrorCode.XPTY0004);

                default:
                    tid = BuiltinTypeRegistry.INSTANCE.getSchemaTypeById(tid).getBaseType().getTypeId();
            }
        }
    }

    public static int getBaseTypeForGeneralComparisons(int tidArg) throws SystemException {
        int tid = tidArg;
        while (true) {
            switch (tid) {
                case ValueTag.NODE_TREE_TAG:
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

    public static void getDoublePointable(TaggedValuePointable tvp, DataOutput dOut, TypedPointables tp)
            throws SystemException, IOException {
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

    public static void getIntegerPointable(TaggedValuePointable tvp, DataOutput dOut, TypedPointables tp)
            throws SystemException, IOException {
        long value;
        switch (tvp.getTag()) {
            case ValueTag.XS_INTEGER_TAG:
            case ValueTag.XS_LONG_TAG:
            case ValueTag.XS_NEGATIVE_INTEGER_TAG:
            case ValueTag.XS_NON_POSITIVE_INTEGER_TAG:
            case ValueTag.XS_NON_NEGATIVE_INTEGER_TAG:
            case ValueTag.XS_POSITIVE_INTEGER_TAG:
            case ValueTag.XS_UNSIGNED_INT_TAG:
            case ValueTag.XS_UNSIGNED_LONG_TAG:
                tvp.getValue(tp.longp);
                value = tp.longp.longValue();
                break;

            case ValueTag.XS_INT_TAG:
            case ValueTag.XS_UNSIGNED_SHORT_TAG:
                tvp.getValue(tp.intp);
                value = tp.intp.longValue();
                break;

            case ValueTag.XS_SHORT_TAG:
            case ValueTag.XS_UNSIGNED_BYTE_TAG:
                tvp.getValue(tp.shortp);
                value = tp.shortp.longValue();
                break;

            case ValueTag.XS_BYTE_TAG:
                tvp.getValue(tp.bytep);
                value = tp.bytep.longValue();
                break;

            default:
                value = 0;
        }
        dOut.write(ValueTag.XS_INTEGER_TAG);
        dOut.writeLong(value);
    }

    /**
     * Get the local node id from a tagged value pointable when available.
     *
     * @param tvp1
     *            pointable
     * @param tp
     *            Typed pointable
     * @return local node id
     */
    public static int getLocalNodeId(TaggedValuePointable tvp1, TypedPointables tp) {
        final TaggedValuePointable tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        int localNodeId = -1;
        if (tvp1.getTag() == ValueTag.NODE_TREE_TAG) {
            tvp1.getValue(tp.ntp);
            tp.ntp.getRootNode(tvp);
            switch (tvp.getTag()) {
                case ValueTag.ATTRIBUTE_NODE_TAG:
                    tvp.getValue(tp.anp);
                    localNodeId = tp.anp.getLocalNodeId(tp.ntp);
                    break;
                case ValueTag.COMMENT_NODE_TAG:
                case ValueTag.TEXT_NODE_TAG:
                    tvp.getValue(tp.tocnp);
                    localNodeId = tp.tocnp.getLocalNodeId(tp.ntp);
                    break;
                case ValueTag.DOCUMENT_NODE_TAG:
                    tvp.getValue(tp.dnp);
                    localNodeId = tp.dnp.getLocalNodeId(tp.ntp);
                    break;
                case ValueTag.ELEMENT_NODE_TAG:
                    tvp.getValue(tp.enp);
                    localNodeId = tp.enp.getLocalNodeId(tp.ntp);
                    break;
                case ValueTag.PI_NODE_TAG:
                    tvp.getValue(tp.pinp);
                    localNodeId = tp.pinp.getLocalNodeId(tp.ntp);
                    break;
                default:
                    localNodeId = -1;
                    break;
            }
        }
        return localNodeId;
    }

    /**
     * Returns the number of digits in a long. A few special cases that needed attention.
     *
     * @param value
     *            value
     * @return Number of digits
     */
    public static int getNumberOfDigits(long value) {
        if (value == 0) {
            return 0;
        }
        double nDigitsRaw = Math.log10(value);
        int nDigits = (int) nDigitsRaw;
        if (nDigits > 11 && nDigitsRaw == nDigits) {
            // Return exact number of digits and does not need adjustment. (Ex 999999999999999999)
            return nDigits;
        } else {
            // Decimal value returned so we must increment to the next number.
            return nDigits + 1;
        }
    }

    public static long getPowerOf10(double valueArg, long max, long min) {
        double value = Math.abs(valueArg);
        for (long i = min; i < max; i++) {
            if (Math.pow(10, i) > value)
                return i;
        }
        return max;
    }

    public static String getStringFromPointable(UTF8StringPointable stringp, ByteBufferInputStream bbis,
            DataInputStream di) throws IOException {
        bbis.setByteBuffer(ByteBuffer.wrap(Arrays.copyOfRange(stringp.getByteArray(), stringp.getStartOffset(),
                stringp.getLength() + stringp.getStartOffset())), 0);
        return di.readUTF();
    }

    public static long getTimezone(ITimezone timezonep) {
        return timezonep.getTimezoneHour() * DateTime.CHRONON_OF_HOUR
                + timezonep.getTimezoneMinute() * DateTime.CHRONON_OF_MINUTE;
    }

    public static boolean isDerivedFromDouble(int tid) {
        switch (tid) {
            case ValueTag.XS_DECIMAL_TAG:
            case ValueTag.XS_DOUBLE_TAG:
            case ValueTag.XS_FLOAT_TAG:
                return true;
            default:
                return isDerivedFromInteger(tid);
        }
    }

    public static boolean isDerivedFromInteger(int tid) {
        switch (tid) {
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
                return true;
            default:
                return false;
        }
    }

    public static boolean isDerivedFromString(int tid) {
        switch (tid) {
            case ValueTag.XS_UNTYPED_ATOMIC_TAG:
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
                return true;
            default:
                return false;
        }
    }

    /**
     * Returns 0 if positive, nonzero if negative.
     *
     * @param value
     *            value
     * @return boolean
     */
    public static boolean isNumberPostive(long value) {
        return (value & 0x8000000000000000L) == 0 ? true : false;
    }

    public static void printUTF8String(UTF8StringPointable stringp) {
        System.err.println(" printUTF8String START length = " + stringp.getUTFLength());
        ICharacterIterator charIterator = new UTF8StringCharacterIterator(stringp);
        charIterator.reset();
        for (int c = charIterator.next(); c != ICharacterIterator.EOS_CHAR; c = charIterator.next()) {
            System.err.println("   parse value '" + c + "' as '" + Character.valueOf((char) c) + "'");
        }
        System.err.println(" printUTF8String END");
    }

    public static void readInDocFromPointable(UTF8StringPointable stringp, ByteBufferInputStream bbis,
            DataInputStream di, ArrayBackedValueStorage abvs, IParser parser) throws IOException {
        String fName = getStringFromPointable(stringp, bbis, di);
        readInDocFromString(fName, bbis, di, abvs, parser);
    }

    public static void readInDocFromString(String fName, ByteBufferInputStream bbis, DataInputStream di,
            ArrayBackedValueStorage abvs, IParser parser) throws IOException {
        Reader input;
        if (!fName.contains("hdfs:/")) {
            File file = new File(fName);
            if (file.exists()) {
                input = new InputStreamReader(new FileInputStream(file));
                parser.parse(input, abvs);
            } else {
                throw new FileNotFoundException(file.getAbsolutePath());
            }
        } else {
            // else check in HDFS file system
            HDFSFunctions hdfs = new HDFSFunctions(null, null);
            FileSystem fs = hdfs.getFileSystem();
            if (fs != null) {
                String fHdfsName = fName.replaceAll("hdfs:/", "");
                Path xmlDocument = new Path(fHdfsName);
                if (fs.exists(xmlDocument)) {
                    InputStream in = fs.open(xmlDocument).getWrappedStream();
                    input = new InputStreamReader(in);
                    parser.parse(input, abvs);
                    in.close();
                } else {
                    throw new FileNotFoundException(xmlDocument.getName());
                }
                fs.close();
            } else {
                throw new IOException();
            }
        }
    }

    public static boolean transformThenCompareMinMaxTaggedValues(AbstractValueComparisonOperation aOp,
            TaggedValuePointable tvp1, TaggedValuePointable tvp2, DynamicContext dCtx, TypedPointables tp1,
            TypedPointables tp2) throws SystemException {
        TaggedValuePointable tvp1new = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        TaggedValuePointable tvp2new = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();

        ArrayBackedValueStorage abvsArgument1 = new ArrayBackedValueStorage();
        DataOutput dOutArgument1 = abvsArgument1.getDataOutput();
        ArrayBackedValueStorage abvsArgument2 = new ArrayBackedValueStorage();
        DataOutput dOutArgument2 = abvsArgument2.getDataOutput();
        CastToDoubleOperation castToDouble = new CastToDoubleOperation();

        try {
            abvsArgument1.reset();
            if (tvp1.getTag() == ValueTag.XS_UNTYPED_ATOMIC_TAG) {
                tvp1.getValue(tp1.utf8sp);
                castToDouble.convertUntypedAtomic(tp1.utf8sp, dOutArgument1);
                tvp1new.set(abvsArgument1.getByteArray(), abvsArgument1.getStartOffset(),
                        DoublePointable.TYPE_TRAITS.getFixedLength() + 1);
            } else if (isDerivedFromInteger(tvp1.getTag())) {
                getIntegerPointable(tvp1, dOutArgument1, tp1);
                tvp1new.set(abvsArgument1.getByteArray(), abvsArgument1.getStartOffset(),
                        LongPointable.TYPE_TRAITS.getFixedLength() + 1);
            } else {
                tvp1new = tvp1;
            }
            abvsArgument2.reset();
            if (tvp2.getTag() == ValueTag.XS_UNTYPED_ATOMIC_TAG) {
                tvp2.getValue(tp2.utf8sp);
                castToDouble.convertUntypedAtomic(tp2.utf8sp, dOutArgument2);
                tvp2new.set(abvsArgument2.getByteArray(), abvsArgument2.getStartOffset(),
                        DoublePointable.TYPE_TRAITS.getFixedLength() + 1);
            } else if (isDerivedFromInteger(tvp2.getTag())) {
                getIntegerPointable(tvp2, dOutArgument2, tp1);
                tvp2new.set(abvsArgument2.getByteArray(), abvsArgument2.getStartOffset(),
                        LongPointable.TYPE_TRAITS.getFixedLength() + 1);
            } else {
                tvp2new = tvp2;
            }

            return compareTaggedValues(aOp, tvp1new, tvp2new, dCtx, tp1, tp2);
        } catch (SystemException se) {
            throw se;
        } catch (Exception e) {
            throw new SystemException(ErrorCode.SYSE0001, e);
        }
    }

    public static void writeChar(char c, DataOutput dOut) {
        try {
            if ((c >= 0x0001) && (c <= 0x007F)) {
                dOut.write((byte) c);
            } else if (c > 0x07FF) {
                dOut.write((byte) (0xE0 | ((c >> 12) & 0x0F)));
                dOut.write((byte) (0x80 | ((c >> 6) & 0x3F)));
                dOut.write((byte) (0x80 | ((c >> 0) & 0x3F)));
            } else {
                dOut.write((byte) (0xC0 | ((c >> 6) & 0x1F)));
                dOut.write((byte) (0x80 | ((c >> 0) & 0x3F)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeCharSequence(CharSequence charSequence, DataOutput dOut) {
        for (int i = 0; i < charSequence.length(); ++i) {
            writeChar(charSequence.charAt(i), dOut);
        }
    }

    public static void writeCharArray(char[] ch, int start, int length, DataOutput dOut) {
        for (int i = start; i < start + length; ++i) {
            writeChar(ch[i], dOut);
        }
    }

    public static void writeDateAsString(IDate date, DataOutput dOut) {
        // Year
        writeNumberWithPadding(date.getYear(), 4, dOut);
        writeChar('-', dOut);

        // Month
        writeNumberWithPadding(date.getMonth(), 2, dOut);
        writeChar('-', dOut);

        // Day
        writeNumberWithPadding(date.getDay(), 2, dOut);
    }

    /**
     * Writes a number to the DataOutput with zeros as place holders if the number is too small to fill the padding.
     *
     * @param value
     *            value
     * @param padding
     *            padding
     * @param dOut
     *            data output
     */

    public static void writeNumberWithPadding(long valueArg, int paddingArg, DataOutput dOut) {
        long value = valueArg;
        int padding = paddingArg;
        if (value < 0) {
            writeChar('-', dOut);
            value = Math.abs(value);
        }
        int nDigits = getNumberOfDigits(value);

        // Add zero padding for set length numbers.
        while (padding > nDigits) {
            writeChar('0', dOut);
            --padding;
        }

        // Write the actual number.
        long pow10 = (long) Math.pow(10, nDigits - 1.0);
        for (int i = nDigits - 1; i >= 0; --i) {
            writeChar((char) ('0' + (value / pow10)), dOut);
            value %= pow10;
            pow10 /= 10;
        }
    }

    public static void writeTimeAsString(ITime time, DataOutput dOut) {
        // Hours
        writeNumberWithPadding(time.getHour(), 2, dOut);
        writeChar(':', dOut);

        // Minute
        writeNumberWithPadding(time.getMinute(), 2, dOut);
        writeChar(':', dOut);

        // Milliseconds
        writeNumberWithPadding(time.getMilliSecond() / DateTime.CHRONON_OF_SECOND, 2, dOut);
        if (time.getMilliSecond() % DateTime.CHRONON_OF_SECOND != 0) {
            writeChar('.', dOut);
            writeNumberWithPadding(time.getMilliSecond() % DateTime.CHRONON_OF_SECOND, 3, dOut);
        }
    }

    public static void writeTimezoneAsString(ITimezone timezone, DataOutput dOut) {
        long timezoneHour = timezone.getTimezoneHour();
        long timezoneMinute = timezone.getTimezoneMinute();
        if (timezoneHour != DateTime.TIMEZONE_HOUR_NULL && timezoneMinute != DateTime.TIMEZONE_MINUTE_NULL) {
            if (timezoneHour == 0 && timezoneMinute == 0) {
                writeChar('Z', dOut);
            } else {
                if (timezoneHour >= 0 && timezoneMinute >= 0) {
                    writeChar('+', dOut);
                } else {
                    writeChar('-', dOut);
                    timezoneHour = Math.abs(timezoneHour);
                    timezoneMinute = Math.abs(timezoneMinute);
                }
                writeNumberWithPadding(timezoneHour, 2, dOut);
                writeChar(':', dOut);
                writeNumberWithPadding(timezoneMinute, 2, dOut);
            }
        }
    }

}
