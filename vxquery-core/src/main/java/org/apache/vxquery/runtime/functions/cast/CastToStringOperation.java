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
import java.io.IOException;

import org.apache.commons.codec.binary.Base64OutputStream;
import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.BytePointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.vxquery.datamodel.accessors.atomic.XSBinaryPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDatePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDateTimePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDurationPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSQNamePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSTimePointable;
import org.apache.vxquery.datamodel.builders.atomic.StringValueBuilder;
import org.apache.vxquery.datamodel.builders.atomic.VXQueryUTF8StringBuilder;
import org.apache.vxquery.datamodel.util.DateTime;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;

public class CastToStringOperation extends AbstractCastToOperation {
    private static final int STRING_EXPECTED_LENGTH = 300;
    private final GrowableArray ga = new GrowableArray();
    private final VXQueryUTF8StringBuilder sb = new VXQueryUTF8StringBuilder();
    private ByteArrayAccessibleOutputStream baaos = new ByteArrayAccessibleOutputStream();
    private ArrayBackedValueStorage abvsInner = new ArrayBackedValueStorage();
    private DataOutput dOutInner = abvsInner.getDataOutput();
    protected int returnTag = ValueTag.XS_STRING_TAG;
    private final char[] hex = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };
    private final UTF8StringPointable stringp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();

    @Override
    public void convertAnyURI(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(returnTag);
        dOut.write(stringp.getByteArray(), stringp.getStartOffset(), stringp.getLength());
    }

    @Override
    public void convertBase64Binary(XSBinaryPointable binaryp, DataOutput dOut) throws SystemException, IOException {
        // Read binary
        Base64OutputStream b64os = new Base64OutputStream(baaos, true);
        b64os.write(binaryp.getByteArray(), binaryp.getStartOffset() + 2, binaryp.getLength() - 2);

        // Write string
        startString();
        sb.appendUtf8Bytes(baaos.getByteArray(), 0, baaos.size());
        sendStringDataOutput(dOut);
    }

    @Override
    public void convertBoolean(BooleanPointable boolp, DataOutput dOut) throws SystemException, IOException {
        startString();
        if (boolp.getBoolean()) {
            FunctionHelper.writeCharSequence("true", sb);
        } else {
            FunctionHelper.writeCharSequence("false", sb);
        }
        sendStringDataOutput(dOut);
    }

    @Override
    public void convertDate(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        startString();
        FunctionHelper.writeDateAsString(datep, sb);
        FunctionHelper.writeTimezoneAsString(datep, sb);
        sendStringDataOutput(dOut);
    }

    @Override
    public void convertDatetime(XSDateTimePointable datetimep, DataOutput dOut) throws SystemException, IOException {
        startString();
        FunctionHelper.writeDateAsString(datetimep, sb);
        FunctionHelper.writeChar('T', sb);
        FunctionHelper.writeTimeAsString(datetimep, sb);
        FunctionHelper.writeTimezoneAsString(datetimep, sb);
        sendStringDataOutput(dOut);
    }

    @Override
    public void convertDecimal(XSDecimalPointable decp, DataOutput dOut) throws SystemException, IOException {
        startString();
        byte decimalPlace = decp.getDecimalPlace();
        long value = decp.getDecimalValue();
        byte nDigits = decp.getDigitCount();

        if (!FunctionHelper.isNumberPostive(value)) {
            // Negative result, but the rest of the calculations can be based on a positive value.
            FunctionHelper.writeChar('-', sb);
            value *= -1;
        }

        if (value == 0) {
            FunctionHelper.writeChar('0', sb);
        } else {
            long pow10 = (long) Math.pow(10, nDigits - 1);
            int start = Math.max(decimalPlace, nDigits - 1);
            int end = Math.min(0, decimalPlace);

            for (int i = start; i >= end; --i) {
                if (i >= nDigits || i < 0) {
                    FunctionHelper.writeChar('0', sb);
                } else {
                    FunctionHelper.writeChar((char) ('0' + (value / pow10)), sb);
                    value %= pow10;
                    pow10 /= 10;
                }
                if (i == decimalPlace && value > 0) {
                    FunctionHelper.writeChar('.', sb);
                }
            }
        }
        sendStringDataOutput(dOut);
    }

    @Override
    public void convertDouble(DoublePointable doublep, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        startString();
        double value = doublep.getDouble();

        if (Double.isInfinite(value)) {
            if (value == Double.NEGATIVE_INFINITY) {
                FunctionHelper.writeCharSequence("-", sb);
            }
            FunctionHelper.writeCharSequence("INF", sb);
            sendStringDataOutput(dOut);
        } else if (Double.isNaN(value)) {
            FunctionHelper.writeCharSequence("NaN", sb);
            sendStringDataOutput(dOut);
        } else if (value == -0.0 || value == 0.0) {
            long bits = Double.doubleToLongBits(value);
            boolean negative = ((bits >> 63) == 0) ? false : true;

            if (negative) {
                FunctionHelper.writeCharSequence("-", sb);
            }
            FunctionHelper.writeCharSequence("0", sb);
            sendStringDataOutput(dOut);
        } else if (Math.abs(value) >= 0.000001 && Math.abs(value) <= 10000000) {
            //the jdk (toString function) does not output number in desired format when
            //a number is between one and ten million, so we take care of this
            //case separately here.
            CastToDecimalOperation castToDecimal = new CastToDecimalOperation();
            castToDecimal.convertDouble(doublep, dOutInner);
            XSDecimalPointable decp = (XSDecimalPointable) XSDecimalPointable.FACTORY.createPointable();
            decp.set(abvsInner.getByteArray(), abvsInner.getStartOffset() + 1,
                    XSDecimalPointable.TYPE_TRAITS.getFixedLength());
            if (Math.abs(value) <= 1000000) {
                convertDecimal(decp, dOut);
            } else {
                decimalToScientificNotn(decp, dOut);
            }
        } else {
            FunctionHelper.writeCharSequence(Double.toString(value), sb);
            sendStringDataOutput(dOut);
        }
    }

    public void convertDoubleCanonical(DoublePointable doublep, DataOutput dOut) throws SystemException, IOException {
        startString();
        double value = doublep.getDouble();
        FunctionHelper.writeCharSequence(Double.toString(value), sb);
        sendStringDataOutput(dOut);
    }

    @Override
    public void convertDTDuration(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        startString();
        long dayTime = longp.getLong();

        if (dayTime == 0) {
            FunctionHelper.writeCharSequence("PT0S", sb);
        } else {
            if (dayTime < 0) {
                FunctionHelper.writeChar('-', sb);
                dayTime *= -1;
            }
            FunctionHelper.writeChar('P', sb);

            // Day
            if (dayTime >= DateTime.CHRONON_OF_DAY) {
                FunctionHelper.writeNumberWithPadding(dayTime / DateTime.CHRONON_OF_DAY, 1, sb);
                FunctionHelper.writeChar('D', sb);
                dayTime %= DateTime.CHRONON_OF_DAY;
            }

            if (dayTime > 0) {
                FunctionHelper.writeChar('T', sb);
            }

            // Hour
            if (dayTime >= DateTime.CHRONON_OF_HOUR) {
                FunctionHelper.writeNumberWithPadding(dayTime / DateTime.CHRONON_OF_HOUR, 1, sb);
                FunctionHelper.writeChar('H', sb);
                dayTime %= DateTime.CHRONON_OF_HOUR;
            }

            // Minute
            if (dayTime >= DateTime.CHRONON_OF_MINUTE) {
                FunctionHelper.writeNumberWithPadding(dayTime / DateTime.CHRONON_OF_MINUTE, 1, sb);
                FunctionHelper.writeChar('M', sb);
                dayTime %= DateTime.CHRONON_OF_MINUTE;
            }

            // Milliseconds
            if (dayTime > 0) {
                FunctionHelper.writeNumberWithPadding(dayTime / DateTime.CHRONON_OF_SECOND, 1, sb);
                if (dayTime % DateTime.CHRONON_OF_SECOND != 0) {
                    FunctionHelper.writeChar('.', sb);
                    FunctionHelper.writeNumberWithPadding(dayTime % DateTime.CHRONON_OF_SECOND, 3, sb);
                }
                FunctionHelper.writeChar('S', sb);
            }
        }
        sendStringDataOutput(dOut);
    }

    @Override
    public void convertDuration(XSDurationPointable durationp, DataOutput dOut) throws SystemException, IOException {
        startString();
        int yearMonth = durationp.getYearMonth();
        long dayTime = durationp.getDayTime();

        if (yearMonth < 0 || dayTime < 0) {
            FunctionHelper.writeChar('-', sb);
            yearMonth *= -1;
            dayTime *= -1;
        }
        FunctionHelper.writeChar('P', sb);

        // Year
        if (yearMonth >= 12) {
            FunctionHelper.writeNumberWithPadding(yearMonth / 12, 1, sb);
            FunctionHelper.writeChar('Y', sb);
        }

        // Month
        if (yearMonth % 12 > 0) {
            FunctionHelper.writeNumberWithPadding(yearMonth % 12, 1, sb);
            FunctionHelper.writeChar('M', sb);
        }

        // Day
        if (dayTime >= DateTime.CHRONON_OF_DAY) {
            FunctionHelper.writeNumberWithPadding(dayTime / DateTime.CHRONON_OF_DAY, 1, sb);
            FunctionHelper.writeChar('D', sb);
            dayTime %= DateTime.CHRONON_OF_DAY;
        }

        if (dayTime > 0) {
            FunctionHelper.writeChar('T', sb);
        }

        // Hour
        if (dayTime >= DateTime.CHRONON_OF_HOUR) {
            FunctionHelper.writeNumberWithPadding(dayTime / DateTime.CHRONON_OF_HOUR, 1, sb);
            FunctionHelper.writeChar('H', sb);
            dayTime %= DateTime.CHRONON_OF_HOUR;
        }

        // Minute
        if (dayTime >= DateTime.CHRONON_OF_MINUTE) {
            FunctionHelper.writeNumberWithPadding(dayTime / DateTime.CHRONON_OF_MINUTE, 1, sb);
            FunctionHelper.writeChar('M', sb);
            dayTime %= DateTime.CHRONON_OF_MINUTE;
        }

        // Milliseconds
        if (dayTime > 0) {
            FunctionHelper.writeNumberWithPadding(dayTime / DateTime.CHRONON_OF_SECOND, 1, sb);
            if (dayTime % DateTime.CHRONON_OF_SECOND != 0) {
                FunctionHelper.writeChar('.', sb);
                FunctionHelper.writeNumberWithPadding(dayTime % DateTime.CHRONON_OF_SECOND, 3, sb);
            }
            FunctionHelper.writeChar('S', sb);
        }

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertFloat(FloatPointable floatp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        startString();
        float value = floatp.getFloat();

        if (!Float.isInfinite(value) && !Float.isNaN(value) && Math.abs(value) >= 0.000001
                && Math.abs(value) <= 1000000) {
            CastToDecimalOperation castToDecimal = new CastToDecimalOperation();
            castToDecimal.convertFloat(floatp, dOutInner);
            XSDecimalPointable decp = (XSDecimalPointable) XSDecimalPointable.FACTORY.createPointable();
            decp.set(abvsInner.getByteArray(), abvsInner.getStartOffset() + 1, abvsInner.getLength());
            convertDecimal(decp, dOut);
        } else if (value == -0.0f || value == 0.0f) {
            long bits = Float.floatToIntBits(value);
            boolean negative = ((bits >> 31) == 0) ? false : true;

            if (negative) {
                FunctionHelper.writeChar('-', sb);
            }
            FunctionHelper.writeCharSequence("0", sb);
            sendStringDataOutput(dOut);
        } else {
            convertFloatCanonical(floatp, dOut);
        }
    }

    public void convertFloatCanonical(FloatPointable floatp, DataOutput dOut) throws SystemException, IOException {
        startString();
        float value = floatp.getFloat();

        if (Float.isInfinite(value)) {
            if (value == Float.NEGATIVE_INFINITY) {
                FunctionHelper.writeCharSequence("-", sb);
            }
            FunctionHelper.writeCharSequence("INF", sb);
        } else if (Float.isNaN(value)) {
            FunctionHelper.writeCharSequence("NaN", sb);
        } else {
            FunctionHelper.writeCharSequence(Float.toString(value), sb);
        }
        sendStringDataOutput(dOut);
    }

    @Override
    public void convertGDay(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        startString();
        // Default
        FunctionHelper.writeChar('-', sb);

        // Year
        FunctionHelper.writeChar('-', sb);

        // Month
        FunctionHelper.writeChar('-', sb);

        // Day
        FunctionHelper.writeNumberWithPadding(datep.getDay(), 2, sb);

        // Timezone
        FunctionHelper.writeTimezoneAsString(datep, sb);

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertGMonth(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        startString();
        // Default
        FunctionHelper.writeChar('-', sb);

        // Year
        FunctionHelper.writeChar('-', sb);

        // Month
        FunctionHelper.writeNumberWithPadding(datep.getMonth(), 2, sb);

        // Timezone
        FunctionHelper.writeTimezoneAsString(datep, sb);

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertGMonthDay(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        startString();
        // Default
        FunctionHelper.writeChar('-', sb);

        // Year
        FunctionHelper.writeChar('-', sb);

        // Month
        FunctionHelper.writeNumberWithPadding(datep.getMonth(), 2, sb);
        FunctionHelper.writeChar('-', sb);

        // Day
        FunctionHelper.writeNumberWithPadding(datep.getDay(), 2, sb);

        // Timezone
        FunctionHelper.writeTimezoneAsString(datep, sb);

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertGYear(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        startString();
        // Year
        FunctionHelper.writeNumberWithPadding(datep.getYear(), 4, sb);

        // Timezone
        FunctionHelper.writeTimezoneAsString(datep, sb);

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertGYearMonth(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        startString();
        // Year
        FunctionHelper.writeNumberWithPadding(datep.getYear(), 4, sb);
        FunctionHelper.writeChar('-', sb);

        // Month
        FunctionHelper.writeNumberWithPadding(datep.getMonth(), 2, sb);

        // Timezone
        FunctionHelper.writeTimezoneAsString(datep, sb);

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertHexBinary(XSBinaryPointable binaryp, DataOutput dOut) throws SystemException, IOException {
        startString();
        for (int index = 0; index < binaryp.getBinaryLength(); ++index) {
            int bi = binaryp.getByteArray()[binaryp.getBinaryStart() + index] & 0xff;
            FunctionHelper.writeChar(hex[bi >> 4], sb);
            FunctionHelper.writeChar(hex[bi & 0xf], sb);
        }
        sendStringDataOutput(dOut);
    }

    @Override
    public void convertInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        startString();
        FunctionHelper.writeNumberWithPadding(longp.getLong(), 1, sb);
        sendStringDataOutput(dOut);
    }

    @Override
    public void convertNotation(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(returnTag);
        dOut.write(stringp.getByteArray(), stringp.getStartOffset(), stringp.getLength());
    }

    @Override
    public void convertQName(XSQNamePointable qnamep, DataOutput dOut) throws SystemException, IOException {
        startString();
        if (qnamep.getPrefixUTFLength() > 0) {
            qnamep.getPrefix(stringp);
            sb.appendUtf8StringPointable(stringp);
            FunctionHelper.writeChar(':', sb);
        }
        qnamep.getLocalName(stringp);
        sb.appendUtf8StringPointable(stringp);
        sendStringDataOutput(dOut);
    }

    @Override
    public void convertString(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(returnTag);
        dOut.write(stringp.getByteArray(), stringp.getStartOffset(), stringp.getLength());
    }

    @Override
    public void convertTime(XSTimePointable timep, DataOutput dOut) throws SystemException, IOException {
        startString();
        FunctionHelper.writeTimeAsString(timep, sb);
        FunctionHelper.writeTimezoneAsString(timep, sb);
        sendStringDataOutput(dOut);
    }

    @Override
    public void convertUntypedAtomic(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        convertString(stringp, dOut);
    }

    @Override
    public void convertYMDuration(IntegerPointable intp, DataOutput dOut) throws SystemException, IOException {
        startString();
        int yearMonth = intp.getInteger();

        if (yearMonth == 0) {
            FunctionHelper.writeCharSequence("P0M", sb);
        } else {
            if (yearMonth < 0) {
                FunctionHelper.writeChar('-', sb);
                yearMonth *= -1;
            }
            FunctionHelper.writeChar('P', sb);

            // Year
            if (yearMonth >= 12) {
                FunctionHelper.writeNumberWithPadding(yearMonth / 12, 1, sb);
                FunctionHelper.writeChar('Y', sb);
            }

            // Month
            if (yearMonth % 12 > 0) {
                FunctionHelper.writeNumberWithPadding(yearMonth % 12, 1, sb);
                FunctionHelper.writeChar('M', sb);
            }
        }
        sendStringDataOutput(dOut);
    }

    /**
     * Derived Numeric Datatypes
     */
    public void convertByte(BytePointable bytep, DataOutput dOut) throws SystemException, IOException {
        startString();
        FunctionHelper.writeNumberWithPadding(bytep.longValue(), 1, sb);
        sendStringDataOutput(dOut);
    }

    public void convertInt(IntegerPointable intp, DataOutput dOut) throws SystemException, IOException {
        startString();
        FunctionHelper.writeNumberWithPadding(intp.longValue(), 1, sb);
        sendStringDataOutput(dOut);
    }

    public void convertLong(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        startString();
        FunctionHelper.writeNumberWithPadding(longp.longValue(), 1, sb);
        sendStringDataOutput(dOut);
    }

    public void convertNegativeInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        startString();
        FunctionHelper.writeNumberWithPadding(longp.longValue(), 1, sb);
        sendStringDataOutput(dOut);
    }

    public void convertNonNegativeInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        startString();
        FunctionHelper.writeNumberWithPadding(longp.longValue(), 1, sb);
        sendStringDataOutput(dOut);
    }

    public void convertNonPositiveInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        startString();
        FunctionHelper.writeNumberWithPadding(longp.longValue(), 1, sb);
        sendStringDataOutput(dOut);
    }

    public void convertPositiveInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        startString();
        FunctionHelper.writeNumberWithPadding(longp.longValue(), 1, sb);
        sendStringDataOutput(dOut);
    }

    public void convertShort(ShortPointable shortp, DataOutput dOut) throws SystemException, IOException {
        startString();
        FunctionHelper.writeNumberWithPadding(shortp.longValue(), 1, sb);
        sendStringDataOutput(dOut);
    }

    public void convertUnsignedByte(ShortPointable shortp, DataOutput dOut) throws SystemException, IOException {
        startString();
        FunctionHelper.writeNumberWithPadding(shortp.longValue(), 1, sb);
        sendStringDataOutput(dOut);
    }

    public void convertUnsignedInt(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        startString();
        FunctionHelper.writeNumberWithPadding(longp.longValue(), 1, sb);
        sendStringDataOutput(dOut);
    }

    public void convertUnsignedLong(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        startString();
        FunctionHelper.writeNumberWithPadding(longp.longValue(), 1, sb);
        sendStringDataOutput(dOut);
    }

    public void convertUnsignedShort(IntegerPointable intp, DataOutput dOut) throws SystemException, IOException {
        startString();
        FunctionHelper.writeNumberWithPadding(intp.longValue(), 1, sb);
        sendStringDataOutput(dOut);
    }

    /**
     * Derived String Data Types
     */
    @Override
    public void convertEntity(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        // TODO Add check to verify string consists of limited character set.
        convertString(stringp, dOut);
    }

    @Override
    public void convertID(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        // TODO Add check to verify string consists of limited character set.
        convertString(stringp, dOut);
    }

    @Override
    public void convertIDREF(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        // TODO Add check to verify string consists of limited character set.
        convertString(stringp, dOut);
    }

    @Override
    public void convertLanguage(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        // TODO Add check to verify string consists of limited character set.
        convertString(stringp, dOut);
    }

    @Override
    public void convertName(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        // TODO Add check to verify string consists of limited character set.
        convertString(stringp, dOut);
    }

    @Override
    public void convertNCName(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        // TODO Add check to verify string consists of limited character set.
        convertString(stringp, dOut);
    }

    @Override
    public void convertNMToken(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        // TODO Add check to verify string consists of limited character set.
        convertString(stringp, dOut);
    }

    @Override
    public void convertNormalizedString(UTF8StringPointable stringp, DataOutput dOut)
            throws SystemException, IOException {
        // TODO Add check to verify string consists of limited character set.
        convertString(stringp, dOut);
    }

    @Override
    public void convertToken(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        // TODO Add check to verify string consists of limited character set.
        convertString(stringp, dOut);
    }

    private void startString() throws IOException {
        ga.reset();
        sb.reset(ga, STRING_EXPECTED_LENGTH);
    }

    private void sendStringDataOutput(DataOutput dOut) throws SystemException, IOException {
        dOut.write(returnTag);
        sb.finish();
        dOut.write(ga.getByteArray(), 0, ga.getLength());
    }

    public void decimalToScientificNotn(XSDecimalPointable decp, DataOutput dOut) throws SystemException, IOException {
        byte decimalPlace = decp.getDecimalPlace();
        long value = decp.getDecimalValue();
        byte nDigits = decp.getDigitCount();
        startString();

        if (!FunctionHelper.isNumberPostive(value)) {
            // Negative result, but the rest of the calculations can be based on a positive value.
            FunctionHelper.writeChar('-', sb);
            value *= -1;
        }

        if (value == 0) {
            FunctionHelper.writeChar('0', sb);
        } else {
            long pow10 = (long) Math.pow(10, nDigits - 1);
            FunctionHelper.writeNumberWithPadding((value / pow10), 0, sb);
            FunctionHelper.writeChar('.', sb);
            long mod = value % pow10;
            FunctionHelper.writeNumberWithPadding(mod, (nDigits - 1), sb);
            FunctionHelper.writeChar('E', sb);
            long power = (nDigits - 1) - decimalPlace;
            FunctionHelper.writeNumberWithPadding(power, 0, sb);
        }
        sendStringDataOutput(dOut);
    }

    @Override
    public void convertNull(DataOutput dOut) throws SystemException, IOException {
        StringValueBuilder svb = new StringValueBuilder();
        dOut.write(returnTag);
        svb.write("null", dOut);
    }
}
