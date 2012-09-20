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
import org.apache.vxquery.datamodel.accessors.atomic.XSBinaryPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDatePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDateTimePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDurationPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSQNamePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSTimePointable;
import org.apache.vxquery.datamodel.api.IDate;
import org.apache.vxquery.datamodel.api.ITime;
import org.apache.vxquery.datamodel.api.ITimezone;
import org.apache.vxquery.datamodel.util.DateTime;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.SystemException;

import edu.uci.ics.hyracks.data.std.primitive.BooleanPointable;
import edu.uci.ics.hyracks.data.std.primitive.BytePointable;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.primitive.ShortPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.data.std.util.ByteArrayAccessibleOutputStream;

public class CastToStringOperation extends AbstractCastToOperation {
    private static long getPowerOf10(double value, long max, long min) {
        value = Math.abs(value);
        for (long i = min; i < max; i++) {
            if (Math.pow(10, i) > value)
                return i;
        }
        return max;
    }

    /**
     * Returns 0 if positive, nonzero if negative.
     * 
     * @param value
     * @return
     */
    public static boolean isNumberPostive(long value) {
        return ((value & 0x8000000000000000L) == 0 ? true : false);
    }

    private ByteArrayAccessibleOutputStream baaos = new ByteArrayAccessibleOutputStream();
    private ArrayBackedValueStorage abvsInner = new ArrayBackedValueStorage();
    private DataOutput dOutInner = abvsInner.getDataOutput();
    int returnTag = ValueTag.XS_STRING_TAG;
    private final char[] hex = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };
    private static final int DOUBLE_MANTISSA_BITS = 52; // size of the mantissa in bits
    private static final int DOUBLE_MANTISSA_OFFSET = -1075;
    private static final int DOUBLE_EXPONENT_MAX = 1023;
    private static final int DOUBLE_EXPONENT_MIN = -1022;
    private static final int FLOAT_MANTISSA_BITS = 23; // size of the mantissa in bits
    private static final int FLOAT_MANTISSA_OFFSET = -150;
    private static final int FLOAT_EXPONENT_MAX = 127;
    private static final int FLOAT_EXPONENT_MIN = -126;
    private static final int b = 2; // base of stored value
    private static final int B = 10; // base of printed value

    @Override
    public void convertAnyURI(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(returnTag);
        dOut.write(stringp.getByteArray(), stringp.getStartOffset(), stringp.getLength());
    }

    @Override
    public void convertBase64Binary(XSBinaryPointable binaryp, DataOutput dOut) throws SystemException, IOException {
        baaos.reset();
        Base64OutputStream b64os = new Base64OutputStream(baaos, true);
        b64os.write(binaryp.getByteArray(), binaryp.getStartOffset() + 2, binaryp.getLength() - 2);

        dOut.write(returnTag);
        dOut.write((byte) ((baaos.size() >>> 8) & 0xFF));
        dOut.write((byte) ((baaos.size() >>> 0) & 0xFF));
        dOut.write(baaos.getByteArray(), 0, baaos.size());
    }

    @Override
    public void convertBoolean(BooleanPointable boolp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        if (boolp.getBoolean()) {
            writeCharSequence("true", dOutInner);
        } else {
            writeCharSequence("false", dOutInner);
        }
        sendStringDataOutput(dOut);
    }

    @Override
    public void convertDate(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        writeDate(datep, dOutInner);
        writeTimezone(datep, dOutInner);

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertDatetime(XSDateTimePointable datetimep, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        writeDate(datetimep, dOutInner);
        writeChar('T', dOutInner);
        writeTime(datetimep, dOutInner);
        writeTimezone(datetimep, dOutInner);

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertDecimal(XSDecimalPointable decp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        byte decimalPlace = decp.getDecimalPlace();
        long value = decp.getDecimalValue();
        byte nDigits = decp.getDigitCount();

        if (!isNumberPostive(value)) {
            // Negative result, but the rest of the calculations can be based on a positive value.
            writeChar('-', dOutInner);
            value *= -1;
        }

        if (value == 0) {
            writeChar('0', dOutInner);
        } else {
            long pow10 = (long) Math.pow(10, nDigits - 1);
            int start = Math.max(decimalPlace, nDigits - 1);
            int end = Math.min(0, decimalPlace);

            for (int i = start; i >= end; --i) {
                if (i >= nDigits || i < 0) {
                    writeChar('0', dOutInner);
                } else {
                    writeChar((char) ('0' + (value / pow10)), dOutInner);
                    value %= pow10;
                    pow10 /= 10;
                }
                if (i == decimalPlace && value > 0) {
                    writeChar('.', dOutInner);
                }
            }
        }
        sendStringDataOutput(dOut);
    }

    @Override
    public void convertDouble(DoublePointable doublep, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        double value = doublep.getDouble();

        if (!Double.isInfinite(value) && !Double.isNaN(value) && Math.abs(value) >= 0.000001
                && Math.abs(value) <= 1000000) {
            CastToDecimalOperation castToDecimal = new CastToDecimalOperation();
            castToDecimal.convertDouble(doublep, dOutInner);
            XSDecimalPointable decp = (XSDecimalPointable) XSDecimalPointable.FACTORY.createPointable();
            decp.set(abvsInner.getByteArray(), abvsInner.getStartOffset() + 1,
                    XSDecimalPointable.TYPE_TRAITS.getFixedLength());
            convertDecimal(decp, dOut);
        } else if (value == -0.0 || value == 0.0) {
            long bits = Double.doubleToLongBits(value);
            boolean negative = ((bits >> 63) == 0) ? false : true;

            if (negative) {
                writeChar('-', dOutInner);
            }
            writeCharSequence("0", dOutInner);
            sendStringDataOutput(dOut);
        } else {
            convertDoubleCanonical(doublep, dOut);
        }
    }

    public void convertDoubleCanonical(DoublePointable doublep, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        double value = doublep.getDouble();

        if (Double.isInfinite(value)) {
            if (value == Double.NEGATIVE_INFINITY) {
                writeCharSequence("-", dOutInner);
            }
            writeCharSequence("INF", dOutInner);
        } else if (Double.isNaN(value)) {
            writeCharSequence("NaN", dOutInner);
        } else {
            /*
             * The double to string algorithm is based on a paper by Robert G Burger and 
             * R Kent Dybvig titled "Print Floating-Point Numbers Quickly and Accurately".
             */
            long bits = Double.doubleToLongBits(value);
            boolean decimalPlaced = false;

            boolean negative = ((bits >> 63) == 0) ? false : true;
            int e = (int) ((bits >> 52) & 0x7ffL);
            long f = (e == 0) ? (bits & 0xfffffffffffffL) << 1 : (bits & 0xfffffffffffffL) | 0x10000000000000L;
            e = e + DOUBLE_MANTISSA_OFFSET;

            if (negative) {
                writeChar('-', dOutInner);
            }
            if (value == 0) {
                writeCharSequence("0.0E0", dOutInner);
            } else {
                // Initialize variables
                double r, s, mPlus, mMinus;
                if (e >= 0) {
                    if (f == Math.pow(b, DOUBLE_MANTISSA_BITS - 1)) {
                        r = f * Math.pow(b, e) * 2;
                        s = 2;
                        mPlus = Math.pow(b, e);
                        mMinus = Math.pow(b, e + 1);
                    } else {
                        r = f * Math.pow(b, e + 1) * 2;
                        s = b * 2;
                        mPlus = Math.pow(b, e);
                        mMinus = Math.pow(b, e);
                    }
                } else {
                    if (e == DOUBLE_EXPONENT_MIN || f != Math.pow(b, DOUBLE_MANTISSA_BITS - 1)) {
                        r = f * Math.pow(b, e) * 2;
                        s = 2;
                        mPlus = Math.pow(b, e);
                        mMinus = Math.pow(b, e + 1);
                    } else {
                        r = f * Math.pow(b, e + 1) * 2;
                        s = b * 2;
                        mPlus = Math.pow(b, e);
                        mMinus = Math.pow(b, e);
                    }
                }

                double k = Math.ceil(Math.log10((r + mPlus) / s));
                if (k >= 0) {
                    s = s * Math.pow(B, k);
                } else {
                    r = r * Math.pow(B, -k);
                    mPlus = mPlus * Math.pow(B, -k);
                    mMinus = mMinus * Math.pow(B, -k);
                }

                double d;
                while (!Double.isInfinite(mPlus) && !Double.isNaN(mPlus) && !Double.isInfinite(mMinus)
                        && !Double.isNaN(mMinus) && !Double.isInfinite(r) && !Double.isNaN(r)) {
                    if (s == r) {
                        // Special case where the value is off by a factor of ten.
                        d = 1;
                    } else {
                        d = Math.floor((r * B) / s);
                    }
                    r = r * B % s;
                    mPlus = mPlus * B;
                    mMinus = mMinus * B;

                    if (r < mMinus && r + mPlus > s) {
                        if (r * 2 > s) {
                            d = d + 1;
                        }
                        writeChar((char) ('0' + d), dOutInner);
                        break;
                    } else if (r + mPlus > s) {
                        d = d + 1;
                        writeChar((char) ('0' + d), dOutInner);
                        break;
                    } else if (r < mMinus) {
                        writeChar((char) ('0' + d), dOutInner);
                        break;
                    }
                    writeChar((char) ('0' + d), dOutInner);
                    if (!decimalPlaced) {
                        decimalPlaced = true;
                        writeChar('.', dOutInner);
                    }
                }

                long decimalPlace = getPowerOf10(value, DOUBLE_EXPONENT_MAX, DOUBLE_EXPONENT_MIN) - 1;
                writeChar('E', dOutInner);
                writeNumberWithPadding(decimalPlace, 1, dOutInner);
            }
        }
        sendStringDataOutput(dOut);
    }

    @Override
    public void convertDTDuration(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        long dayTime = longp.getLong();

        if (dayTime == 0) {
            writeCharSequence("PT0S", dOutInner);
        } else {
            if (dayTime < 0) {
                writeChar('-', dOutInner);
                dayTime *= -1;
            }
            writeChar('P', dOutInner);

            // Day
            if (dayTime >= DateTime.CHRONON_OF_DAY) {
                writeNumberWithPadding(dayTime / DateTime.CHRONON_OF_DAY, 1, dOutInner);
                writeChar('D', dOutInner);
                dayTime %= DateTime.CHRONON_OF_DAY;
            }

            if (dayTime > 0) {
                writeChar('T', dOutInner);
            }

            // Hour
            if (dayTime >= DateTime.CHRONON_OF_HOUR) {
                writeNumberWithPadding(dayTime / DateTime.CHRONON_OF_HOUR, 1, dOutInner);
                writeChar('H', dOutInner);
                dayTime %= DateTime.CHRONON_OF_HOUR;
            }

            // Minute
            if (dayTime >= DateTime.CHRONON_OF_MINUTE) {
                writeNumberWithPadding(dayTime / DateTime.CHRONON_OF_MINUTE, 1, dOutInner);
                writeChar('M', dOutInner);
                dayTime %= DateTime.CHRONON_OF_MINUTE;
            }

            // Milliseconds
            if (dayTime > 0) {
                writeNumberWithPadding(dayTime / DateTime.CHRONON_OF_SECOND, 1, dOutInner);
                if (dayTime % DateTime.CHRONON_OF_SECOND != 0) {
                    writeChar('.', dOutInner);
                    writeNumberWithPadding(dayTime % DateTime.CHRONON_OF_SECOND, 3, dOutInner);
                }
                writeChar('S', dOutInner);
            }
        }
        sendStringDataOutput(dOut);
    }

    @Override
    public void convertDuration(XSDurationPointable durationp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        int yearMonth = durationp.getYearMonth();
        long dayTime = durationp.getDayTime();

        if (yearMonth < 0 || dayTime < 0) {
            writeChar('-', dOutInner);
            yearMonth *= -1;
            dayTime *= -1;
        }
        writeChar('P', dOutInner);

        // Year
        if (yearMonth >= 12) {
            writeNumberWithPadding(yearMonth / 12, 1, dOutInner);
            writeChar('Y', dOutInner);
        }

        // Month
        if (yearMonth % 12 > 0) {
            writeNumberWithPadding(yearMonth % 12, 1, dOutInner);
            writeChar('M', dOutInner);
        }

        // Day
        if (dayTime >= DateTime.CHRONON_OF_DAY) {
            writeNumberWithPadding(dayTime / DateTime.CHRONON_OF_DAY, 1, dOutInner);
            writeChar('D', dOutInner);
            dayTime %= DateTime.CHRONON_OF_DAY;
        }

        if (dayTime > 0) {
            writeChar('T', dOutInner);
        }

        // Hour
        if (dayTime >= DateTime.CHRONON_OF_HOUR) {
            writeNumberWithPadding(dayTime / DateTime.CHRONON_OF_HOUR, 1, dOutInner);
            writeChar('H', dOutInner);
            dayTime %= DateTime.CHRONON_OF_HOUR;
        }

        // Minute
        if (dayTime >= DateTime.CHRONON_OF_MINUTE) {
            writeNumberWithPadding(dayTime / DateTime.CHRONON_OF_MINUTE, 1, dOutInner);
            writeChar('M', dOutInner);
            dayTime %= DateTime.CHRONON_OF_MINUTE;
        }

        // Milliseconds
        if (dayTime > 0) {
            writeNumberWithPadding(dayTime / DateTime.CHRONON_OF_SECOND, 1, dOutInner);
            if (dayTime % DateTime.CHRONON_OF_SECOND != 0) {
                writeChar('.', dOutInner);
                writeNumberWithPadding(dayTime % DateTime.CHRONON_OF_SECOND, 3, dOutInner);
            }
            writeChar('S', dOutInner);
        }

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertFloat(FloatPointable floatp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
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
                writeChar('-', dOutInner);
            }
            writeCharSequence("0", dOutInner);
            sendStringDataOutput(dOut);
        } else {
            convertFloatCanonical(floatp, dOut);
        }
    }

    public void convertFloatCanonical(FloatPointable floatp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        float value = floatp.getFloat();

        if (Float.isInfinite(value)) {
            if (value == Float.NEGATIVE_INFINITY) {
                writeCharSequence("-", dOutInner);
            }
            writeCharSequence("INF", dOutInner);
        } else if (Float.isNaN(value)) {
            writeCharSequence("NaN", dOutInner);
        } else {
            /*
             * The double to string algorithm is based on a paper by Robert G Burger and 
             * R Kent Dybvig titled "Print Floating-Point Numbers Quickly and Accurately".
             */
            long bits = Float.floatToIntBits(value);
            boolean decimalPlaced = false;

            boolean negative = ((bits >> 31) == 0) ? false : true;
            int e = (int) ((bits >> 23) & 0xff);
            int f = (int) ((e == 0) ? (bits & 0x7fffff) << 1 : (bits & 0x7fffff) | 0x800000);
            e = e + FLOAT_MANTISSA_OFFSET;

            if (negative) {
                writeChar('-', dOutInner);
            }
            if (value == 0) {
                writeCharSequence("0.0E0", dOutInner);
            } else {
                // Initialize variables
                double r, s, mPlus, mMinus;
                if (e >= 0) {
                    if (f == Math.pow(b, FLOAT_MANTISSA_BITS - 1)) {
                        r = f * Math.pow(b, e) * 2;
                        s = 2;
                        mPlus = Math.pow(b, e);
                        mMinus = Math.pow(b, e + 1);
                    } else {
                        r = f * Math.pow(b, e + 1) * 2;
                        s = b * 2;
                        mPlus = Math.pow(b, e);
                        mMinus = Math.pow(b, e);
                    }
                } else {
                    if (e == FLOAT_EXPONENT_MIN || f != Math.pow(b, FLOAT_MANTISSA_BITS - 1)) {
                        r = f * Math.pow(b, e) * 2;
                        s = 2;
                        mPlus = Math.pow(b, e);
                        mMinus = Math.pow(b, e + 1);
                    } else {
                        r = f * Math.pow(b, e + 1) * 2;
                        s = b * 2;
                        mPlus = Math.pow(b, e);
                        mMinus = Math.pow(b, e);
                    }
                }

                double k = Math.ceil(Math.log10((r + mPlus) / s));
                if (k >= 0) {
                    s = s * Math.pow(B, k);
                } else {
                    r = r * Math.pow(B, -k);
                    mPlus = mPlus * Math.pow(B, -k);
                    mMinus = mMinus * Math.pow(B, -k);
                }

                double d;
                while (!Double.isInfinite(mPlus) && !Double.isNaN(mPlus) && !Double.isInfinite(mMinus)
                        && !Double.isNaN(mMinus)) {
                    if (s == r) {
                        // Special case where the value is off by a factor of ten.
                        d = 1;
                    } else {
                        d = Math.floor((r * B) / s);
                    }
                    r = r * B % s;
                    mPlus = mPlus * B;
                    mMinus = mMinus * B;

                    if (r < mMinus && r + mPlus > s) {
                        if (r * 2 > s) {
                            d = d + 1;
                        }
                        writeChar((char) ('0' + d), dOutInner);
                        break;
                    } else if (r + mPlus > s) {
                        d = d + 1;
                        writeChar((char) ('0' + d), dOutInner);
                        break;
                    } else if (r < mMinus) {
                        writeChar((char) ('0' + d), dOutInner);
                        break;
                    }
                    writeChar((char) ('0' + d), dOutInner);
                    if (!decimalPlaced) {
                        decimalPlaced = true;
                        writeChar('.', dOutInner);
                    }
                }

                long decimalPlace = getPowerOf10(value, FLOAT_EXPONENT_MAX, FLOAT_EXPONENT_MIN) - 1;
                writeChar('E', dOutInner);
                writeNumberWithPadding(decimalPlace, 1, dOutInner);
            }
        }
        sendStringDataOutput(dOut);
    }

    @Override
    public void convertGDay(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        // Default
        writeChar('-', dOutInner);

        // Year
        writeChar('-', dOutInner);

        // Month
        writeChar('-', dOutInner);

        // Day
        writeNumberWithPadding(datep.getDay(), 2, dOutInner);

        // Timezone
        writeTimezone(datep, dOutInner);

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertGMonth(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        // Default
        writeChar('-', dOutInner);

        // Year
        writeChar('-', dOutInner);

        // Month
        writeNumberWithPadding(datep.getMonth(), 2, dOutInner);

        // Timezone
        writeTimezone(datep, dOutInner);

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertGMonthDay(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        // Default
        writeChar('-', dOutInner);

        // Year
        writeChar('-', dOutInner);

        // Month
        writeNumberWithPadding(datep.getMonth(), 2, dOutInner);
        writeChar('-', dOutInner);

        // Day
        writeNumberWithPadding(datep.getDay(), 2, dOutInner);

        // Timezone
        writeTimezone(datep, dOutInner);

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertGYear(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        // Year
        writeNumberWithPadding(datep.getYear(), 4, dOutInner);

        // Timezone
        writeTimezone(datep, dOutInner);

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertGYearMonth(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        // Year
        writeNumberWithPadding(datep.getYear(), 4, dOutInner);
        writeChar('-', dOutInner);

        // Month
        writeNumberWithPadding(datep.getMonth(), 2, dOutInner);

        // Timezone
        writeTimezone(datep, dOutInner);

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertHexBinary(XSBinaryPointable binaryp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        for (int index = 0; index < binaryp.getBinaryLength(); ++index) {
            int bi = binaryp.getByteArray()[binaryp.getBinaryStart() + index] & 0xff;
            writeChar(hex[(bi >> 4)], dOutInner);
            writeChar(hex[(bi & 0xf)], dOutInner);
        }
        sendStringDataOutput(dOut);
    }

    @Override
    public void convertInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        writeNumberWithPadding(longp.getLong(), 1, dOutInner);
        sendStringDataOutput(dOut);
    }

    @Override
    public void convertNotation(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(returnTag);
        dOut.write(stringp.getByteArray(), stringp.getStartOffset(), stringp.getLength());
    }

    @Override
    public void convertQName(XSQNamePointable qnamep, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        if (qnamep.getPrefixUTFLength() > 0) {
            dOutInner.write(qnamep.getByteArray(), qnamep.getStartOffset() + qnamep.getUriLength() + 2,
                    qnamep.getPrefixUTFLength());
            writeChar(':', dOutInner);
        }
        dOutInner.write(qnamep.getByteArray(),
                qnamep.getStartOffset() + qnamep.getUriLength() + qnamep.getPrefixLength() + 2,
                qnamep.getLocalNameUTFLength());

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertString(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(returnTag);
        dOut.write(stringp.getByteArray(), stringp.getStartOffset(), stringp.getLength());
    }

    @Override
    public void convertTime(XSTimePointable timep, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        writeTime(timep, dOutInner);
        writeTimezone(timep, dOutInner);

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertUntypedAtomic(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        convertString(stringp, dOut);
    }

    @Override
    public void convertYMDuration(IntegerPointable intp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        int yearMonth = intp.getInteger();

        if (yearMonth == 0) {
            writeCharSequence("P0M", dOutInner);
        } else {
            if (yearMonth < 0) {
                writeChar('-', dOutInner);
                yearMonth *= -1;
            }
            writeChar('P', dOutInner);

            // Year
            if (yearMonth >= 12) {
                writeNumberWithPadding(yearMonth / 12, 1, dOutInner);
                writeChar('Y', dOutInner);
            }

            // Month
            if (yearMonth % 12 > 0) {
                writeNumberWithPadding(yearMonth % 12, 1, dOutInner);
                writeChar('M', dOutInner);
            }
        }
        sendStringDataOutput(dOut);
    }

    /**
     * Derived Datatypes
     */
    public void convertByte(BytePointable bytep, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        writeNumberWithPadding(bytep.longValue(), 1, dOutInner);
        sendStringDataOutput(dOut);
    }

    public void convertInt(IntegerPointable intp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        writeNumberWithPadding(intp.longValue(), 1, dOutInner);
        sendStringDataOutput(dOut);
    }

    public void convertLong(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        writeNumberWithPadding(longp.longValue(), 1, dOutInner);
        sendStringDataOutput(dOut);
    }

    public void convertNegativeInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        writeNumberWithPadding(longp.longValue(), 1, dOutInner);
        sendStringDataOutput(dOut);
    }

    public void convertNonNegativeInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        writeNumberWithPadding(longp.longValue(), 1, dOutInner);
        sendStringDataOutput(dOut);
    }

    public void convertNonPositiveInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        writeNumberWithPadding(longp.longValue(), 1, dOutInner);
        sendStringDataOutput(dOut);
    }

    public void convertPositiveInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        writeNumberWithPadding(longp.longValue(), 1, dOutInner);
        sendStringDataOutput(dOut);
    }

    public void convertShort(ShortPointable shortp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        writeNumberWithPadding(shortp.longValue(), 1, dOutInner);
        sendStringDataOutput(dOut);
    }

    public void convertUnsignedByte(ShortPointable shortp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        writeNumberWithPadding(shortp.longValue(), 1, dOutInner);
        sendStringDataOutput(dOut);
    }

    public void convertUnsignedInt(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        writeNumberWithPadding(longp.longValue(), 1, dOutInner);
        sendStringDataOutput(dOut);
    }

    public void convertUnsignedLong(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        writeNumberWithPadding(longp.longValue(), 1, dOutInner);
        sendStringDataOutput(dOut);
    }

    public void convertUnsignedShort(IntegerPointable intp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        writeNumberWithPadding(intp.longValue(), 1, dOutInner);
        sendStringDataOutput(dOut);
    }

    /**
     * Returns the number of digits in a long. A few special cases that needed attention.
     */
    private int getNumberOfDigits(long value) {
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

    private void sendStringDataOutput(DataOutput dOut) throws SystemException, IOException {
        dOut.write(returnTag);
        dOut.write((byte) ((abvsInner.getLength() >>> 8) & 0xFF));
        dOut.write((byte) ((abvsInner.getLength() >>> 0) & 0xFF));
        dOut.write(abvsInner.getByteArray(), abvsInner.getStartOffset(), abvsInner.getLength());
    }

    private void writeChar(char c, DataOutput dOut) {
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

    private void writeCharSequence(CharSequence charSequence, DataOutput dOut) {
        for (int i = 0; i < charSequence.length(); ++i) {
            writeChar(charSequence.charAt(i), dOut);
        }
    }

    private void writeDate(IDate date, DataOutput dOut) {
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
     * @param padding
     * @param dOut
     * @throws IOException
     */
    private void writeNumberWithPadding(long value, int padding, DataOutput dOut) {
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
        long pow10 = (long) Math.pow(10, nDigits - 1);
        for (int i = nDigits - 1; i >= 0; --i) {
            writeChar((char) ('0' + (value / pow10)), dOut);
            value %= pow10;
            pow10 /= 10;
        }
    }

    private void writeTime(ITime time, DataOutput dOut) {
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

    private void writeTimezone(ITimezone timezone, DataOutput dOut) {
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