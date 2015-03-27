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
import org.apache.vxquery.datamodel.util.DateTime;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;

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
        @SuppressWarnings("resource")
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
            FunctionHelper.writeCharSequence("true", dOutInner);
        } else {
            FunctionHelper.writeCharSequence("false", dOutInner);
        }
        sendStringDataOutput(dOut);
    }

    @Override
    public void convertDate(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        FunctionHelper.writeDateAsString(datep, dOutInner);
        FunctionHelper.writeTimezoneAsString(datep, dOutInner);

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertDatetime(XSDateTimePointable datetimep, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        FunctionHelper.writeDateAsString(datetimep, dOutInner);
        FunctionHelper.writeChar('T', dOutInner);
        FunctionHelper.writeTimeAsString(datetimep, dOutInner);
        FunctionHelper.writeTimezoneAsString(datetimep, dOutInner);

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertDecimal(XSDecimalPointable decp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        byte decimalPlace = decp.getDecimalPlace();
        long value = decp.getDecimalValue();
        byte nDigits = decp.getDigitCount();

        if (!FunctionHelper.isNumberPostive(value)) {
            // Negative result, but the rest of the calculations can be based on a positive value.
            FunctionHelper.writeChar('-', dOutInner);
            value *= -1;
        }

        if (value == 0) {
            FunctionHelper.writeChar('0', dOutInner);
        } else {
            long pow10 = (long) Math.pow(10, nDigits - 1);
            int start = Math.max(decimalPlace, nDigits - 1);
            int end = Math.min(0, decimalPlace);

            for (int i = start; i >= end; --i) {
                if (i >= nDigits || i < 0) {
                    FunctionHelper.writeChar('0', dOutInner);
                } else {
                    FunctionHelper.writeChar((char) ('0' + (value / pow10)), dOutInner);
                    value %= pow10;
                    pow10 /= 10;
                }
                if (i == decimalPlace && value > 0) {
                    FunctionHelper.writeChar('.', dOutInner);
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
                FunctionHelper.writeChar('-', dOutInner);
            }
            FunctionHelper.writeCharSequence("0", dOutInner);
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
                FunctionHelper.writeCharSequence("-", dOutInner);
            }
            FunctionHelper.writeCharSequence("INF", dOutInner);
        } else if (Double.isNaN(value)) {
            FunctionHelper.writeCharSequence("NaN", dOutInner);
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
                FunctionHelper.writeChar('-', dOutInner);
            }
            if (value == 0) {
                FunctionHelper.writeCharSequence("0.0E0", dOutInner);
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
                        FunctionHelper.writeChar((char) ('0' + d), dOutInner);
                        break;
                    } else if (r + mPlus > s) {
                        d = d + 1;
                        FunctionHelper.writeChar((char) ('0' + d), dOutInner);
                        break;
                    } else if (r < mMinus) {
                        FunctionHelper.writeChar((char) ('0' + d), dOutInner);
                        break;
                    }
                    FunctionHelper.writeChar((char) ('0' + d), dOutInner);
                    if (!decimalPlaced) {
                        decimalPlaced = true;
                        FunctionHelper.writeChar('.', dOutInner);
                    }
                }

                long decimalPlace = FunctionHelper.getPowerOf10(value, DOUBLE_EXPONENT_MAX, DOUBLE_EXPONENT_MIN) - 1;
                FunctionHelper.writeChar('E', dOutInner);
                FunctionHelper.writeNumberWithPadding(decimalPlace, 1, dOutInner);
            }
        }
        sendStringDataOutput(dOut);
    }

    @Override
    public void convertDTDuration(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        long dayTime = longp.getLong();

        if (dayTime == 0) {
            FunctionHelper.writeCharSequence("PT0S", dOutInner);
        } else {
            if (dayTime < 0) {
                FunctionHelper.writeChar('-', dOutInner);
                dayTime *= -1;
            }
            FunctionHelper.writeChar('P', dOutInner);

            // Day
            if (dayTime >= DateTime.CHRONON_OF_DAY) {
                FunctionHelper.writeNumberWithPadding(dayTime / DateTime.CHRONON_OF_DAY, 1, dOutInner);
                FunctionHelper.writeChar('D', dOutInner);
                dayTime %= DateTime.CHRONON_OF_DAY;
            }

            if (dayTime > 0) {
                FunctionHelper.writeChar('T', dOutInner);
            }

            // Hour
            if (dayTime >= DateTime.CHRONON_OF_HOUR) {
                FunctionHelper.writeNumberWithPadding(dayTime / DateTime.CHRONON_OF_HOUR, 1, dOutInner);
                FunctionHelper.writeChar('H', dOutInner);
                dayTime %= DateTime.CHRONON_OF_HOUR;
            }

            // Minute
            if (dayTime >= DateTime.CHRONON_OF_MINUTE) {
                FunctionHelper.writeNumberWithPadding(dayTime / DateTime.CHRONON_OF_MINUTE, 1, dOutInner);
                FunctionHelper.writeChar('M', dOutInner);
                dayTime %= DateTime.CHRONON_OF_MINUTE;
            }

            // Milliseconds
            if (dayTime > 0) {
                FunctionHelper.writeNumberWithPadding(dayTime / DateTime.CHRONON_OF_SECOND, 1, dOutInner);
                if (dayTime % DateTime.CHRONON_OF_SECOND != 0) {
                    FunctionHelper.writeChar('.', dOutInner);
                    FunctionHelper.writeNumberWithPadding(dayTime % DateTime.CHRONON_OF_SECOND, 3, dOutInner);
                }
                FunctionHelper.writeChar('S', dOutInner);
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
            FunctionHelper.writeChar('-', dOutInner);
            yearMonth *= -1;
            dayTime *= -1;
        }
        FunctionHelper.writeChar('P', dOutInner);

        // Year
        if (yearMonth >= 12) {
            FunctionHelper.writeNumberWithPadding(yearMonth / 12, 1, dOutInner);
            FunctionHelper.writeChar('Y', dOutInner);
        }

        // Month
        if (yearMonth % 12 > 0) {
            FunctionHelper.writeNumberWithPadding(yearMonth % 12, 1, dOutInner);
            FunctionHelper.writeChar('M', dOutInner);
        }

        // Day
        if (dayTime >= DateTime.CHRONON_OF_DAY) {
            FunctionHelper.writeNumberWithPadding(dayTime / DateTime.CHRONON_OF_DAY, 1, dOutInner);
            FunctionHelper.writeChar('D', dOutInner);
            dayTime %= DateTime.CHRONON_OF_DAY;
        }

        if (dayTime > 0) {
            FunctionHelper.writeChar('T', dOutInner);
        }

        // Hour
        if (dayTime >= DateTime.CHRONON_OF_HOUR) {
            FunctionHelper.writeNumberWithPadding(dayTime / DateTime.CHRONON_OF_HOUR, 1, dOutInner);
            FunctionHelper.writeChar('H', dOutInner);
            dayTime %= DateTime.CHRONON_OF_HOUR;
        }

        // Minute
        if (dayTime >= DateTime.CHRONON_OF_MINUTE) {
            FunctionHelper.writeNumberWithPadding(dayTime / DateTime.CHRONON_OF_MINUTE, 1, dOutInner);
            FunctionHelper.writeChar('M', dOutInner);
            dayTime %= DateTime.CHRONON_OF_MINUTE;
        }

        // Milliseconds
        if (dayTime > 0) {
            FunctionHelper.writeNumberWithPadding(dayTime / DateTime.CHRONON_OF_SECOND, 1, dOutInner);
            if (dayTime % DateTime.CHRONON_OF_SECOND != 0) {
                FunctionHelper.writeChar('.', dOutInner);
                FunctionHelper.writeNumberWithPadding(dayTime % DateTime.CHRONON_OF_SECOND, 3, dOutInner);
            }
            FunctionHelper.writeChar('S', dOutInner);
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
                FunctionHelper.writeChar('-', dOutInner);
            }
            FunctionHelper.writeCharSequence("0", dOutInner);
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
                FunctionHelper.writeCharSequence("-", dOutInner);
            }
            FunctionHelper.writeCharSequence("INF", dOutInner);
        } else if (Float.isNaN(value)) {
            FunctionHelper.writeCharSequence("NaN", dOutInner);
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
                FunctionHelper.writeChar('-', dOutInner);
            }
            if (value == 0) {
                FunctionHelper.writeCharSequence("0.0E0", dOutInner);
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
                        FunctionHelper.writeChar((char) ('0' + d), dOutInner);
                        break;
                    } else if (r + mPlus > s) {
                        d = d + 1;
                        FunctionHelper.writeChar((char) ('0' + d), dOutInner);
                        break;
                    } else if (r < mMinus) {
                        FunctionHelper.writeChar((char) ('0' + d), dOutInner);
                        break;
                    }
                    FunctionHelper.writeChar((char) ('0' + d), dOutInner);
                    if (!decimalPlaced) {
                        decimalPlaced = true;
                        FunctionHelper.writeChar('.', dOutInner);
                    }
                }

                long decimalPlace = FunctionHelper.getPowerOf10(value, FLOAT_EXPONENT_MAX, FLOAT_EXPONENT_MIN) - 1;
                FunctionHelper.writeChar('E', dOutInner);
                FunctionHelper.writeNumberWithPadding(decimalPlace, 1, dOutInner);
            }
        }
        sendStringDataOutput(dOut);
    }

    @Override
    public void convertGDay(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        // Default
        FunctionHelper.writeChar('-', dOutInner);

        // Year
        FunctionHelper.writeChar('-', dOutInner);

        // Month
        FunctionHelper.writeChar('-', dOutInner);

        // Day
        FunctionHelper.writeNumberWithPadding(datep.getDay(), 2, dOutInner);

        // Timezone
        FunctionHelper.writeTimezoneAsString(datep, dOutInner);

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertGMonth(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        // Default
        FunctionHelper.writeChar('-', dOutInner);

        // Year
        FunctionHelper.writeChar('-', dOutInner);

        // Month
        FunctionHelper.writeNumberWithPadding(datep.getMonth(), 2, dOutInner);

        // Timezone
        FunctionHelper.writeTimezoneAsString(datep, dOutInner);

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertGMonthDay(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        // Default
        FunctionHelper.writeChar('-', dOutInner);

        // Year
        FunctionHelper.writeChar('-', dOutInner);

        // Month
        FunctionHelper.writeNumberWithPadding(datep.getMonth(), 2, dOutInner);
        FunctionHelper.writeChar('-', dOutInner);

        // Day
        FunctionHelper.writeNumberWithPadding(datep.getDay(), 2, dOutInner);

        // Timezone
        FunctionHelper.writeTimezoneAsString(datep, dOutInner);

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertGYear(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        // Year
        FunctionHelper.writeNumberWithPadding(datep.getYear(), 4, dOutInner);

        // Timezone
        FunctionHelper.writeTimezoneAsString(datep, dOutInner);

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertGYearMonth(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        // Year
        FunctionHelper.writeNumberWithPadding(datep.getYear(), 4, dOutInner);
        FunctionHelper.writeChar('-', dOutInner);

        // Month
        FunctionHelper.writeNumberWithPadding(datep.getMonth(), 2, dOutInner);

        // Timezone
        FunctionHelper.writeTimezoneAsString(datep, dOutInner);

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertHexBinary(XSBinaryPointable binaryp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        for (int index = 0; index < binaryp.getBinaryLength(); ++index) {
            int bi = binaryp.getByteArray()[binaryp.getBinaryStart() + index] & 0xff;
            FunctionHelper.writeChar(hex[(bi >> 4)], dOutInner);
            FunctionHelper.writeChar(hex[(bi & 0xf)], dOutInner);
        }
        sendStringDataOutput(dOut);
    }

    @Override
    public void convertInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        FunctionHelper.writeNumberWithPadding(longp.getLong(), 1, dOutInner);
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
            FunctionHelper.writeChar(':', dOutInner);
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
        FunctionHelper.writeTimeAsString(timep, dOutInner);
        FunctionHelper.writeTimezoneAsString(timep, dOutInner);

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
            FunctionHelper.writeCharSequence("P0M", dOutInner);
        } else {
            if (yearMonth < 0) {
                FunctionHelper.writeChar('-', dOutInner);
                yearMonth *= -1;
            }
            FunctionHelper.writeChar('P', dOutInner);

            // Year
            if (yearMonth >= 12) {
                FunctionHelper.writeNumberWithPadding(yearMonth / 12, 1, dOutInner);
                FunctionHelper.writeChar('Y', dOutInner);
            }

            // Month
            if (yearMonth % 12 > 0) {
                FunctionHelper.writeNumberWithPadding(yearMonth % 12, 1, dOutInner);
                FunctionHelper.writeChar('M', dOutInner);
            }
        }
        sendStringDataOutput(dOut);
    }

    /**
     * Derived Numeric Datatypes
     */
    public void convertByte(BytePointable bytep, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        FunctionHelper.writeNumberWithPadding(bytep.longValue(), 1, dOutInner);
        sendStringDataOutput(dOut);
    }

    public void convertInt(IntegerPointable intp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        FunctionHelper.writeNumberWithPadding(intp.longValue(), 1, dOutInner);
        sendStringDataOutput(dOut);
    }

    public void convertLong(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        FunctionHelper.writeNumberWithPadding(longp.longValue(), 1, dOutInner);
        sendStringDataOutput(dOut);
    }

    public void convertNegativeInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        FunctionHelper.writeNumberWithPadding(longp.longValue(), 1, dOutInner);
        sendStringDataOutput(dOut);
    }

    public void convertNonNegativeInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        FunctionHelper.writeNumberWithPadding(longp.longValue(), 1, dOutInner);
        sendStringDataOutput(dOut);
    }

    public void convertNonPositiveInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        FunctionHelper.writeNumberWithPadding(longp.longValue(), 1, dOutInner);
        sendStringDataOutput(dOut);
    }

    public void convertPositiveInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        FunctionHelper.writeNumberWithPadding(longp.longValue(), 1, dOutInner);
        sendStringDataOutput(dOut);
    }

    public void convertShort(ShortPointable shortp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        FunctionHelper.writeNumberWithPadding(shortp.longValue(), 1, dOutInner);
        sendStringDataOutput(dOut);
    }

    public void convertUnsignedByte(ShortPointable shortp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        FunctionHelper.writeNumberWithPadding(shortp.longValue(), 1, dOutInner);
        sendStringDataOutput(dOut);
    }

    public void convertUnsignedInt(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        FunctionHelper.writeNumberWithPadding(longp.longValue(), 1, dOutInner);
        sendStringDataOutput(dOut);
    }

    public void convertUnsignedLong(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        FunctionHelper.writeNumberWithPadding(longp.longValue(), 1, dOutInner);
        sendStringDataOutput(dOut);
    }

    public void convertUnsignedShort(IntegerPointable intp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        FunctionHelper.writeNumberWithPadding(intp.longValue(), 1, dOutInner);
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
    public void convertNormalizedString(UTF8StringPointable stringp, DataOutput dOut) throws SystemException,
            IOException {
        // TODO Add check to verify string consists of limited character set.
        convertString(stringp, dOut);
    }

    @Override
    public void convertToken(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        // TODO Add check to verify string consists of limited character set.
        convertString(stringp, dOut);
    }

    private void sendStringDataOutput(DataOutput dOut) throws SystemException, IOException {
        dOut.write(returnTag);
        dOut.write((byte) ((abvsInner.getLength() >>> 8) & 0xFF));
        dOut.write((byte) ((abvsInner.getLength() >>> 0) & 0xFF));
        dOut.write(abvsInner.getByteArray(), abvsInner.getStartOffset(), abvsInner.getLength());
    }

}