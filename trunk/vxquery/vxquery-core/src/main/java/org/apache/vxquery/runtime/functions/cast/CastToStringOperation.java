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
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.data.std.util.ByteArrayAccessibleOutputStream;

public class CastToStringOperation extends AbstractCastToOperation {
    private ByteArrayAccessibleOutputStream baaos = new ByteArrayAccessibleOutputStream();
    private ArrayBackedValueStorage abvsInner = new ArrayBackedValueStorage();
    private DataOutput dOutInner = abvsInner.getDataOutput();
    int returnTag = ValueTag.XS_STRING_TAG;
    private final char[] hex = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

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

        if (!Double.isInfinite(value) && !Double.isNaN(value)
                && ((Math.abs(value) >= 0.000001 && Math.abs(value) <= 1000000) || value == 0)) {
            CastToDecimalOperation castToDecimal = new CastToDecimalOperation();
            castToDecimal.convertDouble(doublep, dOutInner);
            XSDecimalPointable decp = new XSDecimalPointable();
            decp.set(abvsInner.getByteArray(), abvsInner.getStartOffset() + 1, abvsInner.getLength());
            convertDecimal(decp, dOut);
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
            if (!isNumberPostive((long) value)) {
                // Negative result, but the rest of the calculations can be based on a positive value.
                writeChar('-', dOutInner);
                value *= -1;
            }
            byte decimalPlace = 0;
            // Move the decimal
            while (value % 1 != 0) {
                --decimalPlace;
                value *= 10;
            }
            // Remove extra zeros
            while (value != 0 && value % 10 == 0) {
                value /= 10;
                ++decimalPlace;
            }
            // Print out the value.
            int nDigits = (int) Math.log10(value) + 1;
            long pow10 = (long) Math.pow(10, nDigits - 1);
            if (nDigits < 0) {
                writeCharSequence("0.0", dOutInner);
            } else {
                for (int i = nDigits - 1; i >= 0; --i) {
                    writeChar((char) ('0' + Math.floor(value / pow10)), dOutInner);
                    value %= pow10;
                    pow10 /= 10;
                    if (i == nDigits - 1) {
                        writeChar('.', dOutInner);
                    } else {
                        ++decimalPlace;
                    }
                }
                if (nDigits == 1) {
                    writeChar('0', dOutInner);
                }
            }
            writeChar('E', dOutInner);
            writeNumberWithPadding(decimalPlace, 1, dOutInner);
        }
        sendStringDataOutput(dOut);
    }

    @Override
    public void convertDTDuration(IntegerPointable intp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        int dayTime = intp.getInteger();

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
        int dayTime = durationp.getDayTime();

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

        if (!Float.isInfinite(value) && !Float.isNaN(value)
                && ((Math.abs(value) >= 0.000001 && Math.abs(value) <= 1000000) || value == 0)) {
            CastToDecimalOperation castToDecimal = new CastToDecimalOperation();
            castToDecimal.convertFloat(floatp, dOutInner);
            XSDecimalPointable decp = new XSDecimalPointable();
            decp.set(abvsInner.getByteArray(), abvsInner.getStartOffset() + 1, abvsInner.getLength());
            convertDecimal(decp, dOut);
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
            if (!isNumberPostive((long) value)) {
                // Negative result, but the rest of the calculations can be based on a positive value.
                writeChar('-', dOutInner);
                value *= -1;
            }
            byte decimalPlace = 0;
            // Move the decimal
            while (value % 1 != 0) {
                --decimalPlace;
                value *= 10;
            }
            // Remove extra zeros
            while (value != 0 && value % 10 == 0) {
                value /= 10;
                ++decimalPlace;
            }
            // Print out the value.
            int nDigits = (int) Math.log10(value) + 1;
            long pow10 = (long) Math.pow(10, nDigits - 1);
            if (nDigits < 0) {
                writeCharSequence("0.0", dOutInner);
            } else {
                for (int i = nDigits - 1; i >= 0; --i) {
                    writeChar((char) ('0' + Math.floor(value / pow10)), dOutInner);
                    value %= pow10;
                    pow10 /= 10;
                    if (i == nDigits - 1) {
                        writeChar('.', dOutInner);
                    } else {
                        ++decimalPlace;
                    }
                }
                if (nDigits == 1) {
                    writeChar('0', dOutInner);
                }
            }
            writeChar('E', dOutInner);
            writeNumberWithPadding(decimalPlace, 1, dOutInner);
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
        long value = longp.getLong();
        if (value < 0) {
            // Negative result, but the rest of the calculations can be based on a positive value.
            writeChar('-', dOutInner);
            value *= -1;
        }
        int nDigits = (int) Math.log10(value) + 1;
        long pow10 = (long) Math.pow(10, nDigits - 1);
        for (int i = nDigits - 1; i >= 0; --i) {
            writeChar((char) ('0' + (value / pow10)), dOutInner);
            value %= pow10;
            pow10 /= 10;
        }
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
        dOutInner.write(qnamep.getByteArray(), qnamep.getStartOffset() + qnamep.getUriLength() + 4,
                qnamep.getPrefixLength());
        writeChar(':', dOutInner);
        dOutInner.write(qnamep.getByteArray(),
                qnamep.getStartOffset() + qnamep.getUriLength() + qnamep.getPrefixLength() + 6,
                qnamep.getLocalNameLength());

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertString(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        //        System.err.println(" convertString in CastToString length = " + stringp.getUTFLength());
        //        ICharacterIterator charIterator = new UTF8StringCharacterIterator(stringp);
        //        charIterator.reset();
        //        for (int c = charIterator.next(); c != ICharacterIterator.EOS_CHAR; c = charIterator.next()) {
        //            System.err.println("   parse value '" + c + "' as '" + Character.valueOf((char) c) + "'");
        //        }
        //        System.err.println(" convertString in CastToString AFTER");

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
     * Returns 0 if positive, nonzero if negative.
     * 
     * @param value
     * @return
     */
    public static boolean isNumberPostive(long value) {
        return ((value & 0x8000000000000000L) == 0 ? true : false);
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
        }
        value = Math.abs(value);
        int nDigits = (value == 0 ? 0 : (int) Math.log10(value) + 1);

        while (padding > nDigits) {
            writeChar('0', dOut);
            --padding;
        }
        int number;
        while (nDigits > 0) {
            number = (int) (value / Math.pow(10, nDigits - 1));
            writeChar(Character.forDigit(number, 10), dOut);
            value = (int) (value - number * Math.pow(10, nDigits - 1));
            --nDigits;
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