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
import org.apache.vxquery.exceptions.ErrorCode;
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
    private ArrayBackedValueStorage abvsInner = new ArrayBackedValueStorage();
    private DataOutput dOutInner = abvsInner.getDataOutput();
    int returnTag = ValueTag.XS_STRING_TAG;

    @Override
    public void convertAnyURI(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(returnTag);
        dOut.write(stringp.getByteArray(), stringp.getStartOffset(), stringp.getLength());
    }

    @Override
    public void convertBase64Binary(XSBinaryPointable binaryp, DataOutput dOut) throws SystemException, IOException {
        ByteArrayAccessibleOutputStream baaos = new ByteArrayAccessibleOutputStream();
        Base64OutputStream b64os = new Base64OutputStream(baaos, false);
        b64os.write(binaryp.getByteArray(), binaryp.getStartOffset() + 2, binaryp.getLength() - 2);

        dOut.write(returnTag);
        dOut.write((byte) ((baaos.size() >>> 8) & 0xFF));
        dOut.write((byte) ((baaos.size() >>> 0) & 0xFF));
        dOut.write(baaos.getByteArray());
    }

    @Override
    public void convertBoolean(BooleanPointable boolp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        if (boolp.getBoolean()) {
            dOutInner.writeChars("true");
        } else {
            dOutInner.writeChars("false");
        }
        sendStringDataOutput(dOut);
    }

    @Override
    public void convertDate(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        // Year
        dOutInner.writeChars(String.format("%04d", datep.getYear()));
        dOutInner.writeChar('-');

        // Month
        dOutInner.writeChars(String.format("%02d", datep.getMonth()));
        dOutInner.writeChar('-');

        // Day
        dOutInner.writeChars(String.format("%02d", datep.getDay()));

        // Timezone
        if (datep.getTimezoneHour() != DateTime.TIMEZONE_HOUR_NULL
                && datep.getTimezoneMinute() != DateTime.TIMEZONE_MINUTE_NULL) {
            if (datep.getTimezoneMinute() >= 0 && datep.getTimezoneMinute() >= 0) {
                dOutInner.writeChar('+');
            } else {
                dOutInner.writeChar('-');
            }
            dOutInner.writeChars(String.format("%02d", datep.getTimezoneHour()));
            dOutInner.writeChar(':');
            dOutInner.writeChars(String.format("%02d", datep.getTimezoneMinute()));
        }

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertDatetime(XSDateTimePointable datetimep, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        // Year
        writeNumberWithPadding(datetimep.getYear(), 4, dOutInner);
        dOutInner.writeChar('-');

        // Month
        writeNumberWithPadding(datetimep.getMonth(), 2, dOutInner);
        dOutInner.writeChar('-');

        // Day
        writeNumberWithPadding(datetimep.getDay(), 2, dOutInner);
        dOutInner.writeChar('T');

        // Hour
        writeNumberWithPadding(datetimep.getHour(), 2, dOutInner);
        dOutInner.writeChar(':');

        // Minute
        writeNumberWithPadding(datetimep.getMinute(), 2, dOutInner);
        dOutInner.writeChar(':');

        // Milliseconds
        writeNumberWithPadding(datetimep.getMilliSecond(), 2, dOutInner);
        if (datetimep.getMilliSecond() % DateTime.CHRONON_OF_SECOND != 0) {
            dOutInner.writeChar('.');
            writeNumberWithPadding(datetimep.getMilliSecond() % DateTime.CHRONON_OF_SECOND, 3, dOutInner);
        }

        // Timezone
        if (datetimep.getTimezoneHour() != DateTime.TIMEZONE_HOUR_NULL
                && datetimep.getTimezoneMinute() != DateTime.TIMEZONE_MINUTE_NULL) {
            if (datetimep.getTimezoneMinute() >= 0 && datetimep.getTimezoneMinute() >= 0) {
                dOutInner.writeChar('+');
            } else {
                dOutInner.writeChar('-');
            }
            writeNumberWithPadding(datetimep.getTimezoneHour(), 2, dOutInner);
            dOutInner.writeChar(':');
            writeNumberWithPadding(datetimep.getTimezoneMinute(), 2, dOutInner);
        }

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertDecimal(XSDecimalPointable decp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        byte decimalPlace = decp.getDecimalPlace();
        long value = decp.getDecimalValue();
        if (value < 0) {
            // Negative result, but the rest of the calculations can be based on a positive value.
            dOutInner.writeChar('-');
            value *= -1;
        }
        int nDigits = (int) Math.log10(value) + 1;
        long pow10 = (long) Math.pow(10, nDigits - 1);
        int start = Math.max(decimalPlace, nDigits - 1);
        int end = Math.min(0, decimalPlace);

        for (int i = start; i >= end; --i) {
            if (i >= nDigits || i < 0) {
                dOutInner.writeChar('0');
            } else {
                dOutInner.writeChar((char) ('0' + (value / pow10)));
                value %= pow10;
                pow10 /= 10;
            }
            if (i == decimalPlace) {
                dOutInner.writeChar('.');
            }
        }

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertDouble(DoublePointable doublep, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        double value = doublep.getDouble();

        if (value < 0) {
            // Negative result, but the rest of the calculations can be based on a positive value.
            dOutInner.writeChar('-');
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
            dOutInner.writeChars("0.0");
        } else {
            for (int i = nDigits - 1; i >= 0; --i) {
                dOutInner.writeChar((char) ('0' + (value / pow10)));
                value %= pow10;
                pow10 /= 10;
                if (i == nDigits - 1) {
                    dOutInner.writeChar('.');
                } else {
                    ++decimalPlace;
                }
            }
        }
        dOutInner.writeChar('E');
        writeNumberWithPadding(decimalPlace, 1, dOutInner);

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertDTDuration(IntegerPointable intp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        int dayTime = intp.getInteger();

        if (dayTime < 0) {
            dOutInner.writeChar('-');
            dayTime *= -1;
        }
        dOutInner.writeChar('P');

        // Day
        if (dayTime > DateTime.CHRONON_OF_DAY) {
            writeNumberWithPadding(dayTime / DateTime.CHRONON_OF_DAY, 1, dOutInner);
            dOutInner.writeChar('D');
            dayTime %= DateTime.CHRONON_OF_DAY;
        }

        if (dayTime > 0) {
            dOutInner.writeChar('T');
        }

        // Hour
        if (dayTime > DateTime.CHRONON_OF_HOUR) {
            writeNumberWithPadding(dayTime / DateTime.CHRONON_OF_HOUR, 1, dOutInner);
            dOutInner.writeChar('H');
            dayTime %= DateTime.CHRONON_OF_HOUR;
        }

        // Minute
        if (dayTime > DateTime.CHRONON_OF_MINUTE) {
            writeNumberWithPadding(dayTime / DateTime.CHRONON_OF_MINUTE, 1, dOutInner);
            dOutInner.writeChar('M');
            dayTime %= DateTime.CHRONON_OF_MINUTE;
        }

        // Milliseconds
        if (dayTime > 0) {
            writeNumberWithPadding(dayTime / DateTime.CHRONON_OF_SECOND, 1, dOutInner);
            if (dayTime % DateTime.CHRONON_OF_SECOND != 0) {
                dOutInner.writeChar('.');
                writeNumberWithPadding(dayTime % DateTime.CHRONON_OF_SECOND, 3, dOutInner);
            }
            dOutInner.writeChar('S');
        }

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertDuration(XSDurationPointable durationp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        int dayTime = durationp.getDayTime();

        if (durationp.getYearMonth() < 0 || dayTime < 0) {
            dOutInner.writeChar('-');
        }
        dOutInner.writeChar('P');

        // Year
        if (durationp.getYearMonth() > 12) {
            dOutInner.writeChars(String.format("%01d", durationp.getYearMonth() / 12));
            dOutInner.writeChar('Y');
        }

        // Month
        if (durationp.getYearMonth() % 12 > 0) {
            dOutInner.writeChars(String.format("%01d", durationp.getYearMonth() % 12));
            dOutInner.writeChar('M');
        }

        // Day
        if (dayTime > DateTime.CHRONON_OF_DAY) {
            dOutInner.writeChars(String.format("%01d", dayTime / DateTime.CHRONON_OF_DAY));
            dOutInner.writeChar('D');
            dayTime %= DateTime.CHRONON_OF_DAY;
        }

        if (dayTime > 0) {
            dOutInner.writeChar('T');
        }

        // Hour
        if (dayTime > DateTime.CHRONON_OF_HOUR) {
            dOutInner.writeChars(String.format("%01d", dayTime / DateTime.CHRONON_OF_HOUR));
            dOutInner.writeChar('H');
            dayTime %= DateTime.CHRONON_OF_HOUR;
        }

        // Minute
        if (dayTime > DateTime.CHRONON_OF_MINUTE) {
            writeNumberWithPadding(dayTime / DateTime.CHRONON_OF_MINUTE, 1, dOutInner);
            dOutInner.writeChar('M');
            dayTime %= DateTime.CHRONON_OF_MINUTE;
        }

        // Milliseconds
        if (dayTime > 0) {
            dOutInner.writeChars(String.format("%01d", dayTime / DateTime.CHRONON_OF_SECOND));
            if (dayTime % DateTime.CHRONON_OF_SECOND != 0) {
                dOutInner.writeChar('.');
                dOutInner.writeChars(String.format("%03d", dayTime % DateTime.CHRONON_OF_SECOND));
            }
            dOutInner.writeChar('S');
        }

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertFloat(FloatPointable floatp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        float value = floatp.getFloat();

        if (value < 0) {
            // Negative result, but the rest of the calculations can be based on a positive value.
            dOutInner.writeChar('-');
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
            dOutInner.writeChars("0.0");
        } else {
            for (int i = nDigits - 1; i >= 0; --i) {
                dOutInner.writeChar((char) ('0' + (value / pow10)));
                value %= pow10;
                pow10 /= 10;
                if (i == nDigits - 1) {
                    dOutInner.writeChar('.');
                } else {
                    ++decimalPlace;
                }
            }
        }
        dOutInner.writeChar('E');
        writeNumberWithPadding(decimalPlace, 1, dOutInner);

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertGDay(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();

        dOutInner.writeChar('-');
        dOutInner.writeChar('-');
        dOutInner.writeChar('-');

        // Day
        dOutInner.writeChars(String.format("%02d", datep.getDay()));

        // Timezone
        if (datep.getTimezoneHour() != DateTime.TIMEZONE_HOUR_NULL
                && datep.getTimezoneMinute() != DateTime.TIMEZONE_MINUTE_NULL) {
            if (datep.getTimezoneMinute() >= 0 && datep.getTimezoneMinute() >= 0) {
                dOutInner.writeChar('+');
            } else {
                dOutInner.writeChar('-');
            }
            dOutInner.writeChars(String.format("%02d", datep.getTimezoneHour()));
            dOutInner.writeChar(':');
            dOutInner.writeChars(String.format("%02d", datep.getTimezoneMinute()));
        }

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertGMonth(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();

        dOutInner.writeChar('-');
        dOutInner.writeChar('-');

        // Month
        dOutInner.writeChars(String.format("%02d", datep.getMonth()));

        // Timezone
        if (datep.getTimezoneHour() != DateTime.TIMEZONE_HOUR_NULL
                && datep.getTimezoneMinute() != DateTime.TIMEZONE_MINUTE_NULL) {
            if (datep.getTimezoneMinute() >= 0 && datep.getTimezoneMinute() >= 0) {
                dOutInner.writeChar('+');
            } else {
                dOutInner.writeChar('-');
            }
            dOutInner.writeChars(String.format("%02d", datep.getTimezoneHour()));
            dOutInner.writeChar(':');
            dOutInner.writeChars(String.format("%02d", datep.getTimezoneMinute()));
        }

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertGMonthDay(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        // Year
        dOutInner.writeChar('-');

        // Month
        dOutInner.writeChars(String.format("%02d", datep.getMonth()));
        dOutInner.writeChar('-');

        // Day
        dOutInner.writeChars(String.format("%02d", datep.getDay()));

        // Timezone
        if (datep.getTimezoneHour() != DateTime.TIMEZONE_HOUR_NULL
                && datep.getTimezoneMinute() != DateTime.TIMEZONE_MINUTE_NULL) {
            if (datep.getTimezoneMinute() >= 0 && datep.getTimezoneMinute() >= 0) {
                dOutInner.writeChar('+');
            } else {
                dOutInner.writeChar('-');
            }
            dOutInner.writeChars(String.format("%02d", datep.getTimezoneHour()));
            dOutInner.writeChar(':');
            dOutInner.writeChars(String.format("%02d", datep.getTimezoneMinute()));
        }

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertGYear(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        // Year
        dOutInner.writeChars(String.format("%04d", datep.getYear()));

        // Timezone
        if (datep.getTimezoneHour() != DateTime.TIMEZONE_HOUR_NULL
                && datep.getTimezoneMinute() != DateTime.TIMEZONE_MINUTE_NULL) {
            if (datep.getTimezoneMinute() >= 0 && datep.getTimezoneMinute() >= 0) {
                dOutInner.writeChar('+');
            } else {
                dOutInner.writeChar('-');
            }
            dOutInner.writeChars(String.format("%02d", datep.getTimezoneHour()));
            dOutInner.writeChar(':');
            dOutInner.writeChars(String.format("%02d", datep.getTimezoneMinute()));
        }

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertGYearMonth(XSDatePointable datep, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        // Year
        dOutInner.writeChars(String.format("%04d", datep.getYear()));
        dOutInner.writeChar('-');

        // Month
        dOutInner.writeChars(String.format("%02d", datep.getMonth()));

        // Timezone
        if (datep.getTimezoneHour() != DateTime.TIMEZONE_HOUR_NULL
                && datep.getTimezoneMinute() != DateTime.TIMEZONE_MINUTE_NULL) {
            if (datep.getTimezoneMinute() >= 0 && datep.getTimezoneMinute() >= 0) {
                dOutInner.writeChar('+');
            } else {
                dOutInner.writeChar('-');
            }
            dOutInner.writeChars(String.format("%02d", datep.getTimezoneHour()));
            dOutInner.writeChar(':');
            dOutInner.writeChars(String.format("%02d", datep.getTimezoneMinute()));
        }

        sendStringDataOutput(dOut);
    }

    @Override
    public void convertHexBinary(XSBinaryPointable binaryp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        //        int index = 0;
        //        while (index < binaryp.getBinaryLength()) {
        //            byte characterTuple = binaryp.getByteArray()[0];
        //            writeHexCharacter(characterTuple & 0x0f, dOutInner);
        //            writeHexCharacter((characterTuple & 0xf0) << 4, dOutInner);
        //        }
        sendStringDataOutput(dOut);
    }

    @Override
    public void convertInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        long value = longp.getLong();
        if (value < 0) {
            // Negative result, but the rest of the calculations can be based on a positive value.
            dOutInner.writeChar('-');
            value *= -1;
        }
        int nDigits = (int) Math.log10(value) + 1;
        long pow10 = (long) Math.pow(10, nDigits - 1);
        for (int i = nDigits - 1; i >= 0; --i) {
            dOutInner.writeChar((char) ('0' + (value / pow10)));
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
        dOutInner.writeChar(':');
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
        // Hours
        dOutInner.writeChars(String.format("%02d", timep.getHour()));
        dOutInner.writeChar(':');

        // Minute
        dOutInner.writeChars(String.format("%02d", timep.getMinute()));
        dOutInner.writeChar(':');

        // Milliseconds
        dOutInner.writeChars(String.format("%02d", timep.getMilliSecond() / DateTime.CHRONON_OF_SECOND));
        if (timep.getMilliSecond() % DateTime.CHRONON_OF_SECOND != 0) {
            dOutInner.writeChar('.');
            dOutInner.writeChars(String.format("%03d", timep.getMilliSecond() % DateTime.CHRONON_OF_SECOND));
        }

        // Timezone
        if (timep.getTimezoneHour() != DateTime.TIMEZONE_HOUR_NULL
                && timep.getTimezoneMinute() != DateTime.TIMEZONE_MINUTE_NULL) {
            if (timep.getTimezoneMinute() >= 0 && timep.getTimezoneMinute() >= 0) {
                dOutInner.writeChar('+');
            } else {
                dOutInner.writeChar('-');
            }
            dOutInner.writeChars(String.format("%02d", timep.getTimezoneHour()));
            dOutInner.writeChar(':');
            dOutInner.writeChars(String.format("%02d", timep.getTimezoneMinute()));
        }

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

        if (yearMonth < 0) {
            dOutInner.writeChar('-');
            yearMonth *= -1;
        }
        dOutInner.writeChar('P');

        // Year
        if (yearMonth > 12) {
            writeNumberWithPadding(yearMonth / 12, 1, dOutInner);
            dOutInner.writeChar('Y');
        }

        // Month
        if (yearMonth % 12 > 0) {
            writeNumberWithPadding(yearMonth % 12, 1, dOutInner);
            dOutInner.writeChar('M');
        }

        sendStringDataOutput(dOut);
    }

    private void sendStringDataOutput(DataOutput dOut) throws SystemException, IOException {
        dOut.write(returnTag);
        dOut.write((byte) ((abvsInner.getLength() >>> 8) & 0xFF));
        dOut.write((byte) ((abvsInner.getLength() >>> 0) & 0xFF));
        dOut.write(abvsInner.getByteArray(), abvsInner.getStartOffset(), abvsInner.getLength());
    }

    private void writeHexCharacter(int hexCharacter, DataOutput dOut) throws IOException {
        switch (hexCharacter) {
            case 0:
                dOut.writeChar('0');
                break;
            case 1:
                dOut.writeChar('1');
                break;
            case 2:
                dOut.writeChar('2');
                break;
            case 3:
                dOut.writeChar('3');
                break;
            case 4:
                dOut.writeChar('4');
                break;
            case 5:
                dOut.writeChar('5');
                break;
            case 6:
                dOut.writeChar('6');
                break;
            case 7:
                dOut.writeChar('7');
                break;
            case 8:
                dOut.writeChar('8');
                break;
            case 9:
                dOut.writeChar('9');
                break;
            case 10:
                dOut.writeChar('a');
                break;
            case 11:
                dOut.writeChar('b');
                break;
            case 12:
                dOut.writeChar('c');
                break;
            case 13:
                dOut.writeChar('d');
                break;
            case 14:
                dOut.writeChar('e');
                break;
            case 15:
                dOut.writeChar('f');
                break;
            default:
                break;
        }
    }

    /**
     * Writes a number to the DataOutput with zeros as place holders if the number is too small to fill the padding.
     * 
     * @param value
     * @param padding
     * @param dOut
     * @throws IOException
     */
    private void writeNumberWithPadding(long value, int padding, DataOutput dOut) throws IOException {
        if (value < 0) {
            dOut.writeChar('-');
        }
        value = Math.abs(value);
        int nDigits = (value == 0 ? 0 : (int) Math.log10(value) + 1);

        while (padding > nDigits) {
            dOut.writeChar('0');
            --padding;
        }
        int number;
        while (nDigits > 0) {
            number = (int) (value / Math.pow(10, nDigits - 1));
            dOut.writeChar(Character.forDigit(number, 10));
            value = (int) (value - number * Math.pow(10, nDigits - 1));
            --nDigits;
        }
    }

}