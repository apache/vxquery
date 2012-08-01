package org.apache.vxquery.runtime.functions.cast;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.atomic.XSDurationPointable;
import org.apache.vxquery.datamodel.util.DateTime;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.strings.ICharacterIterator;
import org.apache.vxquery.runtime.functions.strings.UTF8StringCharacterIterator;

import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

public class CastToDTDurationOperation extends AbstractCastToOperation {

    @Override
    public void convertDTDuration(IntegerPointable intp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.write(intp.getByteArray(), intp.getStartOffset(), intp.getLength());
    }

    @Override
    public void convertDuration(XSDurationPointable durationp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeInt(durationp.getDayTime());
    }

    @Override
    public void convertString(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        ICharacterIterator charIterator = new UTF8StringCharacterIterator(stringp);
        charIterator.reset();
        boolean pastDecimal = false, timeSection = false;
        byte decimalPlace = 3;

        int value = 0;
        long day = 0, hour = 0, minute = 0, millisecond = 0;
        long negativeResult = 1;

        // First character 
        int c = charIterator.next();
        if (c == Character.valueOf('-')) {
            negativeResult = -1;
            c = charIterator.next();
        }
        if (c != Character.valueOf('P')) {
            // Invalid duration format.
            throw new SystemException(ErrorCode.FORG0001);
        }

        while ((c = charIterator.next()) != ICharacterIterator.EOS_CHAR) {
            if (Character.isDigit(c)) {
                value = value * 10 + Character.getNumericValue(c);
                if (pastDecimal) {
                    --decimalPlace;
                }
            } else if (c == Character.valueOf('T')) {
                timeSection = true;
            } else if (c == Character.valueOf('.')) {
                pastDecimal = true;
            } else if (c == Character.valueOf('D') && !timeSection) {
                day = value;
                value = 0;
                pastDecimal = false;
            } else if (c == Character.valueOf('H') && timeSection) {
                hour = value;
                value = 0;
                pastDecimal = false;
            } else if (c == Character.valueOf('M') && timeSection) {
                minute = value;
                value = 0;
                pastDecimal = false;
            } else if (c == Character.valueOf('S') && timeSection) {
                millisecond = (long) (value * Math.pow(10, decimalPlace));
                value = 0;
                pastDecimal = false;
            } else {
                // Invalid duration format.
                throw new SystemException(ErrorCode.FORG0001);
            }
        }

        long dayTime = day * DateTime.CHRONON_OF_DAY + hour * DateTime.CHRONON_OF_HOUR + minute
                * DateTime.CHRONON_OF_MINUTE + millisecond;
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeInt((int) (negativeResult * dayTime));
    }

    @Override
    public void convertYMDuration(IntegerPointable intp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_DAY_TIME_DURATION_TAG);
        dOut.writeInt(0);
    }

    @Override
    public void convertUntypedAtomic(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        convertString(stringp, dOut);
    }

}