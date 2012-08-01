package org.apache.vxquery.runtime.functions.cast;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.strings.ICharacterIterator;
import org.apache.vxquery.runtime.functions.strings.UTF8StringCharacterIterator;

import edu.uci.ics.hyracks.data.std.primitive.BooleanPointable;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

public class CastToFloatOperation extends AbstractCastToOperation {

    @Override
    public void convertBoolean(BooleanPointable boolp, DataOutput dOut) throws SystemException, IOException {
        float value = (boolp.getBoolean() ? 1 : 0);
        dOut.write(ValueTag.XS_FLOAT_TAG);
        dOut.writeFloat(value);
    }

    @Override
    public void convertDecimal(XSDecimalPointable decp, DataOutput dOut) throws SystemException, IOException {
        float value = decp.floatValue();
        dOut.write(ValueTag.XS_FLOAT_TAG);
        dOut.writeFloat(value);
    }

    @Override
    public void convertDouble(DoublePointable doublep, DataOutput dOut) throws SystemException, IOException {
        float value = doublep.floatValue();
        dOut.write(ValueTag.XS_FLOAT_TAG);
        dOut.writeFloat(value);
    }

    @Override
    public void convertFloat(FloatPointable floatp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_FLOAT_TAG);
        dOut.write(floatp.getByteArray(), floatp.getStartOffset(), floatp.getLength());
    }

    @Override
    public void convertInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        float value = longp.floatValue();
        dOut.write(ValueTag.XS_FLOAT_TAG);
        dOut.writeFloat(value);
    }

    @Override
    public void convertString(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        ICharacterIterator charIterator = new UTF8StringCharacterIterator(stringp);
        charIterator.reset();
        byte decimalPlace = 0;
        long value = 0;
        boolean pastDecimal = false, negativeValue = false;
        int c = ICharacterIterator.EOS_CHAR;
        while ((c = charIterator.next()) != ICharacterIterator.EOS_CHAR) {
            if (Character.isDigit(c)) {
                value = value * 10 + Character.getNumericValue(c);
                if (pastDecimal) {
                    decimalPlace--;
                }
            } else if (c == Character.valueOf('-')) {
                negativeValue = true;
            } else if (c == Character.valueOf('E')) {
                break;
            } else {
                pastDecimal = true;
            }
        }
        if (c == Character.valueOf('E')) {
            int moveOffset = 0;
            while ((c = charIterator.next()) != ICharacterIterator.EOS_CHAR) {
                if (Character.isDigit(c)) {
                    moveOffset = moveOffset * 10 + Character.getNumericValue(c);
                } else if (c == Character.valueOf('-')) {
                    moveOffset = moveOffset * -1;
                } else {
                    break;
                }
            }
            decimalPlace += moveOffset;
        }
        if (negativeValue) {
            value *= -1;
        }
        float valueFloat = (float) (value * Math.pow(10, decimalPlace));

        dOut.write(ValueTag.XS_FLOAT_TAG);
        dOut.writeFloat(valueFloat);
    }

    @Override
    public void convertUntypedAtomic(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        convertString(stringp, dOut);
    }

}