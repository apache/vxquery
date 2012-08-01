package org.apache.vxquery.runtime.functions.cast;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.strings.ICharacterIterator;
import org.apache.vxquery.runtime.functions.strings.LowerCaseCharacterIterator;
import org.apache.vxquery.runtime.functions.strings.UTF8StringCharacterIterator;

import edu.uci.ics.hyracks.data.std.primitive.BooleanPointable;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

public class CastToBooleanOperation extends AbstractCastToOperation {

    @Override
    public void convertBoolean(BooleanPointable boolp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write(boolp.getByteArray(), boolp.getStartOffset(), boolp.getLength());
    }

    @Override
    public void convertDecimal(XSDecimalPointable decp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        if (decp.getDecimalValue() == 1 && decp.getBeforeDecimalPlace() == 0) {
            dOut.write(1);
        } else {
            dOut.write(0);
        }
    }

    @Override
    public void convertDouble(DoublePointable doublep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        if (doublep.getDouble() == 1) {
            dOut.write(1);
        } else {
            dOut.write(0);
        }
    }

    @Override
    public void convertFloat(FloatPointable floatp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        if (floatp.getFloat() == 1) {
            dOut.write(1);
        } else {
            dOut.write(0);
        }
    }

    @Override
    public void convertInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        if (longp.getLong() == 1) {
            dOut.write(1);
        } else {
            dOut.write(0);
        }
    }

    @Override
    public void convertString(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        byte result = 0;
        ICharacterIterator charIterator = new LowerCaseCharacterIterator(new UTF8StringCharacterIterator(stringp));
        charIterator.reset();
        int c;
        search: for (int index = 0; (c = charIterator.next()) != ICharacterIterator.EOS_CHAR; ++index) {
            switch (index) {
                case 0:
                    if (c == Character.valueOf('1')) {
                        result = 1;
                    } else if (c != Character.valueOf('t')) {
                        break search;
                    }
                    break;
                case 1:
                    if (c != Character.valueOf('r')) {
                        break search;
                    }
                    break;
                case 2:
                    if (c != Character.valueOf('u')) {
                        break search;
                    }
                    break;
                case 3:
                    if (c == Character.valueOf('e')) {
                        result = 1;
                    }
                    break;
                default:
                    result = 0;
                    break search;
            }
        }
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write(result);
    }

    @Override
    public void convertUntypedAtomic(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        convertString(stringp, dOut);
    }
}