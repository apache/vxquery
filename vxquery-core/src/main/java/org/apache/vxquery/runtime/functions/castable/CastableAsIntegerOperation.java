package org.apache.vxquery.runtime.functions.castable;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.cast.CastToIntegerOperation;

import edu.uci.ics.hyracks.data.std.primitive.BooleanPointable;
import edu.uci.ics.hyracks.data.std.primitive.BytePointable;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.primitive.ShortPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class CastableAsIntegerOperation extends AbstractCastableAsOperation {
    private ArrayBackedValueStorage abvsInner = new ArrayBackedValueStorage();
    private DataOutput dOutInner = abvsInner.getDataOutput();

    @Override
    public void convertBoolean(BooleanPointable boolp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 1);
    }

    @Override
    public void convertDecimal(XSDecimalPointable decp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 1);
    }

    @Override
    public void convertDouble(DoublePointable doublep, DataOutput dOut) throws SystemException, IOException {
        boolean castable = true;
        try {
            abvsInner.reset();
            CastToIntegerOperation castTo = new CastToIntegerOperation();
            castTo.convertDouble(doublep, dOutInner);
        } catch (Exception e) {
            castable = false;
        }
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) (castable ? 1 : 0));
    }

    @Override
    public void convertFloat(FloatPointable floatp, DataOutput dOut) throws SystemException, IOException {
        boolean castable = true;
        try {
            abvsInner.reset();
            CastToIntegerOperation castTo = new CastToIntegerOperation();
            castTo.convertFloat(floatp, dOutInner);
        } catch (Exception e) {
            castable = false;
        }
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) (castable ? 1 : 0));
    }

    @Override
    public void convertInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 1);
    }

    @Override
    public void convertString(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        boolean castable = true;
        try {
            abvsInner.reset();
            CastToIntegerOperation castTo = new CastToIntegerOperation();
            castTo.convertString(stringp, dOutInner);
        } catch (Exception e) {
            castable = false;
        }
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) (castable ? 1 : 0));
    }

    @Override
    public void convertUntypedAtomic(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        convertString(stringp, dOut);
    }

    /**
     * Derived Datatypes
     */
    public void convertByte(BytePointable bytep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 1);
    }

    public void convertInt(IntegerPointable intp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 1);
    }

    public void convertLong(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 1);
    }

    public void convertNegativeInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 1);
    }

    public void convertNonNegativeInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 1);
    }

    public void convertNonPositiveInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 1);
    }

    public void convertPositiveInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 1);
    }

    public void convertShort(ShortPointable shortp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 1);
    }

    public void convertUnsignedByte(BytePointable bytep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 1);
    }

    public void convertUnsignedInt(IntegerPointable intp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 1);
    }

    public void convertUnsignedLong(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 1);
    }

    public void convertUnsignedShort(ShortPointable shortp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_BOOLEAN_TAG);
        dOut.write((byte) 1);
    }
}