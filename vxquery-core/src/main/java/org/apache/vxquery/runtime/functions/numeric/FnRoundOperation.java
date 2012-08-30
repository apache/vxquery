package org.apache.vxquery.runtime.functions.numeric;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.SystemException;

import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;

public class FnRoundOperation extends AbstractNumericOperation {

    @Override
    public void operateDecimal(XSDecimalPointable decp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_DECIMAL_TAG);
        dOut.write(decp.getDecimalPlace());
        dOut.writeLong((long) Math.round(decp.getDecimalValue()));
    }

    @Override
    public void operateDouble(DoublePointable doublep, DataOutput dOut) throws SystemException, IOException {
        double value = doublep.getDouble();
        if (value < 0 && value >= -0.5) {
            value = -0.0;
        } else if (!Double.isNaN(value) && !Double.isInfinite(value) && value != 0.0) {
            value = Math.round(value);
        }
        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.writeDouble(value);
    }

    @Override
    public void operateFloat(FloatPointable floatp, DataOutput dOut) throws SystemException, IOException {
        float value = floatp.getFloat();
        if (value < 0 && value >= -0.5f) {
            value = -0.0f;
        } else if (!Float.isNaN(value) && !Float.isInfinite(value) && value != 0.0f) {
            value = Math.round(value);
        }
        dOut.write(ValueTag.XS_FLOAT_TAG);
        dOut.writeFloat(value);
    }

    @Override
    public void operateInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_INTEGER_TAG);
        dOut.writeLong(longp.getLong());
    }
}