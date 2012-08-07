package org.apache.vxquery.runtime.functions.unary;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.SystemException;

import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;

public class NumericUnaryPlusOperation extends AbstractNumericUnaryOperation {

    @Override
    public void operateDecimal(XSDecimalPointable decp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_DECIMAL_TAG);
        dOut.write(decp.getByteArray(), decp.getStartOffset(), decp.getLength());
    }

    @Override
    public void operateDouble(DoublePointable doublep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_DOUBLE_TAG);
        dOut.write(doublep.getByteArray(), doublep.getStartOffset(), doublep.getLength());
    }

    @Override
    public void operateFloat(FloatPointable floatp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_FLOAT_TAG);
        dOut.write(floatp.getByteArray(), floatp.getStartOffset(), floatp.getLength());
    }

    @Override
    public void operateInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_INTEGER_TAG);
        dOut.write(longp.getByteArray(), longp.getStartOffset(), longp.getLength());
    }
}