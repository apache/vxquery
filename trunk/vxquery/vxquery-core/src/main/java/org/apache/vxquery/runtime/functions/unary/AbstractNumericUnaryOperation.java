package org.apache.vxquery.runtime.functions.unary;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.exceptions.SystemException;

import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;

public abstract class AbstractNumericUnaryOperation {

    public abstract void operateDecimal(XSDecimalPointable decp, DataOutput dOut) throws SystemException, IOException;

    public abstract void operateDouble(DoublePointable doublep, DataOutput dOut) throws SystemException, IOException;

    public abstract void operateFloat(FloatPointable floatp, DataOutput dOut) throws SystemException, IOException;

    public abstract void operateInteger(LongPointable longp, DataOutput dOut) throws SystemException, IOException;

}
