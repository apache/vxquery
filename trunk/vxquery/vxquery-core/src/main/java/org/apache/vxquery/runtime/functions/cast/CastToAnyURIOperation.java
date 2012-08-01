package org.apache.vxquery.runtime.functions.cast;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.SystemException;

import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

public class CastToAnyURIOperation extends AbstractCastToOperation {

    @Override
    public void convertAnyURI(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_ANY_URI_TAG);
        dOut.write(stringp.getByteArray(), stringp.getStartOffset(), stringp.getLength());
    }

    @Override
    public void convertString(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_ANY_URI_TAG);
        dOut.write(stringp.getByteArray(), stringp.getStartOffset(), stringp.getLength());
    }

    @Override
    public void convertUntypedAtomic(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        convertString(stringp, dOut);
    }

}