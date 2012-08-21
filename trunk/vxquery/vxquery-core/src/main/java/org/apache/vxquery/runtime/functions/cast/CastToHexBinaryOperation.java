package org.apache.vxquery.runtime.functions.cast;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.atomic.XSBinaryPointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.strings.ICharacterIterator;
import org.apache.vxquery.runtime.functions.strings.UTF8StringCharacterIterator;

import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class CastToHexBinaryOperation extends AbstractCastToOperation {
    private ArrayBackedValueStorage abvsInner = new ArrayBackedValueStorage();
    private DataOutput dOutInner = abvsInner.getDataOutput();

    @Override
    public void convertBase64Binary(XSBinaryPointable binaryp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_HEX_BINARY_TAG);
        dOut.write(binaryp.getByteArray(), binaryp.getStartOffset(), binaryp.getLength());
    }

    @Override
    public void convertHexBinary(XSBinaryPointable binaryp, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_HEX_BINARY_TAG);
        dOut.write(binaryp.getByteArray(), binaryp.getStartOffset(), binaryp.getLength());
    }

    @Override
    public void convertString(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        ICharacterIterator charIterator = new UTF8StringCharacterIterator(stringp);
        charIterator.reset();
        abvsInner.reset();
        int c1 = 0, c2 = 0, length = 0;
        while ((c1 = charIterator.next()) != ICharacterIterator.EOS_CHAR) {
            c2 = charIterator.next();
            if (c2 == ICharacterIterator.EOS_CHAR) {
                // Odd number of characters.
                throw new SystemException(ErrorCode.FORG0001);
            }
            if (Character.digit(c1, 16) < 0 || Character.digit(c1, 16) > 15 || Character.digit(c2, 16) < 0
                    || Character.digit(c2, 16) > 15) {
                // Invalid of characters.
                throw new SystemException(ErrorCode.FORG0001);
            }
            dOutInner.write(((Character.digit(c1, 16) << 4) + Character.digit(c2, 16)));
            length += 1;
        }

        dOut.write(ValueTag.XS_HEX_BINARY_TAG);
        dOut.write((byte) ((length >>> 8) & 0xFF));
        dOut.write((byte) ((length >>> 0) & 0xFF));
        dOut.write(abvsInner.getByteArray(), abvsInner.getStartOffset(), length);
    }

    @Override
    public void convertUntypedAtomic(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        convertString(stringp, dOut);
    }
}