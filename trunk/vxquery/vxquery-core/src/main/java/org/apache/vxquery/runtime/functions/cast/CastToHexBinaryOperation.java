package org.apache.vxquery.runtime.functions.cast;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.atomic.XSBinaryPointable;
import org.apache.vxquery.datamodel.values.ValueTag;
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
        int c = 0;
        byte halfByte1, halfByte2;
        while ((c = charIterator.next()) != ICharacterIterator.EOS_CHAR) {
            halfByte1 = getHexBinary((char) c);
            halfByte2 = getHexBinary((char) c);

            dOutInner.write(((halfByte1 & 0xf0) << 4) + (halfByte2 & 0x0f));
        }

        dOut.write(ValueTag.XS_HEX_BINARY_TAG);
        dOut.write((byte) ((abvsInner.getLength() >>> 8) & 0xFF));
        dOut.write((byte) ((abvsInner.getLength() >>> 0) & 0xFF));
        dOut.write(abvsInner.getByteArray(), abvsInner.getStartOffset(), abvsInner.getLength());
    }

    @Override
    public void convertUntypedAtomic(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        convertString(stringp, dOut);
    }

    private byte getHexBinary(char hexCharacter) {
        if (hexCharacter == Character.valueOf('0')) {
            return 0x00;
        } else if (hexCharacter == Character.valueOf('1')) {
            return 0x01;
        } else if (hexCharacter == Character.valueOf('2')) {
            return 0x02;
        } else if (hexCharacter == Character.valueOf('3')) {
            return 0x03;
        } else if (hexCharacter == Character.valueOf('4')) {
            return 0x04;
        } else if (hexCharacter == Character.valueOf('5')) {
            return 0x05;
        } else if (hexCharacter == Character.valueOf('6')) {
            return 0x06;
        } else if (hexCharacter == Character.valueOf('7')) {
            return 0x07;
        } else if (hexCharacter == Character.valueOf('8')) {
            return 0x08;
        } else if (hexCharacter == Character.valueOf('9')) {
            return 0x09;
        } else if (hexCharacter == Character.valueOf('a')) {
            return 0x0a;
        } else if (hexCharacter == Character.valueOf('b')) {
            return 0x0b;
        } else if (hexCharacter == Character.valueOf('c')) {
            return 0x0c;
        } else if (hexCharacter == Character.valueOf('d')) {
            return 0x0d;
        } else if (hexCharacter == Character.valueOf('e')) {
            return 0x0e;
        } else {
            return 0x0f;
        }
    }

}