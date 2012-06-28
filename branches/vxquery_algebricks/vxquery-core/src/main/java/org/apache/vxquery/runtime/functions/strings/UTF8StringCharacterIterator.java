package org.apache.vxquery.runtime.functions.strings;

import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

public class UTF8StringCharacterIterator implements ICharacterIterator {
    private int byteOffset;
    private final UTF8StringPointable stringp;

    public UTF8StringCharacterIterator(UTF8StringPointable stringp) {
        this.stringp = stringp;
    }

    public int getByteOffset() {
        return byteOffset;
    }

    @Override
    public char next() {
        // Default - no character exists.
        int c = 0;
        if (byteOffset < stringp.getLength()) {
            c = stringp.charAt(byteOffset);
            System.err.println( "  UTF8StringCharacterIterator char[" + byteOffset + "] = " + c);
                // Increment cursor
            if ((c >= 0x0001) && (c <= 0x007F)) {
                ++byteOffset;
            } else if (c > 0x07FF) {
                byteOffset += 3;
            } else {
                byteOffset += 2;
            }
        }
        System.err.println( "  END UTF8StringCharacterIterator char[" + byteOffset + "] = " + c);
        return (char) c;
    }

    @Override
    public void reset() {
        byteOffset = 2;
    }

    public void setByteOffset(int byteOffset) {
        this.byteOffset = byteOffset;
    }

}
