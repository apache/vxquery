package org.apache.vxquery.runtime.functions.strings;

public class SubstringBeforeCharacterIterator implements ICharacterIterator {
    private ICharacterIterator searchIterator;
    private final UTF8StringCharacterIterator stringIterator;
    private int currentByteOffset;

    public SubstringBeforeCharacterIterator(UTF8StringCharacterIterator stringIterator) {
        this.stringIterator = stringIterator;
    }

    public void setSearch(ICharacterIterator searchIterator) {
        this.searchIterator = searchIterator;
    }

    @Override
    public char next() {
        // Default - no character exists.
        int c = ICharacterIterator.EOS_CHAR;
        boolean firstPass = true;
        searchIterator.reset();

        while (true) {
            int c1 = stringIterator.next();
            int c2 = searchIterator.next();
            if (firstPass) {
                // Save character and location for next call.
                currentByteOffset = stringIterator.getByteOffset();
                c = c1;
                firstPass = false;
            }
            if (c2 == ICharacterIterator.EOS_CHAR) {
                // End of string.
                c = ICharacterIterator.EOS_CHAR;
                break;
            }
            if (c1 != c2) {
                // No match found.
                break;
            }
        }

        stringIterator.setByteOffset(currentByteOffset);
        return (char) c;
    }

    @Override
    public void reset() {
        stringIterator.reset();
    }
}
