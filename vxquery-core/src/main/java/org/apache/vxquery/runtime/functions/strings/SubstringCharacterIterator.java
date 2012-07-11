package org.apache.vxquery.runtime.functions.strings;

public class SubstringCharacterIterator implements ICharacterIterator {
    private final ICharacterIterator in;
    private int start;
    private int length;
    private int charOffset;

    public SubstringCharacterIterator(ICharacterIterator in) {
        this.in = in;
    }

    public void setBounds(int start, int length) {
        this.start = start;
        this.length = length;
    }

    @Override
    public char next() {
        char c = in.next();
        // Only drill down if there is more to the string.
        if (c == ICharacterIterator.EOS_CHAR) {
            return c;
        } else if (charOffset < start) {
            ++charOffset;
            return next();
        } else if (charOffset < start + length) {
            ++charOffset;
            return c;
        }
        return (char) 0;
    }

    @Override
    public void reset() {
        in.reset();
        charOffset = 0;
    }

}
