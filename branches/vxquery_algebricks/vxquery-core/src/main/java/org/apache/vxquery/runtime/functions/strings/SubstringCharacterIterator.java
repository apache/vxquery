package org.apache.vxquery.runtime.functions.strings;


public class SubstringCharacterIterator implements ICharacterIterator {
    private final ICharacterIterator in;
    private int start;
    private int end;
    private int charOffset;
    
    public SubstringCharacterIterator(ICharacterIterator in) {
        this.in = in;
    }

    public void setBounds(int start, int end) {
        this.start = start;
        this.end = end;
    }

    @Override
    public char next() {
        char c = in.next();
        if (charOffset <= start) {
            ++charOffset;
            return next();
        }
        else if (charOffset <= end) {
            ++charOffset;
            return c;
        }
        else {
            return (char) 0;
        }
    }

    @Override
    public void reset() {
        charOffset = 0;
    }

}
