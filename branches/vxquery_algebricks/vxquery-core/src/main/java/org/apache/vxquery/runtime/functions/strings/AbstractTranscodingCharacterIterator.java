package org.apache.vxquery.runtime.functions.strings;

public abstract class AbstractTranscodingCharacterIterator implements ICharacterIterator {
    private ICharacterIterator in;

    public AbstractTranscodingCharacterIterator(ICharacterIterator in) {
        this.in = in;
    }

    final public char next() {
        int c = in.next();
        System.err.println( "  AbstractTranscodingCharacterIterator  " + c);
        return c != ICharacterIterator.EOS_CHAR ? transcodeCharacter((char) c) : (char) ICharacterIterator.EOS_CHAR;
    }

    final public void reset() {
        in.reset();
    }

    protected abstract char transcodeCharacter(char c);

}
