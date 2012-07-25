package org.apache.vxquery.runtime.functions.strings;

public interface ICharacterIterator {
    public static final char EOS_CHAR = 0;

    public char next();

    public void reset();
}
