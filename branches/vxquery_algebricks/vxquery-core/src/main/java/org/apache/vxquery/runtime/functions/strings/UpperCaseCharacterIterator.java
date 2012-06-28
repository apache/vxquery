package org.apache.vxquery.runtime.functions.strings;

public class UpperCaseCharacterIterator extends AbstractTranscodingCharacterIterator implements ICharacterIterator {

    public UpperCaseCharacterIterator(ICharacterIterator in) {
        super(in);
    }

    @Override
    protected char transcodeCharacter(char c) {
        return Character.toUpperCase(c);
    }

}
