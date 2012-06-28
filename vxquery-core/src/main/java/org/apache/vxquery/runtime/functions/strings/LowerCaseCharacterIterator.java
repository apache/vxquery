package org.apache.vxquery.runtime.functions.strings;

public class LowerCaseCharacterIterator extends AbstractTranscodingCharacterIterator implements ICharacterIterator {

    public LowerCaseCharacterIterator(ICharacterIterator in) {
        super(in);
    }

    @Override
    protected char transcodeCharacter(char c) {
        return Character.toLowerCase(c);
    }

}
