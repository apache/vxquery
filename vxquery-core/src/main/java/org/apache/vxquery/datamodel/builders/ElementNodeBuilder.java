package org.apache.vxquery.datamodel.builders;

public class ElementNodeBuilder {
    private DictionaryBuilder db;

    private int nameUriCode;

    private int nameLocalNameCode;

    private int namePrefixCode;

    private int typeUriCode;

    private int typeLocalNameCode;

    private int typePrefixCode;

    public void reset() {
        db = null;
    }

    public void setDictionaryBuilder(DictionaryBuilder db) {
        this.db = db;
    }

    public void setName(int uriCode, int localNameCode, int prefixCode) {
        nameUriCode = uriCode;
        nameLocalNameCode = localNameCode;
        namePrefixCode = prefixCode;
    }

    public void setType(int uriCode, int localNameCode, int prefixCode) {
        typeUriCode = uriCode;
        typeLocalNameCode = localNameCode;
        typePrefixCode = prefixCode;
    }
}