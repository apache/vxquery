package org.apache.vxquery.index;

public class IndexElement {
    private String contents;
    private String id;
    private String type;
    private String elementpath;

    public IndexElement(String co, String string, String string2, String e) {
        contents = co;
        id = string;
        type = string2;
        elementpath = e;
    }

    public String contents() {
        return contents;
    }

    public String id() {
        return id;
    }

    public String type() {
        return type;
    }

    public String epath() {
        return elementpath;
    }

}