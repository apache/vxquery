package org.apache.vxquery.index;

public class IndexElement {
    private String id;
    private String type;
    private String elementpath;

    public IndexElement(String id, String type, String elementpath) {
        this.id = id;
        this.type = type;
        this.elementpath = elementpath;
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