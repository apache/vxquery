package org.apache.vxquery.util;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.json.JsonHierarchicalStreamDriver;

public class Debug {
    public static String toString(Object o) {
        XStream xstream = new XStream(new JsonHierarchicalStreamDriver());
        return xstream.toXML(o);        
    }
}
