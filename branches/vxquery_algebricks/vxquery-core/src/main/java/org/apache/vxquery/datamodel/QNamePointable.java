package org.apache.vxquery.datamodel;

import edu.uci.ics.hyracks.data.std.api.AbstractPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;

public class QNamePointable extends AbstractPointable {
    public static final int SIZE = 12;

    private static final int OFF_PREFIX = 0;
    private static final int OFF_NS = 4;
    private static final int OFF_LOCAL = 8;

    public int getPrefixCode() {
        return IntegerPointable.getInteger(bytes, start + OFF_PREFIX);
    }

    public int getNamespaceCode() {
        return IntegerPointable.getInteger(bytes, start + OFF_NS);
    }

    public int getLocalCode() {
        return IntegerPointable.getInteger(bytes, start + OFF_LOCAL);
    }
}