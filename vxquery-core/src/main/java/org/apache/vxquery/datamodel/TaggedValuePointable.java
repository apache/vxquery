package org.apache.vxquery.datamodel;

import edu.uci.ics.hyracks.data.std.api.AbstractPointable;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.BytePointable;

public class TaggedValuePointable extends AbstractPointable {
    public byte getTag() {
        return BytePointable.getByte(bytes, 0);
    }

    public void getValue(IPointable value) {
        value.set(bytes, start + 1, length - 1);
    }
}