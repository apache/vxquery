package org.apache.vxquery.datamodel.accessors;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.api.AbstractPointable;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.api.IPointableFactory;
import edu.uci.ics.hyracks.data.std.primitive.BytePointable;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;

public class TaggedValuePointable extends AbstractPointable {
    public static final IPointableFactory FACTORY = new IPointableFactory() {
        private static final long serialVersionUID = 1L;

        @Override
        public ITypeTraits getTypeTraits() {
            return VoidPointable.TYPE_TRAITS;
        }

        @Override
        public IPointable createPointable() {
            return new TaggedValuePointable();
        }
    };

    public byte getTag() {
        return BytePointable.getByte(bytes, start);
    }

    public void getValue(IPointable value) {
        value.set(bytes, start + 1, length - 1);
    }
}