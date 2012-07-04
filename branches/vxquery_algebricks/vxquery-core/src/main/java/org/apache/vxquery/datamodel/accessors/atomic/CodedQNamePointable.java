package org.apache.vxquery.datamodel.accessors.atomic;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.api.AbstractPointable;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.api.IPointableFactory;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;

public class CodedQNamePointable extends AbstractPointable {
    public static final int SIZE = 12;

    private static final int OFF_PREFIX = 0;
    private static final int OFF_NS = 4;
    private static final int OFF_LOCAL = 8;

    public static final ITypeTraits TYPE_TRAITS = new ITypeTraits() {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isFixedLength() {
            return true;
        }

        @Override
        public int getFixedLength() {
            return SIZE;
        }
    };

    public static final IPointableFactory FACTORY = new IPointableFactory() {
        private static final long serialVersionUID = 1L;

        @Override
        public ITypeTraits getTypeTraits() {
            return TYPE_TRAITS;
        }

        @Override
        public IPointable createPointable() {
            return new CodedQNamePointable();
        }
    };

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