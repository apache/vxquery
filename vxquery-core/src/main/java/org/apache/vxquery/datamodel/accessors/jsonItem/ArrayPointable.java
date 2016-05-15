package org.apache.vxquery.datamodel.accessors.jsonItem;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IPointableFactory;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.vxquery.datamodel.accessors.SequencePointable;

public class ArrayPointable extends SequencePointable {
    private static final int ENTRY_COUNT_SIZE = 4;
    private static final int SLOT_SIZE = 4;
    public static final IPointableFactory FACTORY = new IPointableFactory() {
        private static final long serialVersionUID = 1L;

        @Override
        public ITypeTraits getTypeTraits() {
            return VoidPointable.TYPE_TRAITS;
        }

        @Override
        public IPointable createPointable() {
            return new ArrayPointable();
        }
    };

    public int getEntryCount() {
        return getEntryCount(bytes, start);
    }

    private static int getEntryCount(byte[] bytes, int start) {
        return IntegerPointable.getInteger(bytes, start);
    }

    public void getEntry(int idx, IPointable pointer) {
        int dataStart = getDataStart(bytes, start);
        pointer.set(bytes, dataStart + getRelativeEntryStartOffset(idx), getEntryLength(idx));
    }

    private static int getEntryOffsetValue(byte[] bytes, int start, int idx) {
        return IntegerPointable.getInteger(bytes, getOffsetsStart(start) + idx * SLOT_SIZE);
    }

    private int getRelativeEntryStartOffset(int idx) {
        return idx == 0 ? 0 : getEntryOffsetValue(bytes, start, idx - 1);
    }

    private int getEntryLength(int idx) {
        return getEntryOffsetValue(bytes, start, idx) - getRelativeEntryStartOffset(idx);
    }

    private static int getOffsetsStart(int start) {
        return start + ENTRY_COUNT_SIZE;
    }

    private static int getDataStart(byte[] bytes, int start) {
        return getOffsetsStart(start) + getEntryCount(bytes, start) * SLOT_SIZE;
    }
}
