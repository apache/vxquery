package org.apache.vxquery.datamodel;

import edu.uci.ics.hyracks.data.std.api.AbstractPointable;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;

public class SequencePointable extends AbstractPointable {
    private static final int ENTRY_COUNT_SIZE = 4;
    private static final int SLOT_SIZE = 4;

    public static int getSequenceLength(byte[] bytes, int start) {
        int entryCount = getEntryCount(bytes, start);
        return getSlotValue(bytes, start, entryCount - 1) + (getDataAreaOffset(bytes, start) - start);
    }

    public int getEntryCount() {
        return getEntryCount(bytes, start);
    }

    private static int getEntryCount(byte[] bytes, int start) {
        return IntegerPointable.getInteger(bytes, start);
    }

    public void getEntry(int idx, IPointable pointer) {
        int dataAreaOffset = getDataAreaOffset(bytes, start);
        pointer.set(bytes, dataAreaOffset + getRelativeEntryStartOffset(idx), getEntryLength(idx));
    }

    private static int getSlotValue(byte[] bytes, int start, int idx) {
        return IntegerPointable.getInteger(bytes, getSlotArrayOffset(start) + idx * SLOT_SIZE);
    }

    private int getRelativeEntryStartOffset(int idx) {
        return idx == 0 ? 0 : getSlotValue(bytes, start, idx - 1);
    }

    private int getEntryLength(int idx) {
        return getSlotValue(bytes, start, idx) - getRelativeEntryStartOffset(idx);
    }

    private static int getSlotArrayOffset(int start) {
        return start + ENTRY_COUNT_SIZE;
    }

    private static int getDataAreaOffset(byte[] bytes, int start) {
        return getSlotArrayOffset(start) + getEntryCount(bytes, start) * SLOT_SIZE;
    }
}