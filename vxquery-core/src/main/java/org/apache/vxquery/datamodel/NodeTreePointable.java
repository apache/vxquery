package org.apache.vxquery.datamodel;

import edu.uci.ics.hyracks.data.std.api.AbstractPointable;
import edu.uci.ics.hyracks.data.std.primitive.BytePointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

public class NodeTreePointable extends AbstractPointable {
    private static final int HEADER_NODEID_EXISTS_MASK = 0x01;
    private static final int HEADER_DICTIONARY_EXISTS_MASK = 0x02;
    private static final int HEADER_TYPE_EXISTS_MASK = 0x03;

    private static final int HEADER_OFFSET = 0;
    private static final int HEADER_SIZE = 1;
    private static final int NODE_ID_SIZE = 8;

    private static final int DICTIONARY_SIZE_SIZE = 4;
    private static final int DICTIONARY_NENTRIES_SIZE = 4;
    private static final int IDX_PTR_SLOT_SIZE = 4;
    private static final int SORTED_PTR_SLOT_SIZE = 4;

    public boolean nodeIdExists() {
        return (getHeader() & HEADER_NODEID_EXISTS_MASK) != 0;
    }

    public boolean dictionaryExists() {
        return (getHeader() & HEADER_DICTIONARY_EXISTS_MASK) != 0;
    }

    public boolean typeExists() {
        return (getHeader() & HEADER_TYPE_EXISTS_MASK) != 0;
    }

    public long getRootNodeId() {
        return nodeIdExists() ? LongPointable.getLong(bytes, getNodeIdOffset()) : -1;
    }

    public int getDictionaryEntryCount() {
        return dictionaryExists() ? IntegerPointable.getInteger(bytes, getDictionaryEntryCountOffset()) : 0;
    }

    public void getString(int idx, UTF8StringPointable string) {
        int nEntries = getDictionaryEntryCount();
        if (idx < 0 || idx >= nEntries) {
            throw new IllegalArgumentException(idx + " not within [0, " + nEntries + ")");
        }
        int dataAreaStart = getDictionaryDataAreaStartOffset();
        int idxSlotValue = IntegerPointable.getInteger(bytes, getDictionaryIndexPointerArrayOffset() + idx
                * IDX_PTR_SLOT_SIZE);
        int strLen = UTF8StringPointable.getStrLen(bytes, dataAreaStart + idxSlotValue);
        string.set(bytes, dataAreaStart + idxSlotValue, strLen + 2);
    }

    public int lookupString(UTF8StringPointable key) {
        int nEntries = getDictionaryEntryCount();
        int left = 0;
        int right = nEntries - 1;
        int sortedPtrArrayStart = getDictionarySortedPointerArrayOffset();
        int dataAreaStart = getDictionaryDataAreaStartOffset();
        while (left <= right) {
            int mid = (left + right) / 2;
            int sortedSlotValue = IntegerPointable.getInteger(bytes, sortedPtrArrayStart + mid * SORTED_PTR_SLOT_SIZE);
            int strStart = dataAreaStart + sortedSlotValue;
            int strLen = UTF8StringPointable.getStrLen(bytes, strStart);
            int cmp = key.compareTo(bytes, strStart, strLen + 2);
            if (cmp > 0) {
                left = mid + 1;
            } else if (cmp < 0) {
                right = mid - 1;
            } else {
                return IntegerPointable.getInteger(bytes, strStart + strLen + 2);
            }
        }
        return -1;
    }

    public void getRootNode(TaggedValuePointable node) {
        node.set(bytes, getRootNodeOffset(), length - getRootNodeOffset() + start);
    }

    private byte getHeader() {
        return BytePointable.getByte(bytes, getHeaderOffset());
    }

    private int getHeaderOffset() {
        return start + HEADER_OFFSET;
    }

    private int getHeaderSize() {
        return HEADER_SIZE;
    }

    private int getNodeIdOffset() {
        return getHeaderOffset() + getHeaderSize();
    }

    private int getNodeIdSize() {
        return nodeIdExists() ? NODE_ID_SIZE : 0;
    }

    private int getDictionaryOffset() {
        return getNodeIdOffset() + getNodeIdSize();
    }

    private int getDictionarySize() {
        return dictionaryExists() ? IntegerPointable.getInteger(bytes, getDictionaryOffset()) : 0;
    }

    private int getDictionaryEntryCountOffset() {
        return getDictionaryOffset() + DICTIONARY_SIZE_SIZE;
    }

    private int getDictionaryIndexPointerArrayOffset() {
        return getDictionaryEntryCountOffset() + DICTIONARY_NENTRIES_SIZE;
    }

    private int getDictionarySortedPointerArrayOffset() {
        return getDictionaryIndexPointerArrayOffset() + getDictionaryEntryCount() * IDX_PTR_SLOT_SIZE;
    }

    private int getDictionaryDataAreaStartOffset() {
        return getDictionaryIndexPointerArrayOffset() + getDictionaryEntryCount()
                * (IDX_PTR_SLOT_SIZE + SORTED_PTR_SLOT_SIZE);
    }

    private int getRootNodeOffset() {
        return getDictionaryOffset() + getDictionarySize();
    }
}