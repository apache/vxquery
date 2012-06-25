package org.apache.vxquery.datamodel.accessors.nodes;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;

import edu.uci.ics.hyracks.data.std.algorithms.BinarySearchAlgorithm;
import edu.uci.ics.hyracks.data.std.api.AbstractPointable;
import edu.uci.ics.hyracks.data.std.collections.api.IValueReferenceVector;
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

    private final IValueReferenceVector sortedStringVector = new IValueReferenceVector() {
        @Override
        public int getSize() {
            return getDictionaryEntryCount();
        }

        @Override
        public byte[] getBytes(int index) {
            return bytes;
        }

        @Override
        public int getStart(int index) {
            int dataAreaStart = getDictionaryDataAreaStartOffset();
            int sortedPtrArrayStart = getDictionarySortedPointerArrayOffset();
            int sortedSlotValue = IntegerPointable
                    .getInteger(bytes, sortedPtrArrayStart + index * SORTED_PTR_SLOT_SIZE);
            return dataAreaStart + sortedSlotValue;
        }

        @Override
        public int getLength(int index) {
            return UTF8StringPointable.getUTFLength(bytes, getStart(index)) + 2;
        }
    };

    private final BinarySearchAlgorithm binSearch = new BinarySearchAlgorithm();

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
        int strLen = UTF8StringPointable.getUTFLength(bytes, dataAreaStart + idxSlotValue);
        string.set(bytes, dataAreaStart + idxSlotValue, strLen + 2);
    }

    public int lookupString(UTF8StringPointable key) {
        boolean found = binSearch.find(sortedStringVector, key);
        if (!found) {
            return -1;
        }
        int index = binSearch.getIndex();
        return IntegerPointable.getInteger(bytes,
                sortedStringVector.getStart(index) + sortedStringVector.getLength(index));
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