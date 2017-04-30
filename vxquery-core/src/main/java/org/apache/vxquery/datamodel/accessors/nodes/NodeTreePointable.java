/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.vxquery.datamodel.accessors.nodes;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.data.std.algorithms.BinarySearchAlgorithm;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IPointableFactory;
import org.apache.hyracks.data.std.collections.api.IValueReferenceVector;
import org.apache.hyracks.data.std.primitive.BytePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.util.string.UTF8StringUtil;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;

/*
 * NodeTree {
 *  NodeTreeHeader header;
 *  NodeId nodeId?;
 *  Dictionary dictionary?;
 *  ElementNode rootNode;
 * }
 *
 * ElementHeader (padded) {
 *  bit nodeIdExists;
 *  bit dictionaryExists;
 *  bit headerTypeExists;
 * }
 *
 * NodeId {
 *  int32 id;
 * }
 *
 * Dictionary {
 *  int32 numberOfItems
 *  int32[numberOfItems] lengthOfItem
 *  int32[numberOfItems] sortedItemIndex
 *  bytes[] itemData
 * }
 */
public class NodeTreePointable extends AbstractPointable {
    public static final int HEADER_NODEID_EXISTS_MASK = (1 << 0);
    public static final int HEADER_DICTIONARY_EXISTS_MASK = (1 << 1);
    public static final int HEADER_TYPE_EXISTS_MASK = (1 << 2);

    private static final int HEADER_OFFSET = 0;
    private static final int HEADER_SIZE = 1;
    private static final int NODE_ID_SIZE = 4;

    private static final int DICTIONARY_SIZE_SIZE = 4;
    private static final int DICTIONARY_NENTRIES_SIZE = 4;
    private static final int IDX_PTR_SLOT_SIZE = 4;
    private static final int SORTED_PTR_SLOT_SIZE = 4;

    public static final IPointableFactory FACTORY = new IPointableFactory() {
        private static final long serialVersionUID = 1L;

        @Override
        public ITypeTraits getTypeTraits() {
            return VoidPointable.TYPE_TRAITS;
        }

        @Override
        public IPointable createPointable() {
            return new NodeTreePointable();
        }
    };

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
            int sortedSlotValue = IntegerPointable.getInteger(bytes,
                    sortedPtrArrayStart + index * SORTED_PTR_SLOT_SIZE);
            return dataAreaStart + sortedSlotValue;
        }

        @Override
        public int getLength(int index) {
            int utfLength = UTF8StringUtil.getUTFLength(bytes, getStart(index));
            return utfLength + UTF8StringUtil.getNumBytesToStoreLength(utfLength);
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

    public int getRootNodeId() {
        return nodeIdExists() ? IntegerPointable.getInteger(bytes, getNodeIdOffset()) : -1;
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
        int idxSlotValue = idx == 0 ? 0
                : IntegerPointable.getInteger(bytes,
                        getDictionaryIndexPointerArrayOffset() + (idx - 1) * IDX_PTR_SLOT_SIZE);
        int strLen = UTF8StringUtil.getUTFLength(bytes, dataAreaStart + idxSlotValue);
        int strMetaLen = UTF8StringUtil.getNumBytesToStoreLength(strLen);
        string.set(bytes, dataAreaStart + idxSlotValue, strMetaLen + strLen);
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

    public int getDictionaryOffset() {
        return getNodeIdOffset() + getNodeIdSize();
    }

    public int getDictionarySize() {
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
        return getDictionaryIndexPointerArrayOffset()
                + getDictionaryEntryCount() * (IDX_PTR_SLOT_SIZE + SORTED_PTR_SLOT_SIZE);
    }

    private int getRootNodeOffset() {
        return getDictionaryOffset() + getDictionarySize();
    }
}
