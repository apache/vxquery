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
package org.apache.vxquery.datamodel.builders.nodes;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hyracks.data.std.algorithms.BinarySearchAlgorithm;
import org.apache.hyracks.data.std.collections.api.IValueReferenceVector;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.util.string.UTF8StringUtil;
import org.apache.hyracks.util.string.UTF8StringWriter;
import org.apache.vxquery.util.GrowableIntArray;

public class DictionaryBuilder {
    private final GrowableIntArray stringEndOffsets;

    private final GrowableIntArray sortedSlotIndexes;

    private final ByteArrayAccessibleOutputStream dataBuffer;

    private final DataOutput dataBufferOut;

    private final ArrayBackedValueStorage cache;

    private final TreeMap<String, Integer> hashSlotIndexes;

    private boolean cacheReady;

    private final UTF8StringWriter UTF8Writer = new UTF8StringWriter();

    private final IValueReferenceVector sortedStringsVector = new IValueReferenceVector() {
        @Override
        public int getStart(int index) {
            int slot = sortedSlotIndexes.getArray()[index];
            return slot == 0 ? 0 : stringEndOffsets.getArray()[slot - 1];
        }

        @Override
        public int getSize() {
            return stringEndOffsets.getSize();
        }

        @Override
        public int getLength(int index) {
            int utfLength = UTF8StringUtil.getUTFLength(dataBuffer.getByteArray(), getStart(index));
            return utfLength + UTF8StringUtil.getNumBytesToStoreLength(utfLength);
        }

        @Override
        public byte[] getBytes(int index) {
            return dataBuffer.getByteArray();
        }
    };

    private final BinarySearchAlgorithm binSearch = new BinarySearchAlgorithm();

    public DictionaryBuilder() {
        stringEndOffsets = new GrowableIntArray();
        sortedSlotIndexes = new GrowableIntArray();
        dataBuffer = new ByteArrayAccessibleOutputStream();
        dataBufferOut = new DataOutputStream(dataBuffer);
        cache = new ArrayBackedValueStorage();
        hashSlotIndexes = new TreeMap<>();
        cacheReady = false;
    }

    public void reset() {
        stringEndOffsets.clear();
        sortedSlotIndexes.clear();
        dataBuffer.reset();
        hashSlotIndexes.clear();
        cacheReady = false;
    }

    public void writeFromCache(ArrayBackedValueStorage abvs) throws IOException {
        if (!cacheReady) {
            cache.reset();
            write(cache);
            cacheReady = true;
        }
        DataOutput out = abvs.getDataOutput();
        out.write(cache.getByteArray(), cache.getStartOffset(), cache.getLength());
    }

    public void write(ArrayBackedValueStorage abvs) throws IOException {
        DataOutput out = abvs.getDataOutput();
        int sizeOffset = abvs.getLength();
        out.writeInt(0);
        int entryCount = stringEndOffsets.getSize();
        out.writeInt(entryCount);
        int[] entryOffsets = stringEndOffsets.getArray();
        for (int i = 0; i < entryCount; ++i) {
            out.writeInt(entryOffsets[i]);
        }
        if (hashSlotIndexes.isEmpty()) {
            int[] sortedOffsets = sortedSlotIndexes.getArray();
            for (int i = 0; i < entryCount; ++i) {
                out.writeInt(sortedOffsets[i]);
            }
        } else {
            for (Entry<String, Integer> me : hashSlotIndexes.entrySet()) {
                out.writeInt((Integer) me.getValue());
            }
        }
        out.write(dataBuffer.getByteArray(), 0, dataBuffer.size());
        // TODO can this value be determined before writing. Could this be append only.
        IntegerPointable.setInteger(abvs.getByteArray(), sizeOffset, abvs.getLength() - sizeOffset);
    }

    public int lookup(String str) {
        Integer slotIndex = hashSlotIndexes.get(str);
        if (slotIndex == null) {
            try {
                UTF8Writer.writeUTF8(str, dataBufferOut);
                slotIndex = stringEndOffsets.getSize();
                dataBufferOut.writeInt(slotIndex);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            stringEndOffsets.append(dataBuffer.size());
            hashSlotIndexes.put(str, slotIndex);
            cacheReady = false;
        }
        return slotIndex;
    }

    public int lookup(UTF8StringPointable str) {
        boolean found = binSearch.find(sortedStringsVector, str);
        int index = binSearch.getIndex();
        if (found) {
            return sortedSlotIndexes.getArray()[index];
        }
        dataBuffer.write(str.getByteArray(), str.getStartOffset(), str.getLength());
        int slotIndex = stringEndOffsets.getSize();
        try {
            dataBufferOut.writeInt(slotIndex);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        stringEndOffsets.append(dataBuffer.size());
        sortedSlotIndexes.insert(index, slotIndex);
        cacheReady = false;
        return slotIndex;
    }
}
