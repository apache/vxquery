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

import org.apache.vxquery.util.GrowableIntArray;

import edu.uci.ics.hyracks.data.std.algorithms.BinarySearchAlgorithm;
import edu.uci.ics.hyracks.data.std.collections.api.IValueReferenceVector;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.data.std.util.ByteArrayAccessibleOutputStream;

public class DictionaryBuilder {
    private final GrowableIntArray stringEndOffsets;

    private final GrowableIntArray sortedSlotIndexes;

    private final ByteArrayAccessibleOutputStream dataBuffer;

    private final DataOutput dataBufferOut;

    private final ByteArrayAccessibleOutputStream tempStringData;

    private final DataOutput tempOut;

    private final UTF8StringPointable tempStringPointable;

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
            return UTF8StringPointable.getUTFLength(dataBuffer.getByteArray(), getStart(index)) + 2;
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
        tempStringData = new ByteArrayAccessibleOutputStream();
        tempOut = new DataOutputStream(tempStringData);
        tempStringPointable = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
    }
    
    public void reset() {
        stringEndOffsets.clear();
        sortedSlotIndexes.clear();
        dataBuffer.reset();
        tempStringData.reset();
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
        int[] sortedOffsets = sortedSlotIndexes.getArray();
        for (int i = 0; i < entryCount; ++i) {
            out.writeInt(sortedOffsets[i]);
        }
        out.write(dataBuffer.getByteArray(), 0, dataBuffer.size());
        IntegerPointable.setInteger(abvs.getByteArray(), sizeOffset, abvs.getLength() - sizeOffset);
    }

    public int lookup(String str) {
        tempStringData.reset();
        try {
            tempOut.writeUTF(str);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        tempStringPointable.set(tempStringData.getByteArray(), 0, tempStringData.size());
        return lookup(tempStringPointable);
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
        return slotIndex;
    }
}