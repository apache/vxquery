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
package org.apache.vxquery.datamodel.accessors.jsonItem;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IPointableFactory;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;
import org.apache.vxquery.util.GrowableIntArray;

/**
 * The datamodel of the JSON object is represented in this class:
 * Byte 1: Value tag of object (109)
 * Byte 2 to 5: number of key-value pairs in the object
 * Next few bytes: Offsets for each key-value pair in the object in the order appearing in the json data
 * Next bytes: The keys in the object each followed by the value of the key. Each key is a StringPointable and the value
 * of the key will be the respective pointable starting with its valuetag.
 */
public class ObjectPointable extends AbstractPointable {
    public static final IPointableFactory FACTORY = new IPointableFactory() {
        private static final long serialVersionUID = 1L;

        @Override
        public ITypeTraits getTypeTraits() {
            return VoidPointable.TYPE_TRAITS;
        }

        @Override
        public IPointable createPointable() {
            return new ObjectPointable();
        }
    };
    private static final int ENTRY_COUNT_SIZE = IntegerPointable.TYPE_TRAITS.getFixedLength();
    private static final int SLOT_SIZE = 4;
    private final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
    private final GrowableIntArray slots = new GrowableIntArray();
    private final ArrayBackedValueStorage dataArea = new ArrayBackedValueStorage();

    private static int getSlotValue(byte[] bytes, int start, int idx) {
        return IntegerPointable.getInteger(bytes, getSlotArrayOffset(start) + idx * SLOT_SIZE);
    }

    private static int getEntryCount(byte[] bytes, int start) {
        return IntegerPointable.getInteger(bytes, start);
    }

    private static int getKeyLength(byte[] b, int s) {
        return UTF8StringPointable.getUTFLength(b, s) + 2;
    }

    private static int getSlotArrayOffset(int start) {
        return start + ENTRY_COUNT_SIZE;
    }

    private static int getDataAreaOffset(byte[] bytes, int start) {
        return getSlotArrayOffset(start) + getEntryCount(bytes, start) * SLOT_SIZE;
    }

    public void getKeys(IPointable result) throws SystemException {
        try {
            abvs.reset();
            slots.clear();
            dataArea.reset();
            int dataAreaOffset = getDataAreaOffset(bytes, start);
            int entryCount = getEntryCount();
            int s;
            for (int i = 0; i < entryCount; i++) {
                s = dataAreaOffset + getRelativeEntryStartOffset(i);
                dataArea.getDataOutput().write(ValueTag.XS_STRING_TAG);
                dataArea.getDataOutput().write(bytes, s, getKeyLength(bytes, s));
                slots.append(dataArea.getLength());
            }
            finishSequenceBuild();
            result.set(abvs);
        } catch (IOException e) {
            throw new SystemException(ErrorCode.SYSE0001);
        }
    }

    private void finishSequenceBuild() throws IOException {
        DataOutput out = abvs.getDataOutput();
        if (slots.getSize() != 1) {
            out.write(ValueTag.SEQUENCE_TAG);
            int size = slots.getSize();
            out.writeInt(size);
            if (size > 0) {
                int[] slotArray = slots.getArray();
                for (int i = 0; i < size; ++i) {
                    out.writeInt(slotArray[i]);
                }
                out.write(dataArea.getByteArray(), dataArea.getStartOffset(), dataArea.getLength());
            }
        } else {
            out.write(dataArea.getByteArray(), dataArea.getStartOffset(), dataArea.getLength());
        }
    }

    public void getValue(TaggedValuePointable key, TaggedValuePointable result) throws SystemException {
        int dataAreaOffset = getDataAreaOffset(bytes, start);
        int entryCount = getEntryCount();
        int s, l, i;
        for (i = 0; i < entryCount; i++) {
            s = dataAreaOffset + getRelativeEntryStartOffset(i);
            l = getKeyLength(bytes, s);
            if (FunctionHelper.arraysEqual(bytes, s, l, key.getByteArray(), key.getStartOffset() + 1,
                    key.getLength() - 1)) {
                result.set(bytes, s + l, getEntryLength(i) - l);
                return;
            }
        }
        if (entryCount == 0 || i == entryCount) {
            XDMConstants.setFalse(result);
        }
    }

    private int getRelativeEntryStartOffset(int idx) {
        return idx == 0 ? 0 : getSlotValue(bytes, start, idx - 1);
    }

    private int getEntryLength(int idx) {
        return getSlotValue(bytes, start, idx) - getRelativeEntryStartOffset(idx);
    }

    public int getEntryCount() {
        return getEntryCount(bytes, start);
    }

}
