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
package org.apache.vxquery.datamodel.accessors.jsonitem;

import java.io.IOException;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IPointableFactory;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.util.string.UTF8StringUtil;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;

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
    private static final int SLOT_SIZE = IntegerPointable.TYPE_TRAITS.getFixedLength();
    private final SequenceBuilder sb = new SequenceBuilder();
    private final UTF8StringPointable key = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();

    private static int getSlotValue(byte[] bytes, int start, int idx) {
        return IntegerPointable.getInteger(bytes, getSlotArrayOffset(start) + idx * SLOT_SIZE);
    }

    private static int getEntryCount(byte[] bytes, int start) {
        return IntegerPointable.getInteger(bytes, start);
    }

    private static int getKeyLength(byte[] bytes, int start) {
        int utfLength = UTF8StringUtil.getUTFLength(bytes, start);
        return utfLength + UTF8StringUtil.getNumBytesToStoreLength(utfLength);
    }

    private static int getSlotArrayOffset(int start) {
        return start + ENTRY_COUNT_SIZE;
    }

    private static int getDataAreaOffset(byte[] bytes, int start) {
        return getSlotArrayOffset(start) + getEntryCount(bytes, start) * SLOT_SIZE;
    }

    public void getKeys(IMutableValueStorage abvs) throws IOException {
        abvs.reset();
        sb.reset(abvs);
        int dataAreaOffset = getDataAreaOffset(bytes, start);
        int entryCount = getEntryCount();
        int s;
        for (int i = 0; i < entryCount; i++) {
            s = dataAreaOffset + getRelativeEntryStartOffset(i);
            key.set(bytes, s, getKeyLength(bytes, s));
            sb.addItem(ValueTag.XS_STRING_TAG, key);
        }
        sb.finish();
    }

    //here the UTF8StringPointable of key is without the tag
    public boolean getValue(UTF8StringPointable key, IPointable result) {
        int dataAreaOffset = getDataAreaOffset(bytes, start);
        int entryCount = getEntryCount();
        int start;
        int length;
        int i;
        for (i = 0; i < entryCount; i++) {
            start = dataAreaOffset + getRelativeEntryStartOffset(i);
            length = getKeyLength(bytes, start);
            if (FunctionHelper.arraysEqual(bytes, start, length, key.getByteArray(), key.getStartOffset(),
                    key.getLength())) {
                result.set(bytes, start + length, getEntryLength(i) - length);
                return true;
            }
        }
        return false;
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
