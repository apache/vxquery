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
package org.apache.vxquery.datamodel.accessors;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IPointableFactory;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;

public class SequencePointable extends AbstractPointable {
    private static final int ENTRY_COUNT_SIZE = IntegerPointable.TYPE_TRAITS.getFixedLength();
    private static final int SLOT_SIZE = IntegerPointable.TYPE_TRAITS.getFixedLength();
    public static final IPointableFactory FACTORY = new IPointableFactory() {
        private static final long serialVersionUID = 1L;

        @Override
        public ITypeTraits getTypeTraits() {
            return VoidPointable.TYPE_TRAITS;
        }

        @Override
        public IPointable createPointable() {
            return new SequencePointable();
        }
    };

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