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

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IPointableFactory;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;

import java.io.IOException;

public class ObjectPointable extends AbstractPointable {
    private static final int ENTRY_COUNT_SIZE = 4;
    private static final int SLOT_SIZE = 4;
    private final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
    private final UTF8StringPointable key1 = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
    private final SequenceBuilder sb = new SequenceBuilder();
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

    public int getEntryCount() {
        return getEntryCount(bytes, start);
    }

    public void getKeys(IPointable result)  throws SystemException{
        try {
            abvs.reset();
            sb.reset(abvs);
            int dataAreaOffset = getDataAreaOffset(bytes, start);
            int entryCount = getEntryCount();
            int s;
            for (int i = 0; i < entryCount; i++) {
                s = dataAreaOffset + getRelativeEntryStartOffset(i);
                key1.set(bytes, s, getKeyLength(bytes, s)+2);
                sb.addItem(key1);
            }
            sb.finish();
            result.set(abvs);
        }catch (IOException e){
            throw new SystemException(ErrorCode.SYSE0001);
        }
    }

    public void getValue(TaggedValuePointable key,IPointable pointer) {
        int dataAreaOffset = getDataAreaOffset(bytes, start);
        int entryCount = getEntryCount();
        int s,l;
        for (int i = 0; i < entryCount; i++) {
            s = dataAreaOffset + getRelativeEntryStartOffset(i);
            l=getKeyLength(bytes, s)+2;
            key1.set(bytes, s, l);
            if(key1.compareTo(key)==0){
                pointer.set(bytes,s+l,getEntryLength(i)-l);
                break;
            }else {
                //Todo: throw an exception
            }
        }
    }

    private int getRelativeEntryStartOffset(int idx) {
        return idx == 0 ? 0 : getSlotValue(bytes, start, idx - 1);
    }

    private static int getSlotValue(byte[] bytes, int start, int idx) {
        return IntegerPointable.getInteger(bytes, getSlotArrayOffset(start) + idx * SLOT_SIZE);
    }

    private static int getEntryCount(byte[] bytes, int start) {
        return IntegerPointable.getInteger(bytes, start);
    }

    private static int getKeyLength(byte[] b, int s) {
        return ((b[s] & 0xff) << 8) + ((b[s + 1] & 0xff) << 0);
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
