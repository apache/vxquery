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
package org.apache.vxquery.datamodel.builders.sequence;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.util.GrowableIntArray;

import edu.uci.ics.hyracks.data.std.api.IMutableValueStorage;
import edu.uci.ics.hyracks.data.std.api.IValueReference;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class SequenceBuilder {
    private final GrowableIntArray slots = new GrowableIntArray();
    private final ArrayBackedValueStorage dataArea = new ArrayBackedValueStorage();
    private IMutableValueStorage mvs;

    public SequenceBuilder() {
    }

    public void reset(IMutableValueStorage mvs) {
        this.mvs = mvs;
        slots.clear();
        dataArea.reset();
    }

    public void addItem(IValueReference p) throws IOException {
        dataArea.getDataOutput().write(p.getByteArray(), p.getStartOffset(), p.getLength());
        slots.append(dataArea.getLength());
    }

    public void finish() throws IOException {
        DataOutput out = mvs.getDataOutput();
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
}