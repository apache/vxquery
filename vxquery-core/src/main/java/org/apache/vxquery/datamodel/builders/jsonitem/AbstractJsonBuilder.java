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
package org.apache.vxquery.datamodel.builders.jsonitem;

import java.io.IOException;

import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.builders.base.AbstractBuilder;
import org.apache.vxquery.datamodel.builders.base.IBuilder;
import org.apache.vxquery.util.GrowableIntArray;

public abstract class AbstractJsonBuilder extends AbstractBuilder implements IBuilder {
    final GrowableIntArray slots = new GrowableIntArray();
    final ArrayBackedValueStorage dataArea = new ArrayBackedValueStorage();

    @Override
    public void reset(IMutableValueStorage mvs) throws IOException {
        super.reset(mvs);
        slots.clear();
        dataArea.reset();
    }

    @Override
    public void finish() throws IOException {
        int size = slots.getSize();
        out.writeInt(size);
        if (size > 0) {
            int[] slotArray = slots.getArray();
            for (int i = 0; i < size; ++i) {
                out.writeInt(slotArray[i]);
            }
            out.write(dataArea.getByteArray(), dataArea.getStartOffset(), dataArea.getLength());
        }
    }
}
