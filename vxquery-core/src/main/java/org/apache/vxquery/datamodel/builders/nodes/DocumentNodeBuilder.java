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

import java.io.IOException;

import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.util.GrowableIntArray;

public class DocumentNodeBuilder extends AbstractNodeBuilder {
    private final GrowableIntArray childrenSlots;

    private final ArrayBackedValueStorage childrenDataArea;

    private int childrenCount;

    public DocumentNodeBuilder() {
        childrenSlots = new GrowableIntArray();
        childrenDataArea = new ArrayBackedValueStorage();
    }

    @Override
    public int getValueTag() {
        return ValueTag.DOCUMENT_NODE_TAG;
    }

    @Override
    public void finish() throws IOException {
    }

    public void setLocalNodeId(int localNodeId) throws IOException {
        out.writeInt(localNodeId);
    }

    public void startChildrenChunk() {
        childrenSlots.clear();
        childrenDataArea.reset();
    }

    public void startChild(AbstractNodeBuilder nb) throws IOException {
        nb.reset(childrenDataArea);
    }

    public void endChild(AbstractNodeBuilder nb) throws IOException {
        nb.finish();
        childrenSlots.append(childrenDataArea.getLength());
    }

    public void endChildrenChunk() throws IOException {
        childrenCount = childrenSlots.getSize();
        if (childrenCount > 0) {
            out.writeInt(childrenCount);
            int[] slotArray = childrenSlots.getArray();
            for (int i = 0; i < childrenCount; ++i) {
                int slot = slotArray[i];
                out.writeInt(slot);
            }
            out.write(childrenDataArea.getByteArray(), childrenDataArea.getStartOffset(), childrenDataArea.getLength());
        }
    }
}
