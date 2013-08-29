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
import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.nodes.NodeTreePointable;
import org.apache.vxquery.datamodel.values.ValueTag;

import edu.uci.ics.hyracks.data.std.api.IMutableValueStorage;

public class NodeSubTreeBuilder extends AbstractNodeBuilder {
    private DataOutput mainOut;

    @Override
    public void reset(IMutableValueStorage mvs) throws IOException {
        mainOut = mvs.getDataOutput();
        mainOut.write(ValueTag.NODE_TREE_TAG);
    }

    @Override
    public void finish() throws IOException {
    }

    public void setChildNode(NodeTreePointable ntp, TaggedValuePointable itemTvp) throws IOException {
        boolean hasDictionary = ntp.dictionaryExists() && hasDictionary(itemTvp.getTag());
        byte header = (byte) (hasDictionary ? NodeTreePointable.HEADER_DICTIONARY_EXISTS_MASK : 0);
        // TODO add all header flags to this setting.
        boolean hasNodeIds = ntp.nodeIdExists();
        if (hasNodeIds) {
            header |= NodeTreePointable.HEADER_NODEID_EXISTS_MASK;
        }
        mainOut.write(header);
        if (hasNodeIds) {
            mainOut.writeInt(ntp.getRootNodeId());
        }
        if (hasDictionary) {
            mainOut.write(ntp.getByteArray(), ntp.getDictionaryOffset(), ntp.getDictionarySize());
        }
        mainOut.write(itemTvp.getByteArray(), itemTvp.getStartOffset(), itemTvp.getLength());
    }

    private boolean hasDictionary(byte tag) {
        switch (tag) {
            case ValueTag.ATTRIBUTE_NODE_TAG:
            case ValueTag.DOCUMENT_NODE_TAG:
            case ValueTag.ELEMENT_NODE_TAG:
                return true;
        }
        return false;
    }


}