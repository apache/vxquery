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
package org.apache.vxquery.datamodel.accessors.nodes;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.api.AbstractPointable;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.api.IPointableFactory;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;

/*
 * PI {
 *  NodeId nodeId?;
 *  String target
 *  String content;
 * }
 */
public class PINodePointable extends AbstractPointable {
    private static final int LOCAL_NODE_ID_SIZE = 4;
    public static final IPointableFactory FACTORY = new IPointableFactory() {
        private static final long serialVersionUID = 1L;

        @Override
        public ITypeTraits getTypeTraits() {
            return VoidPointable.TYPE_TRAITS;
        }

        @Override
        public IPointable createPointable() {
            return new PINodePointable();
        }
    };

    public int getLocalNodeId(NodeTreePointable nodeTree) {
        return nodeTree.nodeIdExists() ? IntegerPointable.getInteger(bytes, getLocalNodeIdOffset()) : -1;
    }

    public void getTarget(NodeTreePointable nodeTree, IPointable target) {
        target.set(bytes, getTargetOffset(nodeTree), getTargetSize(nodeTree));
    }

    public void getContent(NodeTreePointable nodeTree, IPointable content) {
        content.set(bytes, getContentOffset(nodeTree), getContentSize(nodeTree));
    }

    private int getLocalNodeIdOffset() {
        return start;
    }

    private int getLocalNodeIdSize(NodeTreePointable nodeTree) {
        return nodeTree.nodeIdExists() ? LOCAL_NODE_ID_SIZE : 0;
    }

    private int getTargetOffset(NodeTreePointable nodeTree) {
        return getLocalNodeIdOffset() + getLocalNodeIdSize(nodeTree);
    }

    private int getTargetSize(NodeTreePointable nodeTree) {
        return UTF8StringPointable.getUTFLength(bytes, getTargetOffset(nodeTree)) + 2;
    }

    private int getContentOffset(NodeTreePointable nodeTree) {
        return getTargetOffset(nodeTree) + getTargetSize(nodeTree);
    }

    private int getContentSize(NodeTreePointable nodeTree) {
        return UTF8StringPointable.getUTFLength(bytes, getContentOffset(nodeTree)) + 2;
    }
}