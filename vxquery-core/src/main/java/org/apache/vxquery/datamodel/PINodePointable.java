package org.apache.vxquery.datamodel;

import edu.uci.ics.hyracks.data.std.api.AbstractPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;

/*
 * PI {
 *  NodeId nodeId?;
 *  StringPtr target
 *  StringPtr content;
 * }
 */
public class PINodePointable extends AbstractPointable {
    private static final int LOCAL_NODE_ID_SIZE = 4;
    private static final int TARGET_SIZE = 4;

    public int getLocalNodeId(NodeTreePointable nodeTree) {
        return nodeTree.nodeIdExists() ? IntegerPointable.getInteger(bytes, getLocalNodeIdOffset()) : -1;
    }

    public int getTargetCode(NodeTreePointable nodeTree) {
        return IntegerPointable.getInteger(bytes, getTargetOffset(nodeTree));
    }

    public int getContentCode(NodeTreePointable nodeTree) {
        return IntegerPointable.getInteger(bytes, getContentOffset(nodeTree));
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

    private int getTargetSize() {
        return TARGET_SIZE;
    }

    private int getContentOffset(NodeTreePointable nodeTree) {
        return getTargetOffset(nodeTree) + getTargetSize();
    }
}