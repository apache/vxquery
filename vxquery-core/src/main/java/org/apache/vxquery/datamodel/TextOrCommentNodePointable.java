package org.apache.vxquery.datamodel;

import edu.uci.ics.hyracks.data.std.api.AbstractPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;

/*
 * Text | Comment {
 *  NodeId nodeId?;
 *  TaggedValue value;
 * }
 */
public class TextOrCommentNodePointable extends AbstractPointable {
    private static final int LOCAL_NODE_ID_SIZE = 4;

    public int getLocalNodeId(NodeTreePointable nodeTree) {
        return nodeTree.nodeIdExists() ? IntegerPointable.getInteger(bytes, getLocalNodeIdOffset()) : -1;
    }

    public void getValue(NodeTreePointable nodeTree, TaggedValuePointable value) {
        value.set(bytes, getValueOffset(nodeTree), getValueSize(nodeTree));
    }

    private int getLocalNodeIdOffset() {
        return start;
    }

    private int getLocalNodeIdSize(NodeTreePointable nodeTree) {
        return nodeTree.nodeIdExists() ? LOCAL_NODE_ID_SIZE : 0;
    }

    private int getValueOffset(NodeTreePointable nodeTree) {
        return getLocalNodeIdOffset() + getLocalNodeIdSize(nodeTree);
    }

    private int getValueSize(NodeTreePointable nodeTree) {
        return length - (getValueOffset(nodeTree) - start);
    }
}