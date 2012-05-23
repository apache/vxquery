package org.apache.vxquery.datamodel;

import edu.uci.ics.hyracks.data.std.api.AbstractPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;

/*
 * Attribute {
 *  NamePtr namePtr;
 *  NamePtr typePtr?;
 *  NodeId nodeId?;
 *  TaggedValue value;
 * }
 */
public class AttributeNodePointable extends AbstractPointable {
    private static final int LOCAL_NODE_ID_SIZE = 4;

    public void getName(QNamePointable name) {
        name.set(bytes, getNameOffset(), getNameSize());
    }

    public void getTypeName(NodeTreePointable nodeTree, QNamePointable typeName) {
        if (nodeTree.typeExists()) {
            typeName.set(bytes, getTypeOffset(), getTypeSize(nodeTree));
        } else {
            typeName.set(null, -1, -1);
        }
    }

    public int getLocalNodeId(NodeTreePointable nodeTree) {
        return nodeTree.nodeIdExists() ? IntegerPointable.getInteger(bytes, getLocalNodeIdOffset(nodeTree)) : -1;
    }

    public void getValue(NodeTreePointable nodeTree, TaggedValuePointable value) {
        value.set(bytes, getValueOffset(nodeTree), getValueSize(nodeTree));
    }

    private int getNameOffset() {
        return start;
    }

    private int getNameSize() {
        return QNamePointable.SIZE;
    }

    private int getTypeOffset() {
        return getNameOffset() + getNameSize();
    }

    private int getTypeSize(NodeTreePointable nodeTree) {
        return nodeTree.typeExists() ? QNamePointable.SIZE : 0;
    }

    private int getLocalNodeIdOffset(NodeTreePointable nodeTree) {
        return getTypeOffset() + getTypeSize(nodeTree);
    }

    private int getLocalNodeIdSize(NodeTreePointable nodeTree) {
        return nodeTree.nodeIdExists() ? LOCAL_NODE_ID_SIZE : 0;
    }

    private int getValueOffset(NodeTreePointable nodeTree) {
        return getLocalNodeIdOffset(nodeTree) + getLocalNodeIdSize(nodeTree);
    }

    private int getValueSize(NodeTreePointable nodeTree) {
        return length - (getValueOffset(nodeTree) - start);
    }
}