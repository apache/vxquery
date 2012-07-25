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

    public void getTarget(NodeTreePointable nodeTree, UTF8StringPointable target) {
        target.set(bytes, getTargetOffset(nodeTree), getTargetSize(nodeTree));
    }

    public void getContent(NodeTreePointable nodeTree, UTF8StringPointable content) {
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