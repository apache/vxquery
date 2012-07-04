package org.apache.vxquery.datamodel.accessors.nodes;

import org.apache.vxquery.datamodel.accessors.SequencePointable;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.api.AbstractPointable;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.api.IPointableFactory;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;

/*
 * Document {
 *  NodeId nodeId?;
 *  Sequence content;
 * }
 */
public class DocumentNodePointable extends AbstractPointable {
    private static final int LOCAL_NODE_ID_SIZE = 4;
    public static final IPointableFactory FACTORY = new IPointableFactory() {
        private static final long serialVersionUID = 1L;

        @Override
        public ITypeTraits getTypeTraits() {
            return VoidPointable.TYPE_TRAITS;
        }

        @Override
        public IPointable createPointable() {
            return new DocumentNodePointable();
        }
    };

    public int getLocalNodeId(NodeTreePointable nodeTree) {
        return nodeTree.nodeIdExists() ? IntegerPointable.getInteger(bytes, getLocalNodeIdOffset()) : -1;
    }

    public void getContent(NodeTreePointable nodeTree, SequencePointable content) {
        content.set(bytes, getContentOffset(nodeTree), getContentSize(nodeTree));
    }

    private int getLocalNodeIdOffset() {
        return start;
    }

    private int getLocalNodeIdSize(NodeTreePointable nodeTree) {
        return nodeTree.nodeIdExists() ? LOCAL_NODE_ID_SIZE : 0;
    }

    private int getContentOffset(NodeTreePointable nodeTree) {
        return getLocalNodeIdOffset() + getLocalNodeIdSize(nodeTree);
    }

    private int getContentSize(NodeTreePointable nodeTree) {
        return length - (getContentOffset(nodeTree) - start);
    }
}