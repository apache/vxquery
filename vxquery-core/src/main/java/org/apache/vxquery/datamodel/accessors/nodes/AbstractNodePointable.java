package org.apache.vxquery.datamodel.accessors.nodes;

import edu.uci.ics.hyracks.data.std.api.AbstractPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;

public abstract class AbstractNodePointable extends AbstractPointable {
     public int getLocalNodeId(NodeTreePointable nodeTree) {
        return nodeTree.nodeIdExists() ? IntegerPointable.getInteger(bytes, getLocalNodeIdOffset(nodeTree)) : -1;
    }

    abstract protected int getLocalNodeIdOffset(NodeTreePointable nodeTree);
}
