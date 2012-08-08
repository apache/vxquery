package org.apache.vxquery.datamodel.builders.nodes;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.util.GrowableIntArray;

import edu.uci.ics.hyracks.data.std.api.IMutableValueStorage;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class DocumentNodeBuilder extends AbstractNodeBuilder {
    private final GrowableIntArray childrenSlots;

    private final ArrayBackedValueStorage childrenDataArea;

    private DataOutput out;

    private int childrenCount;

    public DocumentNodeBuilder() {
        childrenSlots = new GrowableIntArray();
        childrenDataArea = new ArrayBackedValueStorage();
    }

    @Override
    public void reset(IMutableValueStorage mvs) throws IOException {
        out = mvs.getDataOutput();
        out.write(ValueTag.DOCUMENT_NODE_TAG);
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