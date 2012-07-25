package org.apache.vxquery.datamodel.builders.nodes;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.nodes.ElementNodePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.util.GrowableIntArray;

import edu.uci.ics.hyracks.data.std.primitive.BytePointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;

public class ElementNodeBuilder extends AbstractNodeBuilder {
    private final GrowableIntArray attrSlots;

    private final ArrayBackedValueStorage attrDataArea;

    private final GrowableIntArray childrenSlots;

    private final ArrayBackedValueStorage childrenDataArea;

    private ArrayBackedValueStorage abvs;

    private DataOutput out;

    private int headerOffset;

    private int nsChunkStart;

    private int nsCount;

    private int attrCount;

    private int childrenCount;

    public ElementNodeBuilder() {
        attrSlots = new GrowableIntArray();
        attrDataArea = new ArrayBackedValueStorage();
        childrenSlots = new GrowableIntArray();
        childrenDataArea = new ArrayBackedValueStorage();
    }

    @Override
    public void reset(ArrayBackedValueStorage abvs) throws IOException {
        this.abvs = abvs;
        out = abvs.getDataOutput();
        out.write(ValueTag.ELEMENT_NODE_TAG);
        headerOffset = abvs.getLength();
        out.write(0);
    }

    @Override
    public void finish() throws IOException {
        byte header = 0;
        if (nsCount > 0) {
            header |= ElementNodePointable.NS_CHUNK_EXISTS_MASK;
        }
        if (attrCount > 0) {
            header |= ElementNodePointable.ATTRIBUTES_CHUNK_EXISTS_MASK;
        }
        if (childrenCount > 0) {
            header |= ElementNodePointable.CHILDREN_CHUNK_EXISTS_MASK;
        }
        BytePointable.setByte(abvs.getByteArray(), headerOffset, header);
    }

    public void setName(int uriCode, int localNameCode, int prefixCode) throws IOException {
        out.writeInt(prefixCode);
        out.writeInt(uriCode);
        out.writeInt(localNameCode);
    }

    public void setType(int uriCode, int localNameCode, int prefixCode) throws IOException {
        out.writeInt(prefixCode);
        out.writeInt(uriCode);
        out.writeInt(localNameCode);
    }

    public void setLocalNodeId(int localNodeId) throws IOException {
        out.writeInt(localNodeId);
    }

    public void startNamespaceChunk() {
        nsChunkStart = abvs.getLength();
        nsCount = 0;
    }

    public void addNamespace(int prefixCode, int uriCode) throws IOException {
        if (nsCount == 0) {
            out.writeInt(0);
        }
        out.writeInt(prefixCode);
        out.writeInt(uriCode);
        ++nsCount;
    }

    public void endNamespaceChunk() {
        byte[] bytes = abvs.getByteArray();
        IntegerPointable.setInteger(bytes, nsChunkStart, nsCount);
    }

    public void startAttributeChunk() {
        attrSlots.clear();
        attrDataArea.reset();
    }

    public void startAttribute(AttributeNodeBuilder attrb) throws IOException {
        attrb.reset(attrDataArea);
    }

    public void endAttribute(AttributeNodeBuilder attrb) throws IOException {
        attrb.finish();
        attrSlots.append(attrDataArea.getLength());
    }

    public void endAttributeChunk() throws IOException {
        attrCount = attrSlots.getSize();
        if (attrCount > 0) {
            out.writeInt(attrCount);
            int[] slotArray = attrSlots.getArray();
            for (int i = 0; i < attrCount; ++i) {
                int slot = slotArray[i];
                out.writeInt(slot);
            }
            out.write(attrDataArea.getByteArray(), attrDataArea.getStartOffset(), attrDataArea.getLength());
        }
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