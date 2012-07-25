package org.apache.vxquery.datamodel.builders.nodes;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.values.ValueTag;

import edu.uci.ics.hyracks.data.std.api.IValueReference;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class AttributeNodeBuilder extends AbstractNodeBuilder {
    private DataOutput out;

    @Override
    public void reset(ArrayBackedValueStorage abvs) throws IOException {
        out = abvs.getDataOutput();
        out.write(ValueTag.ATTRIBUTE_NODE_TAG);
    }

    @Override
    public void finish() throws IOException {
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

    public void setValue(IValueReference value) throws IOException {
        out.write(value.getByteArray(), value.getStartOffset(), value.getLength());
    }
}