package org.apache.vxquery.datamodel.builders.jsonitem;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.builders.base.AbstractBuilder;
import org.apache.vxquery.datamodel.builders.base.IBuilder;
import org.apache.vxquery.util.GrowableIntArray;

public abstract class JsonAbstractBuilder extends AbstractBuilder implements IBuilder {
    final GrowableIntArray slots = new GrowableIntArray();
    final ArrayBackedValueStorage dataArea = new ArrayBackedValueStorage();
    protected DataOutput out;

    public void reset(IMutableValueStorage mvs) throws IOException {
        out = mvs.getDataOutput();
        out.write(getValueTag());
        slots.clear();
        dataArea.reset();
    }

    public void finish() throws IOException {
        int size = slots.getSize();
        out.writeInt(size);
        if (size > 0) {
            int[] slotArray = slots.getArray();
            for (int i = 0; i < size; ++i) {
                out.writeInt(slotArray[i]);
            }
            out.write(dataArea.getByteArray(), dataArea.getStartOffset(), dataArea.getLength());
        }
    }
}
