package org.apache.vxquery.datamodel.builders.jsonItem;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.util.GrowableIntArray;

public class ArrayBuilder {
    private final GrowableIntArray slots = new GrowableIntArray();
    private final ArrayBackedValueStorage dataArea = new ArrayBackedValueStorage();
    private IMutableValueStorage mvs;

    public ArrayBuilder() {
    }

    public void reset(IMutableValueStorage mvs) {
        this.mvs = mvs;
        slots.clear();
        dataArea.reset();
    }

    public void addItem(IValueReference p) throws IOException {
        dataArea.getDataOutput().write(p.getByteArray(), p.getStartOffset(), p.getLength());
        slots.append(dataArea.getLength());
    }

    public void finish() throws IOException {
        DataOutput out = mvs.getDataOutput();
        out.write(ValueTag.ARRAY_TAG);
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
