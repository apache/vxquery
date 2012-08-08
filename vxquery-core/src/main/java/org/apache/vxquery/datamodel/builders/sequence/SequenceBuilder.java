package org.apache.vxquery.datamodel.builders.sequence;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.util.GrowableIntArray;

import edu.uci.ics.hyracks.data.std.api.IMutableValueStorage;
import edu.uci.ics.hyracks.data.std.api.IValueReference;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class SequenceBuilder {
    private final GrowableIntArray slots = new GrowableIntArray();
    private final ArrayBackedValueStorage dataArea = new ArrayBackedValueStorage();
    private IMutableValueStorage mvs;

    public SequenceBuilder() {
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
        if (slots.getSize() != 1) {
            out.write(ValueTag.SEQUENCE_TAG);
            int size = slots.getSize();
            out.writeInt(size);
            if (size > 0) {
                int[] slotArray = slots.getArray();
                for (int i = 0; i < size; ++i) {
                    out.writeInt(slotArray[i]);
                }
                out.write(dataArea.getByteArray(), dataArea.getStartOffset(), dataArea.getLength());
            }
        } else {
            out.write(dataArea.getByteArray(), dataArea.getStartOffset(), dataArea.getLength());
        }
    }
}