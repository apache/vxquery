package org.apache.vxquery.datamodel.builders.jsonitem;

import java.io.IOException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.vxquery.datamodel.values.ValueTag;

public class ArrayBuilder extends AbstractJsonBuilder {

    public void addItem(IValueReference p) throws IOException {
        dataArea.getDataOutput().write(p.getByteArray(), p.getStartOffset(), p.getLength());
        slots.append(dataArea.getLength());
    }

    @Override
    public int getValueTag() {
        return ValueTag.ARRAY_TAG;
    }

}
