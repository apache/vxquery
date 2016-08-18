package org.apache.vxquery.datamodel.accessors;

import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

import java.util.ArrayList;
import java.util.List;

public class ArrayBackedValueStoragePool {
    private final List<ArrayBackedValueStorage> abvsList;

    public ArrayBackedValueStoragePool() {
        abvsList = new ArrayList<>();
    }

    public ArrayBackedValueStorage takeOne() {
        if (abvsList.isEmpty()) {
            return new ArrayBackedValueStorage();
        }
        return abvsList.remove(abvsList.size() - 1);
    }

    public void giveBack(ArrayBackedValueStorage abvs) {
        abvsList.add(abvs);
    }
}
