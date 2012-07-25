package org.apache.vxquery.datamodel.accessors;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.data.std.api.IPointable;

public class PointableCache<T extends IPointable> {
    private final List<T> list;

    public PointableCache() {
        list = new ArrayList<T>();
    }

    public T takeOne() {
        if (list.isEmpty()) {
            return null;
        }
        return list.remove(list.size() - 1);
    }

    public void giveBack(T pointable) {
        list.add(pointable);
    }
}