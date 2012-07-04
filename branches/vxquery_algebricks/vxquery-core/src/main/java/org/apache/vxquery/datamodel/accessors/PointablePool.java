package org.apache.vxquery.datamodel.accessors;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.api.IPointableFactory;

public class PointablePool {
    private final Map<Class<? extends IPointable>, IPointableFactory> pfMap;

    private final Map<Class<? extends IPointable>, PointableCache<? extends IPointable>> pcMap;

    public PointablePool() {
        pfMap = new HashMap<Class<? extends IPointable>, IPointableFactory>();
        pcMap = new HashMap<Class<? extends IPointable>, PointableCache<? extends IPointable>>();
    }

    public <T extends IPointable> void register(Class<T> klass, IPointableFactory factory) {
        pfMap.put(klass, factory);
        pcMap.put(klass, new PointableCache<T>());
    }

    @SuppressWarnings("unchecked")
    public <T extends IPointable> T takeOne(Class<T> klass) {
        PointableCache<T> pc = (PointableCache<T>) pcMap.get(klass);
        T p = pc.takeOne();
        if (p != null) {
            return p;
        }
        IPointableFactory pf = pfMap.get(klass);
        return (T) pf.createPointable();
    }

    @SuppressWarnings("unchecked")
    public <T extends IPointable> void giveBack(T pointable) {
        PointableCache<T> pc = (PointableCache<T>) pcMap.get(pointable.getClass());
        pc.giveBack(pointable);
    }
}