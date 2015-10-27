/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.vxquery.datamodel.accessors;

import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IPointableFactory;

public class PointablePool {
    private final Map<Class<? extends IPointable>, IPointableFactory> pfMap;

    private final Map<Class<? extends IPointable>, PointableCache<? extends IPointable>> pcMap;

    PointablePool() {
        pfMap = new HashMap<Class<? extends IPointable>, IPointableFactory>();
        pcMap = new HashMap<Class<? extends IPointable>, PointableCache<? extends IPointable>>();
    }

    <T extends IPointable> void register(Class<T> klass, IPointableFactory factory) {
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
