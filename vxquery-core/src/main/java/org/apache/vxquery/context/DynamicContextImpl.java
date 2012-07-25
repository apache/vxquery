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
package org.apache.vxquery.context;

import java.util.HashMap;
import java.util.Map;

import javax.xml.namespace.QName;

import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.api.IValueReference;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class DynamicContextImpl implements DynamicContext {
    private StaticContext sCtx;

    private final Map<QName, ArrayBackedValueStorage> variables;

    private final ArrayBackedValueStorage currentDateTime;

    public DynamicContextImpl(StaticContext sCtx) {
        this.sCtx = sCtx;
        currentDateTime = new ArrayBackedValueStorage();
        variables = new HashMap<QName, ArrayBackedValueStorage>();
    }

    public IDynamicContextFactory createFactory() {
        return DynamicContextImplFactory.createInstance(this);
    }

    @Override
    public StaticContext getStaticContext() {
        return sCtx;
    }

    @Override
    public void setCurrentDateTime(IValueReference value) {
        currentDateTime.assign(value);
    }

    @Override
    public void getCurrentDateTime(IPointable value) {
        value.set(currentDateTime);
    }

    @Override
    public void bindVariable(QName var, IValueReference value) {
        ArrayBackedValueStorage abvs = variables.get(var);
        if (abvs == null) {
            abvs = new ArrayBackedValueStorage();
            variables.put(var, abvs);
        }
        abvs.assign(value);
    }

    @Override
    public void lookupVariable(QName var, IPointable value) {
        ArrayBackedValueStorage abvs = variables.get(var);
        if (abvs == null) {
            value.set(null, -1, -1);
        } else {
            value.set(abvs);
        }
    }

    Map<QName, ArrayBackedValueStorage> getVariableMap() {
        return variables;
    }
}