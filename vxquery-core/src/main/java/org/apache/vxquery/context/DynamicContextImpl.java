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

import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.atomic.XSDateTimePointable;

public class DynamicContextImpl implements DynamicContext {
    private StaticContext sCtx;

    private final Map<QName, ArrayBackedValueStorage> variables;

    private byte[] currentDateTime;

    public DynamicContextImpl(StaticContext sCtx) {
        this.sCtx = sCtx;
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
        if (currentDateTime == null) {
            currentDateTime = new byte[value.getLength()];
        }
        System.arraycopy(value.getByteArray(), value.getStartOffset(), currentDateTime, 0, value.getLength());
    }

    @Override
    public void getCurrentDateTime(IPointable value) {
        if (currentDateTime == null) {
            // if not set, get it from the JVM
            final int dtLen = XSDateTimePointable.TYPE_TRAITS.getFixedLength();
            currentDateTime = new byte[dtLen];
            XSDateTimePointable datetimep = new XSDateTimePointable();
            datetimep.set(currentDateTime, 0, dtLen);
            datetimep.setCurrentDateTime();
        }
        value.set(currentDateTime, 0, currentDateTime.length);
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
