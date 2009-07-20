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
package org.apache.vxquery.runtime.core;

import org.apache.vxquery.datamodel.atomic.BooleanValue;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.CallStackFrame;
import org.apache.vxquery.runtime.LocalRegisterAccessor;
import org.apache.vxquery.runtime.RegisterAllocator;
import org.apache.vxquery.runtime.base.AbstractRuntimeIterator;
import org.apache.vxquery.runtime.base.RuntimeIterator;

public class IfThenElseIterator extends AbstractRuntimeIterator {
    private final RuntimeIterator ci;
    private final RuntimeIterator ti;
    private final RuntimeIterator ei;
    private final LocalRegisterAccessor<Boolean> condition;

    public IfThenElseIterator(RegisterAllocator rAllocator, RuntimeIterator ci, RuntimeIterator ti, RuntimeIterator ei) {
        super(rAllocator);
        this.ci = ci;
        this.ti = ti;
        this.ei = ei;
        condition = new LocalRegisterAccessor<Boolean>(rAllocator.allocate(1));
    }

    @Override
    public void open(CallStackFrame frame) {
        condition.set(frame, null);
    }

    @Override
    public void close(CallStackFrame frame) {
        Boolean c = condition.get(frame);
        if (c != null) {
            (c ? ti : ei).close(frame);
        }
    }

    @Override
    public Object next(CallStackFrame frame) throws SystemException {
        Boolean c = condition.get(frame);
        if (c == null) {
            boolean cond = getConditionResult(frame);
            condition.set(frame, cond);
            (cond ? ti : ci).open(frame);
        }
        return (c ? ti : ei).next(frame);
    }

    @Override
    public Object evaluateEagerly(CallStackFrame frame) throws SystemException {
        RuntimeIterator ri = getConditionResult(frame) ? ti : ei;
        return ri.evaluateEagerly(frame);
    }

    private boolean getConditionResult(CallStackFrame frame) throws SystemException {
        return ((BooleanValue) ci.evaluateEagerly(frame)).getBooleanValue();
    }
}