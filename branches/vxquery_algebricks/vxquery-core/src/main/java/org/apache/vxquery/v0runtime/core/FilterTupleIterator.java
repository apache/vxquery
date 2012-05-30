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
package org.apache.vxquery.v0runtime.core;

import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.v0datamodel.atomic.BooleanValue;
import org.apache.vxquery.v0runtime.CallStackFrame;
import org.apache.vxquery.v0runtime.LocalRegisterAccessor;
import org.apache.vxquery.v0runtime.RegisterAllocator;
import org.apache.vxquery.v0runtime.base.AbstractUnaryTupleIterator;
import org.apache.vxquery.v0runtime.base.RuntimeIterator;
import org.apache.vxquery.v0runtime.base.TupleIterator;

public class FilterTupleIterator extends AbstractUnaryTupleIterator {
    private final RuntimeIterator condition;
    private final LocalRegisterAccessor<Boolean> done;

    public FilterTupleIterator(RegisterAllocator rAllocator, TupleIterator input, RuntimeIterator condition) {
        super(rAllocator, input);
        this.condition = condition;
        done = new LocalRegisterAccessor<Boolean>(rAllocator.allocate(1));
    }

    protected final void setDone(CallStackFrame frame, boolean done) {

    }

    @Override
    public boolean next(CallStackFrame frame) throws SystemException {
        while (true) {
            if (done.get(frame)) {
                return false;
            }
            boolean status = input.next(frame);
            if (status) {
                BooleanValue bool = (BooleanValue) condition.evaluateEagerly(frame);
                if (bool.getBooleanValue()) {
                    return true;
                }
            } else {
                done.set(frame, true);
            }
        }
    }

    @Override
    public void open(CallStackFrame frame) {
        super.open(frame);
        done.set(frame, false);
    }
}