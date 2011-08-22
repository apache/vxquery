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

import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.CallStackFrame;
import org.apache.vxquery.runtime.LocalRegisterAccessor;
import org.apache.vxquery.runtime.RegisterAllocator;
import org.apache.vxquery.runtime.base.AbstractLazilyEvaluatedIterator;
import org.apache.vxquery.runtime.base.RuntimeIterator;
import org.apache.vxquery.runtime.base.TupleIterator;

public class FLWORIterator extends AbstractLazilyEvaluatedIterator {
    private final TupleIterator ti;
    private final RuntimeIterator ri;
    private final LocalRegisterAccessor<Boolean> riOpen;
    private final LocalRegisterAccessor<Boolean> done;

    public FLWORIterator(RegisterAllocator rAllocator, TupleIterator ti, RuntimeIterator ri) {
        super(rAllocator);
        this.ti = ti;
        this.ri = ri;
        riOpen = new LocalRegisterAccessor<Boolean>(rAllocator.allocate(1));
        done = new LocalRegisterAccessor<Boolean>(rAllocator.allocate(1));
    }

    @Override
    public void close(CallStackFrame frame) {
        if (riOpen.get(frame)) {
            ri.close(frame);
        }
        ti.close(frame);
    }

    @Override
    public Object next(CallStackFrame frame) throws SystemException {
        while (true) {
            if (done.get(frame)) {
                return null;
            }
            if (!riOpen.get(frame)) {
                boolean status = ti.next(frame);
                if (status) {
                    ri.open(frame);
                    riOpen.set(frame, true);
                } else {
                    done.set(frame, true);
                    return null;
                }
            }
            Object o = ri.next(frame);
            if (o != null) {
                return o;
            }
            ri.close(frame);
            riOpen.set(frame, false);
        }
    }

    @Override
    public void open(CallStackFrame frame) {
        done.set(frame, false);
        riOpen.set(frame, false);
        ti.open(frame);
    }
}