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
package org.apache.vxquery.runtime.base;

import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.CallStackFrame;
import org.apache.vxquery.runtime.LocalRegisterAccessor;
import org.apache.vxquery.runtime.RegisterAllocator;

public abstract class AbstractEagerlyEvaluatedIterator extends AbstractRuntimeIterator {
    private final LocalRegisterAccessor<Boolean> done;

    public AbstractEagerlyEvaluatedIterator(RegisterAllocator rAllocator) {
        super(rAllocator);
        done = new LocalRegisterAccessor<Boolean>(rAllocator.allocate(1));
    }

    @Override
    public final void open(CallStackFrame frame) {
        done.set(frame, false);
    }

    @Override
    public final void close(CallStackFrame frame) {
    }

    @Override
    public final Object next(CallStackFrame frame) throws SystemException {
        if (done.get(frame)) {
            return null;
        }
        done.set(frame, true);
        return evaluateEagerly(frame);
    }
}