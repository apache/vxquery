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
package org.apache.vxquery.runtime.functions;

import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.runtime.CallStackFrame;
import org.apache.vxquery.runtime.LocalRegisterAccessor;
import org.apache.vxquery.runtime.RegisterAllocator;
import org.apache.vxquery.runtime.base.AbstractLazilyEvaluatedFunctionIterator;
import org.apache.vxquery.runtime.base.RuntimeIterator;

public class ConcatenateIterator extends AbstractLazilyEvaluatedFunctionIterator {
    private final LocalRegisterAccessor<Integer> index;

    public ConcatenateIterator(RegisterAllocator rAllocator, Function fn, RuntimeIterator[] arguments,
            StaticContext ctx) {
        super(rAllocator, fn, arguments, ctx);
        index = new LocalRegisterAccessor<Integer>(rAllocator.allocate(1));
    }

    @Override
    public void open(CallStackFrame frame) {
        for (RuntimeIterator i : arguments) {
            i.open(frame);
        }
        index.set(frame, 0);
    }

    @Override
    public Object next(CallStackFrame frame) throws SystemException {
        int i = index.get(frame);
        while (i < arguments.length) {
            Object o = arguments[i].next(frame);
            if (o != null) {
                return o;
            }
            index.set(frame, ++i);
        }
        return null;
    }

    @Override
    public void close(CallStackFrame frame) {
        for (RuntimeIterator i : arguments) {
            i.close(frame);
        }
    }
}