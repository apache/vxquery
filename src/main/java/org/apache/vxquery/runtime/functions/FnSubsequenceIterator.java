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
import org.apache.vxquery.runtime.RuntimeUtils;
import org.apache.vxquery.runtime.base.AbstractLazilyEvaluatedFunctionIterator;
import org.apache.vxquery.runtime.base.RuntimeIterator;

public class FnSubsequenceIterator extends AbstractLazilyEvaluatedFunctionIterator {
    private final LocalRegisterAccessor<Integer> remaining;

    public FnSubsequenceIterator(RegisterAllocator rAllocator, Function fn, RuntimeIterator[] arguments,
            StaticContext ctx) {
        super(rAllocator, fn, arguments, ctx);
        remaining = new LocalRegisterAccessor<Integer>(rAllocator.allocate(1));
    }

    @Override
    public void open(CallStackFrame frame) {
        for (RuntimeIterator i : arguments) {
            i.open(frame);
        }
    }

    @Override
    public Object next(CallStackFrame frame) throws SystemException {
        if (remaining.get(frame) == null) {
            int start = (int) Math.round(RuntimeUtils.fetchNumericItemEagerly(arguments[1], frame).getDoubleValue());
            while (start > 1) {
                arguments[0].next(frame);
                start--;
            }
        
            if (arguments.length > 2) {
                int length = (int) Math.round(RuntimeUtils.fetchNumericItemEagerly(arguments[2], frame).getDoubleValue());
                remaining.set(frame, length < 0 ? 0 : length);
            } else {
                remaining.set(frame, -1);
            }
        }

        int remain = remaining.get(frame);
        if (remain == 0) {
            return null;
        }
        Object o = arguments[0].next(frame);
        if (o == null) {
            remain = 0;
        } else {
            --remain;                
        }
        remaining.set(frame, remain);
        return o;
    }

    @Override
    public void close(CallStackFrame frame) {
        for (RuntimeIterator i : arguments) {
            i.close(frame);
        }
    }
}