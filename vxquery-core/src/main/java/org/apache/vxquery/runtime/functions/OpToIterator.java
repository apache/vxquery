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

import java.math.BigInteger;

import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.runtime.CallStackFrame;
import org.apache.vxquery.runtime.LocalRegisterAccessor;
import org.apache.vxquery.runtime.RegisterAllocator;
import org.apache.vxquery.runtime.RegisterSet;
import org.apache.vxquery.runtime.base.AbstractLazilyEvaluatedFunctionIterator;
import org.apache.vxquery.runtime.base.RuntimeIterator;
import org.apache.vxquery.v0datamodel.atomic.NumericValue;

public class OpToIterator extends AbstractLazilyEvaluatedFunctionIterator {
    private final LocalRegisterAccessor<Boolean> first;
    private final LocalRegisterAccessor<BigInteger> value;
    private final LocalRegisterAccessor<BigInteger> end;

    public OpToIterator(RegisterAllocator rAllocator, Function fn, RuntimeIterator[] arguments, StaticContext ctx) {
        super(rAllocator, fn, arguments, ctx);
        first = new LocalRegisterAccessor<Boolean>(rAllocator.allocate(1));
        value = new LocalRegisterAccessor<BigInteger>(rAllocator.allocate(1));
        end = new LocalRegisterAccessor<BigInteger>(rAllocator.allocate(1));
    }

    @Override
    public void open(CallStackFrame frame) {
        first.set(frame, true);
    }

    @Override
    public Object next(CallStackFrame frame) throws SystemException {
        RegisterSet regs = frame.getLocalRegisters();
        if (first.get(regs)) {
            first.set(regs, false);
            NumericValue start = (NumericValue) arguments[0].evaluateEagerly(frame);
            value.set(regs, start.getIntegerValue());
            NumericValue last = (NumericValue) arguments[1].evaluateEagerly(frame);
            end.set(regs, last.getIntegerValue());
        }
        if (end.get(regs).compareTo(value.get(regs)) < 0) {
            return null;
        }
        BigInteger temp = value.get(regs);
        value.set(regs, value.get(regs).add(BigInteger.ONE));
        return frame.getRuntimeControlBlock().getAtomicValueFactory().createInteger(temp);
    }

    @Override
    public void close(CallStackFrame frame) {
    }
}