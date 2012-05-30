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
package org.apache.vxquery.v0runtime.functions;

import java.math.BigInteger;

import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.v0datamodel.XDMValue;
import org.apache.vxquery.v0datamodel.atomic.NumericValue;
import org.apache.vxquery.v0runtime.CallStackFrame;
import org.apache.vxquery.v0runtime.LocalRegisterAccessor;
import org.apache.vxquery.v0runtime.RegisterAllocator;
import org.apache.vxquery.v0runtime.base.AbstractLazilyEvaluatedFunctionIterator;
import org.apache.vxquery.v0runtime.base.RuntimeIterator;

public class FnRemoveIterator extends AbstractLazilyEvaluatedFunctionIterator {
    private final LocalRegisterAccessor<BigInteger> removePoint;
    private final LocalRegisterAccessor<BigInteger> index;
    private final LocalRegisterAccessor<State> state;

    private enum State {
        PRE, POST,
    }

    public FnRemoveIterator(RegisterAllocator rAllocator, Function fn, RuntimeIterator[] arguments,
            StaticContext ctx) {
        super(rAllocator, fn, arguments, ctx);
        removePoint = new LocalRegisterAccessor<BigInteger>(rAllocator.allocate(1));
        index = new LocalRegisterAccessor<BigInteger>(rAllocator.allocate(1));
        state = new LocalRegisterAccessor<State>(rAllocator.allocate(1));
    }

    @Override
    public void close(CallStackFrame frame) {
        arguments[0].close(frame);
    }

    @Override
    public Object next(CallStackFrame frame) throws SystemException {
        if (removePoint.get(frame) == null) {
            NumericValue nVal = (NumericValue) arguments[1].evaluateEagerly(frame);
            BigInteger i = nVal.getIntegerValue();
            if (i.compareTo(BigInteger.ONE) < 0) {
                i = BigInteger.ONE;
            }
            removePoint.set(frame, i);
        }
        switch (state.get(frame)) {
            case PRE:
                XDMValue v = (XDMValue) arguments[0].next(frame);
                if (v == null) {
                    return null;
                }
                if (index.get(frame).compareTo(removePoint.get(frame)) < 0) {
                    index.set(frame, index.get(frame).add(BigInteger.ONE));
                    return v;
                }
                state.set(frame, State.POST);

            case POST:
                return arguments[0].next(frame);

            default:
                throw new IllegalStateException();
        }
    }

    @Override
    public void open(CallStackFrame frame) {
        arguments[0].open(frame);
        index.set(frame, BigInteger.ONE);
        removePoint.set(frame, null);
        state.set(frame, State.PRE);
    }
}