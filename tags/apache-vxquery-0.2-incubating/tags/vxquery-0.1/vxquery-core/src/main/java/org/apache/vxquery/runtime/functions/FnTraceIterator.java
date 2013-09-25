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
import org.apache.vxquery.datamodel.atomic.StringValue;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.runtime.CallStackFrame;
import org.apache.vxquery.runtime.LocalRegisterAccessor;
import org.apache.vxquery.runtime.RegisterAllocator;
import org.apache.vxquery.runtime.base.AbstractFunctionIterator;
import org.apache.vxquery.runtime.base.RuntimeIterator;

public class FnTraceIterator extends AbstractFunctionIterator {
    private final LocalRegisterAccessor<String> label;

    public FnTraceIterator(RegisterAllocator rAllocator, Function fn, RuntimeIterator[] arguments,
            StaticContext ctx) {
        super(rAllocator, fn, arguments, ctx);
        label = new LocalRegisterAccessor<String>(rAllocator.allocate(1));
    }

    @Override
    public void close(CallStackFrame frame) {
        System.err.println(label.get(frame) + " close()");
        arguments[0].close(frame);
    }

    @Override
    public Object next(CallStackFrame frame) throws SystemException {
        Object v = arguments[0].next(frame);
        System.err.println(label.get(frame) + " next() -> " + v);
        return v;
    }

    @Override
    public void open(CallStackFrame frame) {
        StringValue l;
        try {
            l = (StringValue) arguments[1].evaluateEagerly(frame);
        } catch (SystemException e) {
            System.err.println("Exception occured");
            throw new RuntimeException(e);
        }
        label.set(frame, l.getStringValue().toString());
        System.err.println(label.get(frame) + " open()");
        arguments[0].open(frame);
    }

    @Override
    public Object evaluateEagerly(CallStackFrame frame) throws SystemException {
        StringValue l = (StringValue) arguments[1].evaluateEagerly(frame);
        Object v = arguments[0].evaluateEagerly(frame);
        System.err.println(l.getStringValue() + " evaluateEagerly -> " + v);
        return v;
    }
}