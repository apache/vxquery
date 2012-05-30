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

import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.v0runtime.CallStackFrame;
import org.apache.vxquery.v0runtime.LocalRegisterAccessor;
import org.apache.vxquery.v0runtime.RegisterAllocator;
import org.apache.vxquery.v0runtime.RuntimeUtils;
import org.apache.vxquery.v0runtime.base.AbstractLazilyEvaluatedFunctionIterator;
import org.apache.vxquery.v0runtime.base.RuntimeIterator;
import org.apache.vxquery.v0runtime.core.Deflater;

public class OpExtDeflateSequencesIterator extends AbstractLazilyEvaluatedFunctionIterator {
    private final LocalRegisterAccessor<Deflater> deflater;

    public OpExtDeflateSequencesIterator(RegisterAllocator rAllocator, Function fn, RuntimeIterator[] arguments,
            StaticContext ctx) {
        super(rAllocator, fn, arguments, ctx);
        deflater = new LocalRegisterAccessor<Deflater>(rAllocator.allocate(1));
    }

    @Override
    public void close(CallStackFrame frame) {
        deflater.get(frame).close();
    }

    @Override
    public Object next(CallStackFrame frame) throws SystemException {
        return deflater.get(frame).next();
    }

    @Override
    public void open(CallStackFrame frame) {
        arguments[0].open(frame);
        Deflater d = new Deflater();
        d.reset(RuntimeUtils.createCloseableIterator(frame, arguments[0]));
        deflater.set(frame, d);
    }
}