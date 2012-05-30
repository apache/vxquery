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
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.v0datamodel.XDMItem;
import org.apache.vxquery.v0runtime.CallStackFrame;
import org.apache.vxquery.v0runtime.RegisterAllocator;
import org.apache.vxquery.v0runtime.base.AbstractEagerlyEvaluatedFunctionIterator;
import org.apache.vxquery.v0runtime.base.RuntimeIterator;

public class FnZeroOrOneIterator extends AbstractEagerlyEvaluatedFunctionIterator {
    public FnZeroOrOneIterator(RegisterAllocator allocator, Function fn, RuntimeIterator[] arguments,
            StaticContext ctx) {
        super(allocator, fn, arguments, ctx);
    }

    @Override
    public Object evaluateEagerly(CallStackFrame frame) throws SystemException {
        final RuntimeIterator arg = arguments[0];
        arg.open(frame);
        try {
            XDMItem i1 = (XDMItem) arg.next(frame);
            if (i1 == null) {
                return null;
            }
            if (arg.next(frame) != null) {
                throw new SystemException(ErrorCode.FORG0003);
            }
            return i1;
        } finally {
            arg.close(frame);
        }
    }
}