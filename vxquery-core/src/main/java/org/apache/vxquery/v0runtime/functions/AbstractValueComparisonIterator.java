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
import org.apache.vxquery.v0datamodel.XDMAtomicValue;
import org.apache.vxquery.v0datamodel.atomic.compare.ComparisonUtils;
import org.apache.vxquery.v0datamodel.atomic.compare.ValueComparator;
import org.apache.vxquery.v0runtime.CallStackFrame;
import org.apache.vxquery.v0runtime.RegisterAllocator;
import org.apache.vxquery.v0runtime.RuntimeControlBlock;
import org.apache.vxquery.v0runtime.base.AbstractEagerlyEvaluatedFunctionIterator;
import org.apache.vxquery.v0runtime.base.RuntimeIterator;

public abstract class AbstractValueComparisonIterator extends AbstractEagerlyEvaluatedFunctionIterator {
    public AbstractValueComparisonIterator(RegisterAllocator rAllocator, Function fn, RuntimeIterator[] arguments,
            StaticContext ctx) {
        super(rAllocator, fn, arguments, ctx);
    }

    @Override
    public Object evaluateEagerly(CallStackFrame frame) throws SystemException {
        final XDMAtomicValue v1 = (XDMAtomicValue) arguments[0].evaluateEagerly(frame);
        if (v1 == null) {
            return null;
        }
        final XDMAtomicValue v2 = (XDMAtomicValue) arguments[1].evaluateEagerly(frame);
        if (v2 == null) {
            return null;
        }
        final RuntimeControlBlock rcb = frame.getRuntimeControlBlock();
        Boolean res = ComparisonUtils.valueCompare(rcb, v1, v2, getComparator(), rcb
                .getDefaultCollation());
        if (res == null) {
            throw new SystemException(ErrorCode.XPTY0004);
        }
        return rcb.getAtomicValueFactory().createBoolean(res);
    }

    protected abstract ValueComparator getComparator();
}