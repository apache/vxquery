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
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.runtime.CallStackFrame;
import org.apache.vxquery.runtime.RegisterAllocator;
import org.apache.vxquery.runtime.base.AbstractEagerlyEvaluatedFunctionIterator;
import org.apache.vxquery.runtime.base.RuntimeIterator;
import org.apache.vxquery.v0datamodel.DMOKind;
import org.apache.vxquery.v0datamodel.XDMItem;
import org.apache.vxquery.v0datamodel.atomic.BooleanValue;
import org.apache.vxquery.v0datamodel.atomic.NumericValue;
import org.apache.vxquery.v0datamodel.atomic.StringValue;

public class FnNotIterator extends AbstractEagerlyEvaluatedFunctionIterator {
    public FnNotIterator(RegisterAllocator rAllocator, Function fn, RuntimeIterator[] arguments, StaticContext ctx) {
        super(rAllocator, fn, arguments, ctx);
    }

    @Override
    public Object evaluateEagerly(CallStackFrame frame) throws SystemException {
        arguments[0].open(frame);
        try {
            XDMItem o1 = (XDMItem) arguments[0].next(frame);
            if (o1 == null) {
                return BooleanValue.TRUE;
            }
            DMOKind o1Kind = o1.getDMOKind();
            if (o1Kind != DMOKind.ATOMIC_VALUE) {
                return BooleanValue.FALSE;
            }
            XDMItem o2 = (XDMItem) arguments[0].next(frame);
            if (o2 == null) {
                if (o1 instanceof BooleanValue) {
                    return frame.getRuntimeControlBlock().getAtomicValueFactory().createBoolean(
                            !((BooleanValue) o1).getBooleanValue());
                } else if (o1 instanceof StringValue) {
                    return ((StringValue) o1).getStringLength() == 0 ? BooleanValue.TRUE : BooleanValue.FALSE;
                } else if (o1 instanceof NumericValue) {
                    double val = ((NumericValue) o1).getDoubleValue();
                    return val == 0 || Double.isNaN(val) ? BooleanValue.TRUE : BooleanValue.FALSE;
                }
            }
            throw new SystemException(ErrorCode.FORG0006);
        } finally {
            arguments[0].close(frame);
        }
    }
}