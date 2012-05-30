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
import org.apache.vxquery.v0datamodel.DMOKind;
import org.apache.vxquery.v0datamodel.XDMItem;
import org.apache.vxquery.v0datamodel.atomic.BooleanValue;
import org.apache.vxquery.v0datamodel.atomic.NumericValue;
import org.apache.vxquery.v0datamodel.atomic.StringValue;
import org.apache.vxquery.v0runtime.CallStackFrame;
import org.apache.vxquery.v0runtime.RegisterAllocator;
import org.apache.vxquery.v0runtime.base.AbstractEagerlyEvaluatedFunctionIterator;
import org.apache.vxquery.v0runtime.base.RuntimeIterator;

/**
 * Compute the effective boolean value.
 * 
 * Section 15.1.1
 * 
 * If $arg is the empty sequence, fn:boolean returns false.
 * 
 * 
 * If $arg is a sequence whose first item is a node, fn:boolean returns true.
 * 
 * 
 * If $arg is a singleton value of type xs:boolean or a derived from xs:boolean,
 * fn:boolean returns $arg.
 * 
 * 
 * If $arg is a singleton value of type xs:string or a type derived from
 * xs:string, xs:anyURI or a type derived from xs:anyURI or xs:untypedAtomic,
 * fn:boolean returns false if the operand value has zero length; otherwise it
 * returns true.
 * 
 * 
 * If $arg is a singleton value of any numeric type or a type derived from a
 * numeric type, fn:boolean returns false if the operand value is NaN or is
 * numerically equal to zero; otherwise it returns true.
 * 
 * 
 * In all other cases, fn:boolean raises a type error [err:FORG0006].
 */
public class FnBooleanIterator extends AbstractEagerlyEvaluatedFunctionIterator {
    public FnBooleanIterator(RegisterAllocator rAllocator, Function fn, RuntimeIterator[] arguments,
            StaticContext ctx) {
        super(rAllocator, fn, arguments, ctx);
    }

    @Override
    public Object evaluateEagerly(CallStackFrame frame) throws SystemException {
        arguments[0].open(frame);
        try {
            XDMItem o1 = (XDMItem) arguments[0].next(frame);
            if (o1 == null) {
                return BooleanValue.FALSE;
            }
            DMOKind o1Kind = o1.getDMOKind();
            if (o1Kind != DMOKind.ATOMIC_VALUE) {
                return BooleanValue.TRUE;
            }
            XDMItem o2 = (XDMItem) arguments[0].next(frame);
            if (o2 == null) {
                if (o1 instanceof BooleanValue) {
                    return o1;
                } else if (o1 instanceof StringValue) {
                    return ((StringValue) o1).getStringLength() == 0 ? BooleanValue.FALSE : BooleanValue.TRUE;
                } else if (o1 instanceof NumericValue) {
                    double val = ((NumericValue) o1).getDoubleValue();
                    return val == 0 || Double.isNaN(val) ? BooleanValue.FALSE : BooleanValue.TRUE;
                }
            }
            throw new SystemException(ErrorCode.FORG0006);
        } finally {
            arguments[0].close(frame);
        }
    }
}