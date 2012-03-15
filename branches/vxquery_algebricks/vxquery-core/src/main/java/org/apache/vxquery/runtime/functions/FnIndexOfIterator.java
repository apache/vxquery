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

import org.apache.vxquery.collations.Collation;
import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.runtime.CallStackFrame;
import org.apache.vxquery.runtime.LocalRegisterAccessor;
import org.apache.vxquery.runtime.RegisterAllocator;
import org.apache.vxquery.runtime.RuntimeControlBlock;
import org.apache.vxquery.runtime.RuntimeUtils;
import org.apache.vxquery.runtime.base.AbstractLazilyEvaluatedFunctionIterator;
import org.apache.vxquery.runtime.base.RuntimeIterator;
import org.apache.vxquery.v0datamodel.XDMAtomicValue;
import org.apache.vxquery.v0datamodel.XDMItem;
import org.apache.vxquery.v0datamodel.atomic.compare.ComparisonUtils;
import org.apache.vxquery.v0datamodel.atomic.compare.ValueEqComparator;

public class FnIndexOfIterator extends AbstractLazilyEvaluatedFunctionIterator {
    private final LocalRegisterAccessor<XDMAtomicValue> value;
    private final LocalRegisterAccessor<BigInteger> index;
    private final LocalRegisterAccessor<Collation> collation;

    public FnIndexOfIterator(RegisterAllocator rAllocator, Function fn, RuntimeIterator[] arguments, StaticContext ctx) {
        super(rAllocator, fn, arguments, ctx);
        value = new LocalRegisterAccessor<XDMAtomicValue>(rAllocator.allocate(1));
        index = new LocalRegisterAccessor<BigInteger>(rAllocator.allocate(1));
        collation = new LocalRegisterAccessor<Collation>(rAllocator.allocate(1));
    }

    @Override
    public void close(CallStackFrame frame) {
        arguments[0].close(frame);
    }

    @Override
    public Object next(CallStackFrame frame) throws SystemException {
        final RuntimeControlBlock rcb = frame.getRuntimeControlBlock();
        if (value.get(frame) == null) {
            value.set(frame, (XDMAtomicValue) arguments[1].evaluateEagerly(frame));
            Collation c;
            if (arguments.length > 2) {
                XDMItem cv = RuntimeUtils.fetchItemEagerly(arguments[2], frame);
                String collationName = cv.getStringValue().toString();
                c = rcb.getDynamicContext().getStaticContext().lookupCollation(collationName);
                if (c == null) {
                    throw new SystemException(ErrorCode.FOCH0002);
                }
            } else {
                c = rcb.getDefaultCollation();
            }
            collation.set(frame, c);
        }
        while (true) {
            XDMAtomicValue v = (XDMAtomicValue) arguments[0].next(frame);
            if (v == null) {
                return null;
            }
            BigInteger idx = index.get(frame).add(BigInteger.ONE);
            index.set(frame, idx);
            Boolean res = ComparisonUtils.valueCompare(rcb, v, value.get(frame), ValueEqComparator.INSTANCE, collation
                    .get(frame));
            if (res != null && res) {
                return rcb.getAtomicValueFactory().createInteger(idx);
            }
        }
    }

    @Override
    public void open(CallStackFrame frame) {
        arguments[0].open(frame);
        value.set(frame, null);
        index.set(frame, BigInteger.ZERO);
    }
}