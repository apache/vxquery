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

import org.apache.vxquery.collations.Collation;
import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.runtime.CallStackFrame;
import org.apache.vxquery.runtime.RegisterAllocator;
import org.apache.vxquery.runtime.RuntimeControlBlock;
import org.apache.vxquery.runtime.RuntimeUtils;
import org.apache.vxquery.runtime.base.AbstractEagerlyEvaluatedFunctionIterator;
import org.apache.vxquery.runtime.base.RuntimeIterator;
import org.apache.vxquery.v0datamodel.XDMAtomicValue;
import org.apache.vxquery.v0datamodel.XDMItem;
import org.apache.vxquery.v0datamodel.atomic.compare.ComparisonUtils;
import org.apache.vxquery.v0datamodel.atomic.compare.ValueComparator;

public abstract class AbstractMinMaxIterator extends AbstractEagerlyEvaluatedFunctionIterator {
    public AbstractMinMaxIterator(RegisterAllocator rAllocator, Function fn, RuntimeIterator[] arguments,
            StaticContext ctx) {
        super(rAllocator, fn, arguments, ctx);
    }

    @Override
    public Object evaluateEagerly(CallStackFrame frame) throws SystemException {
        RuntimeIterator ri = arguments[0];
        ri.open(frame);
        // TODO ensure that we work on the "converted sequence"
        try {
            XDMAtomicValue minMax = (XDMAtomicValue) ri.next(frame);
            if (minMax == null) {
                return null;
            }
            final RuntimeControlBlock rcb = frame.getRuntimeControlBlock();
            final ValueComparator cmp = getComparator();
            final Collation coll = getCollation(frame);
            XDMAtomicValue nxt = (XDMAtomicValue) ri.next(frame);
            while (nxt != null) {
                Boolean res = ComparisonUtils.valueCompare(rcb, nxt, minMax, cmp, coll);
                if (res == null) {
                    throw new SystemException(ErrorCode.FORG0006);
                }
                if (res) {
                    minMax = nxt;
                }
                nxt = (XDMAtomicValue) ri.next(frame);
            }
            return minMax;
        } finally {
            ri.close(frame);
        }
    }

    protected abstract ValueComparator getComparator();

    private Collation getCollation(CallStackFrame frame) throws SystemException {
        if (arguments.length > 1) {
            XDMItem cv = RuntimeUtils.fetchItemEagerly(arguments[1], frame);
            String collationName = cv.getStringValue().toString();
            Collation collation = frame.getRuntimeControlBlock().getDynamicContext().getStaticContext()
                    .lookupCollation(collationName);
            if (collation == null) {
                throw new SystemException(ErrorCode.FOCH0002);
            }
            return collation;
        }
        return frame.getRuntimeControlBlock().getDefaultCollation();
    }
}
