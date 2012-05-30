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

import org.apache.vxquery.collations.Collation;
import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.v0datamodel.XDMItem;
import org.apache.vxquery.v0runtime.CallStackFrame;
import org.apache.vxquery.v0runtime.RegisterAllocator;
import org.apache.vxquery.v0runtime.RuntimeUtils;
import org.apache.vxquery.v0runtime.base.AbstractEagerlyEvaluatedFunctionIterator;
import org.apache.vxquery.v0runtime.base.RuntimeIterator;

public abstract class AbstractStringMatchingIterator extends AbstractEagerlyEvaluatedFunctionIterator {
    public AbstractStringMatchingIterator(RegisterAllocator rAllocator, Function fn, RuntimeIterator[] arguments,
            StaticContext ctx) {
        super(rAllocator, fn, arguments, ctx);
    }

    @Override
    public final Object evaluateEagerly(CallStackFrame frame) throws SystemException {
        Collation collation = getCollation(frame);
        if (!collation.supportsStringMatching()) {
            throw new SystemException(ErrorCode.FOCH0004);
        }
        XDMItem v2 = RuntimeUtils.fetchItemEagerly(arguments[1], frame);
        XDMItem v1 = RuntimeUtils.fetchItemEagerly(arguments[0], frame);
        return matchingResult(frame, v1, v2, collation);
    }

    protected abstract Object matchingResult(CallStackFrame frame, XDMItem v1, XDMItem v2, Collation collation)
            throws SystemException;

    private Collation getCollation(CallStackFrame frame) throws SystemException {
        Collation collation = null;
        if (arguments.length > 2) {
            XDMItem cv = RuntimeUtils.fetchItemEagerly(arguments[2], frame);
            String collationName = cv.getStringValue().toString();
            collation = frame.getRuntimeControlBlock().getDynamicContext().getStaticContext().lookupCollation(
                    collationName);
            if (collation == null) {
                throw new SystemException(ErrorCode.FOCH0002);
            }
        } else {
            collation = frame.getRuntimeControlBlock().getDefaultCollation();
        }
        return collation;
    }
}