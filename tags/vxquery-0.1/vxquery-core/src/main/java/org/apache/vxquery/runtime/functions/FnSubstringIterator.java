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
import org.apache.vxquery.datamodel.XDMItem;
import org.apache.vxquery.datamodel.atomic.StringValue;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.runtime.CallStackFrame;
import org.apache.vxquery.runtime.RegisterAllocator;
import org.apache.vxquery.runtime.RuntimeUtils;
import org.apache.vxquery.runtime.base.AbstractEagerlyEvaluatedFunctionIterator;
import org.apache.vxquery.runtime.base.RuntimeIterator;

public class FnSubstringIterator extends AbstractEagerlyEvaluatedFunctionIterator {
    public FnSubstringIterator(RegisterAllocator rAllocator, Function fn, RuntimeIterator[] arguments,
            StaticContext ctx) {
        super(rAllocator, fn, arguments, ctx);
    }

    @Override
    public Object evaluateEagerly(CallStackFrame frame) throws SystemException {
        XDMItem v = RuntimeUtils.fetchItemEagerly(arguments[0], frame);
        if (v == null) {
            return StringValue.EMPTY_STRING;
        }
        CharSequence cs = v.getStringValue();
        int start = (int) Math.round(RuntimeUtils.fetchNumericItemEagerly(arguments[1], frame).getDoubleValue());
        start--;
        if (start < 0) {
            start = 0;
        }
        int end = cs.length();
        if (arguments.length > 2) {
            int length = (int) Math.round(RuntimeUtils.fetchNumericItemEagerly(arguments[2], frame).getDoubleValue());
            end = Math.min(end, start + length);
        }
        return frame.getRuntimeControlBlock().getAtomicValueFactory().createString(cs.subSequence(start, end));
    }
}