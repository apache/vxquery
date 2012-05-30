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
import org.apache.vxquery.v0datamodel.XDMItem;
import org.apache.vxquery.v0datamodel.atomic.StringValue;
import org.apache.vxquery.v0runtime.CallStackFrame;
import org.apache.vxquery.v0runtime.RegisterAllocator;
import org.apache.vxquery.v0runtime.RuntimeUtils;
import org.apache.vxquery.v0runtime.base.AbstractEagerlyEvaluatedFunctionIterator;
import org.apache.vxquery.v0runtime.base.RuntimeIterator;

public class FnNormalizeSpaceIterator extends AbstractEagerlyEvaluatedFunctionIterator {
    public FnNormalizeSpaceIterator(RegisterAllocator rAllocator, Function fn, RuntimeIterator[] arguments,
            StaticContext ctx) {
        super(rAllocator, fn, arguments, ctx);
    }

    @Override
    public Object evaluateEagerly(CallStackFrame frame) throws SystemException {
        XDMItem value = RuntimeUtils.fetchItemEagerly(arguments[0], frame);
        if (value == null) {
            return StringValue.EMPTY_STRING;
        }
        CharSequence cs = value.getStringValue();
        StringBuilder normalized = new StringBuilder();
        int i = 0;
        int n = cs.length();
        boolean ws = false;
        boolean first = true;
        while (i < n) {
            char ch = cs.charAt(i++);
            if (Character.isWhitespace(ch)) {
                ws = true;
                continue;
            }
            if (!first && ws) {
                normalized.append(' ');
            }
            first = false;
            ws = false;
            normalized.append(ch);
        }
        return frame.getRuntimeControlBlock().getAtomicValueFactory().createString(normalized);
    }
}