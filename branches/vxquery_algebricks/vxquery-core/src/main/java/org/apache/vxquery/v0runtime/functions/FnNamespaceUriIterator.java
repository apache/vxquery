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
import org.apache.vxquery.v0datamodel.XDMNode;
import org.apache.vxquery.v0datamodel.atomic.AnyUriValue;
import org.apache.vxquery.v0datamodel.atomic.QNameValue;
import org.apache.vxquery.v0runtime.CallStackFrame;
import org.apache.vxquery.v0runtime.RegisterAllocator;
import org.apache.vxquery.v0runtime.base.AbstractEagerlyEvaluatedFunctionIterator;
import org.apache.vxquery.v0runtime.base.RuntimeIterator;

public class FnNamespaceUriIterator extends AbstractEagerlyEvaluatedFunctionIterator {
    public FnNamespaceUriIterator(RegisterAllocator allocator, Function fn, RuntimeIterator[] arguments,
            StaticContext ctx) {
        super(allocator, fn, arguments, ctx);
    }

    @Override
    public Object evaluateEagerly(CallStackFrame frame) throws SystemException {
        XDMNode n = (XDMNode) arguments[0].evaluateEagerly(frame);
        if (n == null) {
            return AnyUriValue.EMPTY_URI;
        }
        QNameValue nodeName = n.getNodeName();
        final int nc = nodeName.getCode();
        return nc < 0 ? AnyUriValue.EMPTY_URI : frame.getRuntimeControlBlock().getAtomicValueFactory().createAnyUri(
                nodeName.getNameCache().getUri(nc));
    }
}