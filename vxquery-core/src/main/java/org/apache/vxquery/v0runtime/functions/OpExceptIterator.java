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
import org.apache.vxquery.v0runtime.CallStackFrame;
import org.apache.vxquery.v0runtime.LocalRegisterAccessor;
import org.apache.vxquery.v0runtime.RegisterAllocator;
import org.apache.vxquery.v0runtime.base.AbstractLazilyEvaluatedFunctionIterator;
import org.apache.vxquery.v0runtime.base.RuntimeIterator;

public class OpExceptIterator extends AbstractLazilyEvaluatedFunctionIterator {
    private final LocalRegisterAccessor<XDMNode> nextException;

    public OpExceptIterator(RegisterAllocator rAllocator, Function fn, RuntimeIterator[] arguments, StaticContext ctx) {
        super(rAllocator, fn, arguments, ctx);
        nextException = new LocalRegisterAccessor<XDMNode>(rAllocator.allocate(1));
    }

    @Override
    public void close(CallStackFrame frame) {
        arguments[1].close(frame);
        arguments[0].close(frame);
    }

    @Override
    public Object next(CallStackFrame frame) throws SystemException {
        XDMNode left = (XDMNode) arguments[0].next(frame);
        XDMNode right = nextException.get(frame);
        while (left != null) {
            if (right == null) {
                right = (XDMNode) arguments[1].next(frame);
            }
            if (right == null || left.compareDocumentOrder(right) < 0) {
                nextException.set(frame, right);
                return left;
            }
            while (right != null && left.compareDocumentOrder(right) > 0) {
                right = (XDMNode) arguments[1].next(frame);
            }
            if (right == null || !left.isSameNode(right)) {
                nextException.set(frame, right);
                return left;
            }
            left = (XDMNode) arguments[0].next(frame);
        }
        return null;
    }

    @Override
    public void open(CallStackFrame frame) {
        arguments[0].open(frame);
        arguments[1].open(frame);
    }
}
