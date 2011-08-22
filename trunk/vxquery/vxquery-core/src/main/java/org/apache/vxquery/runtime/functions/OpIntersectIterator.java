/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.vxquery.runtime.functions;

import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.datamodel.XDMNode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.runtime.CallStackFrame;
import org.apache.vxquery.runtime.RegisterAllocator;
import org.apache.vxquery.runtime.base.AbstractLazilyEvaluatedFunctionIterator;
import org.apache.vxquery.runtime.base.RuntimeIterator;

public class OpIntersectIterator extends AbstractLazilyEvaluatedFunctionIterator {
    public OpIntersectIterator(RegisterAllocator rAllocator, Function fn, RuntimeIterator[] arguments, StaticContext ctx) {
        super(rAllocator, fn, arguments, ctx);
    }

    @Override
    public void close(CallStackFrame frame) {
        arguments[1].close(frame);
        arguments[0].close(frame);
    }

    @Override
    public Object next(CallStackFrame frame) throws SystemException {
        XDMNode right = (XDMNode) arguments[1].next(frame);
        if (right == null) {
            return null;
        }
        XDMNode left = (XDMNode) arguments[0].next(frame);
        while (true) {
            while (left != null && left.compareDocumentOrder(right) < 0) {
                left = (XDMNode) arguments[0].next(frame);
            }
            if (left == null) {
                return null;
            }
            while (right != null && left.compareDocumentOrder(right) > 0) {
                right = (XDMNode) arguments[1].next(frame);
            }
            if (right == null) {
                return null;
            }
            if (left.isSameNode(right)) {
                return left;
            }
        }
    }

    @Override
    public void open(CallStackFrame frame) {
        arguments[0].open(frame);
        arguments[1].open(frame);
    }
}