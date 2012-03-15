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
package org.apache.vxquery.runtime.paths;

import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.CallStackFrame;
import org.apache.vxquery.runtime.LocalRegisterAccessor;
import org.apache.vxquery.runtime.RegisterAllocator;
import org.apache.vxquery.runtime.RegisterSet;
import org.apache.vxquery.runtime.base.AbstractLazilyEvaluatedIterator;
import org.apache.vxquery.runtime.base.CloseableIterator;
import org.apache.vxquery.runtime.base.RuntimeIterator;
import org.apache.vxquery.util.Filter;
import org.apache.vxquery.v0datamodel.XDMNode;
import org.apache.vxquery.v0datamodel.XDMValue;

public class DescendantAxisIterator extends AbstractLazilyEvaluatedIterator {
    private final RuntimeIterator input;
    private final Filter<XDMValue> filter;
    private final LocalRegisterAccessor<PathStack> pStack;

    public DescendantAxisIterator(RegisterAllocator rAllocator, RuntimeIterator input, Filter<XDMValue> filter) {
        super(rAllocator);
        this.input = input;
        this.filter = filter;
        pStack = new LocalRegisterAccessor<PathStack>(rAllocator.allocate(1));
    }

    @Override
    public void open(CallStackFrame frame) {
        input.open(frame);
        pStack.set(frame, new PathStack());
    }

    @Override
    public void close(CallStackFrame frame) {
        PathStack ps = pStack.get(frame);
        while (!ps.isEmpty()) {
            ps.pop();
        }
        input.close(frame);
    }

    @Override
    public Object next(CallStackFrame frame) throws SystemException {
        final RegisterSet regs = frame.getLocalRegisters();
        PathStack ps = pStack.get(regs);
        while (true) {
            CloseableIterator seq = null;
            if (!ps.isEmpty()) {
                seq = ps.peek();
            }
            while (seq != null) {
                XDMNode candidate = (XDMNode) seq.next();
                if (candidate == null) {
                    ps.pop();
                    seq = ps.isEmpty() ? null : ps.peek();
                    continue;
                }
                if (candidate.hasChildren()) {
                    ps.push(candidate.getChildren());
                    seq = ps.peek();
                }
                if (filter.accept(candidate)) {
                    return candidate;
                }
            }
            XDMNode node = (XDMNode) input.next(frame);
            if (node == null) {
                return null;
            }
            ps.push(node.getChildren());
        }
    }
}