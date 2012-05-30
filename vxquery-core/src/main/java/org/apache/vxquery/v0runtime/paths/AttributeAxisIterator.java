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
package org.apache.vxquery.v0runtime.paths;

import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.util.Filter;
import org.apache.vxquery.v0datamodel.XDMNode;
import org.apache.vxquery.v0datamodel.XDMValue;
import org.apache.vxquery.v0runtime.CallStackFrame;
import org.apache.vxquery.v0runtime.LocalRegisterAccessor;
import org.apache.vxquery.v0runtime.RegisterAllocator;
import org.apache.vxquery.v0runtime.RegisterSet;
import org.apache.vxquery.v0runtime.base.AbstractLazilyEvaluatedIterator;
import org.apache.vxquery.v0runtime.base.CloseableIterator;
import org.apache.vxquery.v0runtime.base.RuntimeIterator;

public class AttributeAxisIterator extends AbstractLazilyEvaluatedIterator {
    private final RuntimeIterator input;
    private final Filter<XDMValue> filter;
    private final LocalRegisterAccessor<CloseableIterator> sequence;

    public AttributeAxisIterator(RegisterAllocator rAllocator, RuntimeIterator input, Filter<XDMValue> filter) {
        super(rAllocator);
        this.input = input;
        this.filter = filter;
        sequence = new LocalRegisterAccessor<CloseableIterator>(rAllocator.allocate(1));
    }

    @Override
    public void open(CallStackFrame frame) {
        input.open(frame);
        sequence.set(frame, null);
    }

    @Override
    public void close(CallStackFrame frame) {
        CloseableIterator ci = sequence.get(frame);
        if (ci != null) {
            ci.close();
        }
        input.close(frame);
    }

    @Override
    public Object next(CallStackFrame frame) throws SystemException {
        RegisterSet regs = frame.getLocalRegisters();
        while (true) {
            while (sequence.get(regs) != null) {
                XDMNode candidate = (XDMNode) sequence.get(regs).next();
                if (candidate == null) {
                    sequence.get(regs).close();
                    sequence.set(regs, null);
                } else if (filter.accept(candidate)) {
                    return candidate;
                }
            }
            XDMNode node = (XDMNode) input.next(frame);
            if (node == null) {
                return null;
            }
            if (node.hasAttributes()) {
                sequence.set(regs, node.getAttributes());
            }
        }
    }
}