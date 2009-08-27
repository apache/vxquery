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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.datamodel.DMOKind;
import org.apache.vxquery.datamodel.XDMItem;
import org.apache.vxquery.datamodel.XDMSequence;
import org.apache.vxquery.datamodel.XDMValue;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.runtime.CallStackFrame;
import org.apache.vxquery.runtime.LocalRegisterAccessor;
import org.apache.vxquery.runtime.RegisterAllocator;
import org.apache.vxquery.runtime.RegisterSet;
import org.apache.vxquery.runtime.base.AbstractLazilyEvaluatedFunctionIterator;
import org.apache.vxquery.runtime.base.RuntimeIterator;

abstract class AbstractItemSortingIterator extends AbstractLazilyEvaluatedFunctionIterator {
    private final LocalRegisterAccessor<Boolean> first;
    private final LocalRegisterAccessor<ArrayList<XDMItem>> buffer;
    private final LocalRegisterAccessor<Integer> index;

    public AbstractItemSortingIterator(RegisterAllocator rAllocator, Function fn, RuntimeIterator[] arguments,
            StaticContext ctx) {
        super(rAllocator, fn, arguments, ctx);
        first = new LocalRegisterAccessor<Boolean>(rAllocator.allocate(1));
        buffer = new LocalRegisterAccessor<ArrayList<XDMItem>>(rAllocator.allocate(1));
        index = new LocalRegisterAccessor<Integer>(rAllocator.allocate(1));
    }

    @Override
    public void open(CallStackFrame frame) {
        arguments[0].open(frame);
        first.set(frame, true);
        index.set(frame, 0);
        buffer.set(frame, new ArrayList<XDMItem>());
    }

    @Override
    public Object next(CallStackFrame frame) throws SystemException {
        RegisterSet regs = frame.getLocalRegisters();
        ArrayList<XDMItem> bufArr = buffer.get(regs);
        if (first.get(regs)) {
            XDMValue v;
            RuntimeIterator in = arguments[0];
            while ((v = (XDMValue) in.next(frame)) != null) {
                if (v.getDMOKind() == DMOKind.SEQUENCE) {
                    ((XDMSequence) v).appendToList(bufArr);
                } else {
                    bufArr.add((XDMItem) v);
                }
            }
            if (performSorting(frame, bufArr)) {
                Collections.sort(buffer.get(regs), getComparator());
            }
            first.set(regs, false);
        }
        int idx = index.get(regs);
        if (idx < bufArr.size()) {
            index.set(regs, idx + 1);
            return bufArr.get(idx);
        }
        return null;
    }

    protected boolean performSorting(CallStackFrame frame, ArrayList<XDMItem> buffer) {
        return true;
    }

    protected abstract Comparator<XDMItem> getComparator();

    @Override
    public void close(CallStackFrame frame) {
        arguments[0].close(frame);
    }
}