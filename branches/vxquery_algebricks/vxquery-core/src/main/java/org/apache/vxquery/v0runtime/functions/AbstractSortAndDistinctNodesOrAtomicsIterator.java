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

import java.util.ArrayList;

import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.v0datamodel.DMOKind;
import org.apache.vxquery.v0datamodel.XDMItem;
import org.apache.vxquery.v0datamodel.XDMNode;
import org.apache.vxquery.v0runtime.CallStackFrame;
import org.apache.vxquery.v0runtime.LocalRegisterAccessor;
import org.apache.vxquery.v0runtime.RegisterAllocator;
import org.apache.vxquery.v0runtime.base.RuntimeIterator;

abstract class AbstractSortAndDistinctNodesOrAtomicsIterator extends AbstractItemSortingAndDistinctIterator {
    private final LocalRegisterAccessor<Boolean> atomics;

    public AbstractSortAndDistinctNodesOrAtomicsIterator(RegisterAllocator rAllocator, Function fn,
            RuntimeIterator[] arguments, StaticContext ctx) {
        super(rAllocator, fn, arguments, ctx);
        atomics = new LocalRegisterAccessor<Boolean>(rAllocator.allocate(1));
    }

    @Override
    protected boolean performSorting(CallStackFrame frame, ArrayList<XDMItem> buffer) {
        Boolean a = atomics.get(frame);
        if (a == null) {
            a = true;
            if (!buffer.isEmpty()) {
                a = buffer.get(0).getDMOKind() == DMOKind.ATOMIC_VALUE;
            }
            atomics.set(frame, a);
        }
        return !a;
    }

    @Override
    protected boolean performDistinct(CallStackFrame frame) {
        return !atomics.get(frame);
    }

    @Override
    protected boolean itemsEqual(XDMItem i0, XDMItem i1) throws SystemException {
        if (!i0.getDMOKind().isNode() || !i1.getDMOKind().isNode()) {
            throw new SystemException(ErrorCode.XPTY0018);
        }
        return ((XDMNode) i0).isSameNode((XDMNode) i1);
    }
}