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
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.runtime.CallStackFrame;
import org.apache.vxquery.runtime.LocalRegisterAccessor;
import org.apache.vxquery.runtime.RegisterAllocator;
import org.apache.vxquery.runtime.base.RuntimeIterator;

abstract class AbstractItemSortingAndDistinctIterator extends AbstractItemSortingIterator {
    private final LocalRegisterAccessor<XDMItem> node;

    public AbstractItemSortingAndDistinctIterator(RegisterAllocator rAllocator, Function fn,
            RuntimeIterator[] arguments, StaticContext ctx) {
        super(rAllocator, fn, arguments, ctx);
        node = new LocalRegisterAccessor<XDMItem>(rAllocator.allocate(1));
    }

    @Override
    public void open(CallStackFrame frame) {
        super.open(frame);
        node.set(frame, null);
    }

    @Override
    public Object next(CallStackFrame frame) throws SystemException {
        while (true) {
            XDMItem nv = (XDMItem) super.next(frame);
            XDMItem n = node.get(frame);
            if (!performDistinct(frame) || n == null || nv == null || !itemsEqual(n, nv)) {
                node.set(frame, nv);
                return nv;
            }
        }
    }

    protected boolean performDistinct(CallStackFrame frame) {
        return true;
    }

    protected abstract boolean itemsEqual(XDMItem i0, XDMItem i1) throws SystemException;
}