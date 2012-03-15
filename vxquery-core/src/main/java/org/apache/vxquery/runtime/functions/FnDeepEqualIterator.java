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

import org.apache.vxquery.collations.Collation;
import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.runtime.CallStackFrame;
import org.apache.vxquery.runtime.RegisterAllocator;
import org.apache.vxquery.runtime.RuntimeControlBlock;
import org.apache.vxquery.runtime.base.AbstractEagerlyEvaluatedFunctionIterator;
import org.apache.vxquery.runtime.base.RuntimeIterator;
import org.apache.vxquery.v0datamodel.NodeEqComparator;
import org.apache.vxquery.v0datamodel.XDMAtomicValue;
import org.apache.vxquery.v0datamodel.XDMItem;
import org.apache.vxquery.v0datamodel.XDMNode;
import org.apache.vxquery.v0datamodel.atomic.StringValue;

public class FnDeepEqualIterator extends AbstractEagerlyEvaluatedFunctionIterator {
    public FnDeepEqualIterator(RegisterAllocator rAllocator, Function fn, RuntimeIterator[] arguments, StaticContext ctx) {
        super(rAllocator, fn, arguments, ctx);
    }

    @Override
    public Object evaluateEagerly(CallStackFrame frame) throws SystemException {
        final NodeEqComparator comparator = getComparator(frame);

        final RuntimeIterator left = arguments[0];
        final RuntimeIterator right = arguments[1];
        left.open(frame);
        right.open(frame);

        final RuntimeControlBlock rcb = frame.getRuntimeControlBlock();
        Object lObj;
        while ((lObj = left.next(frame)) != null) {
            Object rObj = right.next(frame);
            if (rObj == null) {
                left.close(frame);
                right.close(frame);
                return rcb.getAtomicValueFactory().createBoolean(false);
            }
            XDMItem lItem = (XDMItem) lObj;
            XDMItem rItem = (XDMItem) rObj;
            if (lItem.getDMOKind() != rItem.getDMOKind()) {
                left.close(frame);
                right.close(frame);
                return rcb.getAtomicValueFactory().createBoolean(false);
            }
            if (!itemsEqual(lItem, rItem, comparator, rcb)) {
                return rcb.getAtomicValueFactory().createBoolean(false);
            }
        }
        left.close(frame);
        final boolean result = right.next(frame) == null;
        right.close(frame);
        return rcb.getAtomicValueFactory().createBoolean(result);
    }

    private boolean itemsEqual(XDMItem lItem, XDMItem rItem, final NodeEqComparator comparator,
            final RuntimeControlBlock rcb) throws SystemException {
        switch (lItem.getDMOKind()) {
            case ATOMIC_VALUE:
                return comparator.compareAtomic((XDMAtomicValue) lItem, (XDMAtomicValue) rItem, rcb);
            case DOCUMENT_NODE:
                return comparator.compareDocument((XDMNode) lItem, (XDMNode) rItem);
            case ELEMENT_NODE:
                return comparator.compareElement((XDMNode) lItem, (XDMNode) rItem, rcb);
            case ATTRIBUTE_NODE:
                return comparator.compareAttribute((XDMNode) lItem, (XDMNode) rItem, rcb);
            case PI_NODE:
                return comparator.compareProcessingInstruction((XDMNode) lItem, (XDMNode) rItem);
            case TEXT_NODE:
                return comparator.compareText((XDMNode) lItem, (XDMNode) rItem);
            case COMMENT_NODE:
                return comparator.compareComment((XDMNode) lItem, (XDMNode) rItem);
            default:
                throw new IllegalArgumentException();
        }
    }

    private NodeEqComparator getComparator(final CallStackFrame frame) throws SystemException {
        final RuntimeControlBlock rcb = frame.getRuntimeControlBlock();
        final Collation collation;
        if (arguments.length == 3) {
            final RuntimeIterator collationIterator = arguments[2];
            final StringValue collationName = (StringValue) collationIterator.evaluateEagerly(frame);
            collation = rcb.getDynamicContext().getStaticContext().lookupCollation(
                    collationName.getStringValue().toString());
            if (collation == null) {
                throw new SystemException(ErrorCode.FOCH0002);
            }
        } else {
            collation = rcb.getDefaultCollation();
        }
        return NodeEqComparator.get(collation);
    }

}
