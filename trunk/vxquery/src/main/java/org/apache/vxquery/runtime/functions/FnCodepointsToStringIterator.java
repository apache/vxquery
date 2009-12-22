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

import java.util.List;

import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.datamodel.DMOKind;
import org.apache.vxquery.datamodel.XDMItem;
import org.apache.vxquery.datamodel.XDMNode;
import org.apache.vxquery.datamodel.XDMSequence;
import org.apache.vxquery.datamodel.XDMValue;
import org.apache.vxquery.datamodel.atomic.NumericValue;
import org.apache.vxquery.datamodel.atomic.StringValue;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.runtime.CallStackFrame;
import org.apache.vxquery.runtime.RegisterAllocator;
import org.apache.vxquery.runtime.base.AbstractEagerlyEvaluatedFunctionIterator;
import org.apache.vxquery.runtime.base.RuntimeIterator;

public class FnCodepointsToStringIterator extends AbstractEagerlyEvaluatedFunctionIterator {
    public FnCodepointsToStringIterator(RegisterAllocator rAllocator, Function fn, RuntimeIterator[] arguments,
            StaticContext ctx) {
        super(rAllocator, fn, arguments, ctx);
    }

    @Override
    public Object evaluateEagerly(CallStackFrame frame) throws SystemException {
        XDMValue value = (XDMValue) arguments[0].evaluateEagerly(frame);
        if (value == null) {
            return StringValue.EMPTY_STRING;
        }
        if (value.getDMOKind() != DMOKind.ATOMIC_VALUE && value.getDMOKind() != DMOKind.SEQUENCE) {
            value = ((XDMNode) value).getTypedValue();
        }
        if (value.getDMOKind() == DMOKind.ATOMIC_VALUE) {
            return frame.getRuntimeControlBlock().getAtomicValueFactory().createString(
                    new String(new int[] { (int) ((NumericValue) value).getIntValue() }, 0, 1));
        }
        XDMSequence seq = (XDMSequence) value;
        List<XDMItem> cpList = seq.getAsImmutableList();
        int[] codepoints = new int[cpList.size()];
        for (int i = 0; i < codepoints.length; ++i) {
            codepoints[i] = (int) ((NumericValue) cpList.get(i)).getIntValue();
        }
        return frame.getRuntimeControlBlock().getAtomicValueFactory().createString(
                new String(codepoints, 0, codepoints.length));
    }
}