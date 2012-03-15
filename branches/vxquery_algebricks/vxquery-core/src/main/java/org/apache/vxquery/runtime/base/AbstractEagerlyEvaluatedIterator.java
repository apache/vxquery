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
package org.apache.vxquery.runtime.base;

import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.CallStackFrame;
import org.apache.vxquery.runtime.LocalRegisterAccessor;
import org.apache.vxquery.runtime.RegisterAllocator;
import org.apache.vxquery.v0datamodel.DMOKind;
import org.apache.vxquery.v0datamodel.XDMSequence;
import org.apache.vxquery.v0datamodel.XDMValue;

public abstract class AbstractEagerlyEvaluatedIterator extends AbstractRuntimeIterator {
    private final LocalRegisterAccessor<Boolean> done;
    private final LocalRegisterAccessor<CloseableSkippableIterator> deflator;

    public AbstractEagerlyEvaluatedIterator(RegisterAllocator rAllocator) {
        super(rAllocator);
        done = new LocalRegisterAccessor<Boolean>(rAllocator.allocate(1));
        deflator = new LocalRegisterAccessor<CloseableSkippableIterator>(rAllocator.allocate(1));
    }

    @Override
    public final void open(CallStackFrame frame) {
        done.set(frame, false);
    }

    @Override
    public final void close(CallStackFrame frame) {
        CloseableSkippableIterator seqIter = deflator.get(frame);
        if (seqIter != null) {
            seqIter.close();
            deflator.set(frame, null);
        }
    }

    @Override
    public final Object next(CallStackFrame frame) throws SystemException {
        if (done.get(frame)) {
            return null;
        }
        CloseableSkippableIterator seqIter = deflator.get(frame);
        if (seqIter == null) {
            XDMValue value = (XDMValue) evaluateEagerly(frame);
            if (value != null && value.getDMOKind() == DMOKind.SEQUENCE) {
                seqIter = ((XDMSequence) value).createItemIterator();
                deflator.set(frame, seqIter);
            } else {
                done.set(frame, true);
                return value;
            }
        }
        Object o = seqIter.next();
        if (o != null) {
            return o;
        }
        done.set(frame, true);
        return null;
    }

    @Override
    public int skip(CallStackFrame frame, int len) throws SystemException {
        if (len == 0 || done.get(frame)) {
            return len;
        }
        CloseableSkippableIterator seqIter = deflator.get(frame);
        if (seqIter == null) {
            XDMValue value = (XDMValue) evaluateEagerly(frame);
            if (value != null && value.getDMOKind() == DMOKind.SEQUENCE) {
                seqIter = ((XDMSequence) value).createItemIterator();
                deflator.set(frame, seqIter);
            } else {
                done.set(frame, true);
                return len - 1;
            }
        }
        return seqIter.skip(len);
    }
}