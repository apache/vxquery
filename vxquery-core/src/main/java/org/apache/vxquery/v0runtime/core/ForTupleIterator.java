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
package org.apache.vxquery.v0runtime.core;

import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.v0datamodel.XDMItem;
import org.apache.vxquery.v0datamodel.atomic.IntValue;
import org.apache.vxquery.v0runtime.CallStackFrame;
import org.apache.vxquery.v0runtime.LocalRegisterAccessor;
import org.apache.vxquery.v0runtime.RegisterAllocator;
import org.apache.vxquery.v0runtime.RegisterSet;
import org.apache.vxquery.v0runtime.base.AbstractUnaryTupleIterator;
import org.apache.vxquery.v0runtime.base.RuntimeIterator;
import org.apache.vxquery.v0runtime.base.TupleIterator;

public class ForTupleIterator extends AbstractUnaryTupleIterator {
    private final RuntimeIterator sequence;
    private final LocalRegisterAccessor<XDMItem> forVar;
    private final LocalRegisterAccessor<IntValue> posVar;
    private final LocalRegisterAccessor<Boolean> sequenceOpen;

    public ForTupleIterator(RegisterAllocator rAllocator, TupleIterator input, RuntimeIterator sequence, int forVarReg,
            int posVarReg) {
        super(rAllocator, input);
        this.sequence = sequence;
        this.forVar = new LocalRegisterAccessor<XDMItem>(forVarReg);
        posVar = (posVarReg >= 0) ? new LocalRegisterAccessor<IntValue>(posVarReg) : null;
        sequenceOpen = new LocalRegisterAccessor<Boolean>(rAllocator.allocate(1));
    }

    @Override
    public void close(CallStackFrame frame) {
        if (sequenceOpen.get(frame)) {
            sequence.close(frame);
            sequenceOpen.set(frame, false);
        }
        super.close(frame);
    }

    @Override
    public boolean next(CallStackFrame frame) throws SystemException {
        final RegisterSet regs = frame.getLocalRegisters();
        while (true) {
            if (!sequenceOpen.get(regs)) {
                if (!input.next(frame)) {
                    return false;
                }
                sequence.open(frame);
                if (posVar != null) {
                    posVar.set(regs, frame.getRuntimeControlBlock().getAtomicValueFactory().createInt(0));
                }
                sequenceOpen.set(regs, true);
            }
            XDMItem item = (XDMItem) sequence.next(frame);
            if (item != null) {
                forVar.set(regs, item);
                if (posVar != null) {
                    long pos = posVar.get(regs).getIntValue();
                    posVar.set(regs, frame.getRuntimeControlBlock().getAtomicValueFactory().createInt(++pos));
                }
                return true;
            }
            sequence.close(frame);
            sequenceOpen.set(regs, false);
        }
    }

    @Override
    public void open(CallStackFrame frame) {
        super.open(frame);
        sequenceOpen.set(frame, false);
    }
}