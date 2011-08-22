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
package org.apache.vxquery.runtime.nodes;

import org.apache.vxquery.datamodel.NodeConstructingEventAcceptor;
import org.apache.vxquery.datamodel.NodeFactory;
import org.apache.vxquery.datamodel.XDMItem;
import org.apache.vxquery.datamodel.XDMSequence;
import org.apache.vxquery.datamodel.XDMValue;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.CallStackFrame;
import org.apache.vxquery.runtime.RegisterAllocator;
import org.apache.vxquery.runtime.base.AbstractEagerlyEvaluatedIterator;
import org.apache.vxquery.runtime.base.CloseableIterator;
import org.apache.vxquery.runtime.base.RuntimeIterator;

public class DocumentConstructorIterator extends AbstractEagerlyEvaluatedIterator {
    private final RuntimeIterator ci;

    public DocumentConstructorIterator(RegisterAllocator rAllocator, RuntimeIterator ci) {
        super(rAllocator);
        this.ci = ci;
    }

    @Override
    public Object evaluateEagerly(CallStackFrame frame) throws SystemException {
        NodeFactory nf = frame.getRuntimeControlBlock().getNodeFactory();
        NodeConstructingEventAcceptor dc = nf.createDocumentConstructor();
        dc.open();
        dc.startDocument();
        XDMValue xdmv = (XDMValue) ci.evaluateEagerly(frame);
        if (xdmv != null) {
            if (xdmv.getDMOKind().isItem()) {
                dc.item((XDMItem) xdmv);
            } else {
                XDMSequence xdms = (XDMSequence) xdmv;
                CloseableIterator iter = xdms.createItemIterator();
                XDMItem xdmi;
                while ((xdmi = (XDMItem) iter.next()) != null) {
                    dc.item(xdmi);
                }
                iter.close();
            }
        }
        dc.endDocument();
        dc.close();
        return dc.getConstructedNode();
    }
}