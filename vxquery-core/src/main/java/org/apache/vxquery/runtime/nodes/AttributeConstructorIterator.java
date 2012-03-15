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
package org.apache.vxquery.runtime.nodes;

import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.CallStackFrame;
import org.apache.vxquery.runtime.RegisterAllocator;
import org.apache.vxquery.runtime.RuntimeUtils;
import org.apache.vxquery.runtime.base.AbstractEagerlyEvaluatedIterator;
import org.apache.vxquery.runtime.base.RuntimeIterator;
import org.apache.vxquery.v0datamodel.NodeFactory;
import org.apache.vxquery.v0datamodel.atomic.QNameValue;

public class AttributeConstructorIterator extends AbstractEagerlyEvaluatedIterator {
    private final RuntimeIterator ni;
    private final RuntimeIterator ci;

    public AttributeConstructorIterator(RegisterAllocator rAllocator, RuntimeIterator ni, RuntimeIterator ci) {
        super(rAllocator);
        this.ni = ni;
        this.ci = ci;
    }

    @Override
    public Object evaluateEagerly(CallStackFrame frame) throws SystemException {
        QNameValue name = (QNameValue) ni.evaluateEagerly(frame);
        NodeFactory nf = frame.getRuntimeControlBlock().getNodeFactory();
        ci.open(frame);
        return nf.createAttribute(name, null, RuntimeUtils.createCloseableIterator(frame, ci));
    }
}