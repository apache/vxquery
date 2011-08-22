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
package org.apache.vxquery.runtime;

import org.apache.vxquery.collations.Collation;
import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.datamodel.DatamodelStaticInterface;
import org.apache.vxquery.datamodel.NodeFactory;
import org.apache.vxquery.datamodel.atomic.AtomicValueFactory;

public final class RuntimeControlBlock {
    private final DynamicContext dCtx;
    private final DatamodelStaticInterface dmStaticInterface;

    private final Collation defaultCollation;
    private final int implicitTZ;

    public RuntimeControlBlock(DynamicContext dynamicContext, DatamodelStaticInterface dmStaticInterface) {
        this.dCtx = dynamicContext;
        this.dmStaticInterface = dmStaticInterface;

        defaultCollation = dynamicContext.getStaticContext().lookupCollation(
                dynamicContext.getStaticContext().getDefaultCollation());

        implicitTZ = dynamicContext.getCurrentDateTime().getTimezoneValue();
    }

    public DynamicContext getDynamicContext() {
        return dCtx;
    }

    public AtomicValueFactory getAtomicValueFactory() {
        return dmStaticInterface.getAtomicValueFactory();
    }

    public NodeFactory getNodeFactory() {
        return dmStaticInterface.getNodeFactory();
    }

    public Collation getDefaultCollation() {
        return defaultCollation;
    }

    public int getImplicitTimezone() {
        return implicitTZ;
    }
}