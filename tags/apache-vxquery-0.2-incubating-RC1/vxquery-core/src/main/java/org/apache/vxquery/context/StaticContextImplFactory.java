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
package org.apache.vxquery.context;

import java.util.ArrayList;
import java.util.List;

import org.apache.vxquery.types.SequenceType;

class StaticContextImplFactory implements IStaticContextFactory {
    private static final long serialVersionUID = 1L;

    private final IStaticContextFactory parentSCFactory;

    private final List<SequenceType> seqTypes;

    private StaticContextImplFactory(IStaticContextFactory parentSCFactory, List<SequenceType> seqTypes) {
        this.parentSCFactory = parentSCFactory;
        this.seqTypes = seqTypes;
    }

    @Override
    public StaticContext createStaticContext() {
        StaticContextImpl sctx = new StaticContextImpl(parentSCFactory.createStaticContext());
        for (SequenceType sType : seqTypes) {
            sctx.encodeSequenceType(sType);
        }
        return sctx;
    }

    static IStaticContextFactory createInstance(StaticContextImpl staticContextImpl) {
        IStaticContextFactory parentSCFactory = staticContextImpl.getParent().createFactory();
        return new StaticContextImplFactory(parentSCFactory, new ArrayList<SequenceType>(
                staticContextImpl.getSequenceTypeList()));
    }
}