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
package org.apache.vxquery.compiler;

import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.datamodel.NameCache;
import org.apache.vxquery.datamodel.atomic.AtomicValueFactory;

public class CompilerControlBlock {
    private final StaticContext ctx;
    private final NameCache nameCache;
    private AtomicValueFactory avf;

    public CompilerControlBlock(StaticContext ctx, NameCache nameCache, AtomicValueFactory avf) {
        this.ctx = ctx;
        this.nameCache = nameCache;
        this.avf = avf;
    }

    public StaticContext getStaticContext() {
        return ctx;
    }

    public NameCache getNameCache() {
        return nameCache;
    }

    public AtomicValueFactory getAtomicValueFactory() {
        return avf;
    }
}