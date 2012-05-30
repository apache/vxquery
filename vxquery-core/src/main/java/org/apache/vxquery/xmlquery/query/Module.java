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
package org.apache.vxquery.xmlquery.query;

import org.apache.vxquery.compiler.CompilerControlBlock;
import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.v0runtime.RegisterSet;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;

public class Module {
    private ModuleType moduleType;
    private StaticContext moduleContext;
    private CompilerControlBlock ccb;
    private String namespaceUri;
    private PrologVariable[] gVariables;
    private ILogicalPlan body;

    public Module() {
    }

    public ModuleType getModuleType() {
        return moduleType;
    }

    public void setModuleType(ModuleType moduleType) {
        this.moduleType = moduleType;
    }

    public StaticContext getModuleContext() {
        return moduleContext;
    }

    public void setModuleContext(StaticContext moduleContext) {
        this.moduleContext = moduleContext;
    }

    public CompilerControlBlock getCompilerControlBlock() {
        return ccb;
    }

    public void setCompilerControlBlock(CompilerControlBlock ccb) {
        this.ccb = ccb;
    }

    public String getNamespaceUri() {
        return namespaceUri;
    }

    public void setNamespaceUri(String namespaceUri) {
        this.namespaceUri = namespaceUri;
    }

    public void setPrologVariables(PrologVariable[] gVariables) {
        this.gVariables = gVariables;
    }

    public PrologVariable[] getPrologVariables() {
        return gVariables;
    }

    public ILogicalPlan getBody() {
        return body;
    }

    public void setBody(ILogicalPlan body) {
        this.body = body;
    }

    public RegisterSet createGlobalRegisterSet() {
        return new RegisterSet(new Object[gVariables.length]);
    }
}