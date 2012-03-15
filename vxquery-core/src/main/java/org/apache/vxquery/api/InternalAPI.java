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
package org.apache.vxquery.api;

import java.io.File;
import java.io.Reader;
import java.util.GregorianCalendar;

import org.apache.vxquery.compiler.CompilerControlBlock;
import org.apache.vxquery.context.DataspaceContextImpl;
import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.context.DynamicContextImpl;
import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.context.XQueryVariable;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.CallStackFrame;
import org.apache.vxquery.runtime.RegisterSet;
import org.apache.vxquery.runtime.RuntimeControlBlock;
import org.apache.vxquery.runtime.RuntimePlan;
import org.apache.vxquery.runtime.base.OpenableCloseableIterator;
import org.apache.vxquery.runtime.base.RuntimeIterator;
import org.apache.vxquery.runtime.util.XMLParserUtils;
import org.apache.vxquery.v0datamodel.DatamodelStaticInterface;
import org.apache.vxquery.v0datamodel.XDMValue;
import org.apache.vxquery.xmlquery.ast.ModuleNode;
import org.apache.vxquery.xmlquery.query.Module;
import org.apache.vxquery.xmlquery.query.XMLQueryCompiler;
import org.apache.vxquery.xmlquery.query.XMLQueryParser;
import org.apache.vxquery.xmlquery.query.XQueryCompilationListener;

public class InternalAPI {
    private StaticContext sCtx;
    private CompilerControlBlock ccb;
    private DynamicContext dCtx;
    private RuntimeControlBlock rcb;

    public InternalAPI(DatamodelStaticInterface dmStaticInterface) {
        sCtx = new DataspaceContextImpl();
        ccb = new CompilerControlBlock(sCtx, dmStaticInterface);
        dCtx = new DynamicContextImpl(sCtx);
        dCtx.setCurrentDateTime(dmStaticInterface.getAtomicValueFactory().createDateTime(new GregorianCalendar()));
        rcb = new RuntimeControlBlock(dCtx, dmStaticInterface);
    }

    public ModuleNode parse(String name, Reader query) throws SystemException {
        return XMLQueryParser.parse(name, query);
    }

    public Module compile(XQueryCompilationListener listener, ModuleNode ast, int optimizationLevel)
            throws SystemException {
        return XMLQueryCompiler.compile(listener, ast, ccb, optimizationLevel);
    }

    public StaticContext getStaticContext() {
        return sCtx;
    }

    public void bindExternalVariable(XQueryVariable var, File file) throws SystemException {
        XDMValue value = XMLParserUtils.parseFile(rcb, file);
        dCtx.bindVariable(var, value);
    }

    public OpenableCloseableIterator execute(Module module) throws SystemException {
        /*
        RegisterSet gRegs = module.createGlobalRegisterSet();
        int i = 0;
        for (PrologVariable pVar : module.getPrologVariables()) {
            GlobalVariable gVar = pVar.getVariable();
            RuntimePlan rPlan = pVar.getRuntimePlan();
            XDMValue vValue = rPlan == null ? dCtx.lookupVariable(gVar) : evaluateInitializer(gRegs, rPlan);
            gRegs.setValue(i++, vValue);
        }
        RuntimePlan plan = module.getBodyRuntimePlan();
        RegisterSet lRegs = plan.createLocalRegisterSet();
        final CallStackFrame frame = new CallStackFrame();
        frame.setRuntimeControlBlock(rcb);
        frame.setGlobalRegisters(gRegs);
        frame.setLocalRegisters(lRegs);
        final RuntimeIterator ri = plan.getRuntimeIterator();
        return new OpenableCloseableIterator() {
            @Override
            public void open() {
                ri.open(frame);
            }

            @Override
            public void close() {
                ri.close(frame);
            }

            @Override
            public Object next() throws SystemException {
                return ri.next(frame);
            }
        };
        */
        return null;
    }

    private XDMValue evaluateInitializer(RegisterSet gRegs, RuntimePlan plan) throws SystemException {
        RegisterSet lRegs = plan.createLocalRegisterSet();
        final CallStackFrame frame = new CallStackFrame();
        frame.setRuntimeControlBlock(rcb);
        frame.setGlobalRegisters(gRegs);
        frame.setLocalRegisters(lRegs);
        final RuntimeIterator ri = plan.getRuntimeIterator();
        return (XDMValue) ri.evaluateEagerly(frame);
    }
}