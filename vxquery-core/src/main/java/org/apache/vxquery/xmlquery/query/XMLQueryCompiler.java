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
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.xmlquery.ast.ModuleNode;
import org.apache.vxquery.xmlquery.translator.XMLQueryTranslator;

public class XMLQueryCompiler {
    public static Module compile(ModuleNode moduleNode, CompilerControlBlock ccb, int optimizationLevel,
            boolean debugOptimizer) throws SystemException {
        return compile(NoopXQueryCompilationListener.INSTANCE, moduleNode, ccb, optimizationLevel);
    }

    public static Module compile(XQueryCompilationListener listener, ModuleNode moduleNode, CompilerControlBlock ccb,
            int optimizationLevel) throws SystemException {
        if (listener == null) {
            listener = NoopXQueryCompilationListener.INSTANCE;
        }
        Module module = new XMLQueryTranslator(ccb).translateModule(moduleNode);
        listener.notifyTranslationResult(module);
        XMLQueryTypeChecker.typeCheckModule(module);
        listener.notifyTypecheckResult(module);
        XMLQueryOptimizer optimizer = new XMLQueryOptimizer();
        optimizer.optimize(module, optimizationLevel);
        listener.notifyOptimizedResult(module);
        XMLQueryCodeGenerator.codegenModule(module);
        listener.notifyCodegenResult(module);
        return module;
    }

    private XMLQueryCompiler() {
    }
}