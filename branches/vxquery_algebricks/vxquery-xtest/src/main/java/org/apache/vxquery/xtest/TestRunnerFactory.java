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
package org.apache.vxquery.xtest;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.xml.namespace.QName;

import org.apache.vxquery.api.InternalAPI;
import org.apache.vxquery.context.XQueryVariable;
import org.apache.vxquery.datamodel.XDMItem;
import org.apache.vxquery.datamodel.dtm.DTMDatamodelStaticInterfaceImpl;
import org.apache.vxquery.datamodel.serialization.XMLSerializer;
import org.apache.vxquery.runtime.base.OpenableCloseableIterator;
import org.apache.vxquery.xmlquery.ast.ModuleNode;
import org.apache.vxquery.xmlquery.query.Module;
import org.apache.vxquery.xmlquery.query.PrologVariable;

public class TestRunnerFactory {
    private List<ResultReporter> reporters;
    private XTestOptions opts;

    public TestRunnerFactory(XTestOptions opts) {
        reporters = new ArrayList<ResultReporter>();
        this.opts = opts;
    }

    public void registerReporter(ResultReporter reporter) {
        reporters.add(reporter);
    }

    public Runnable createRunner(final TestCase testCase) {
        return new Runnable() {
            @Override
            public void run() {
                TestCaseResult res = new TestCaseResult(testCase);
                if (opts.verbose) {
                    System.err.println("Starting " + testCase.getXQueryDisplayName());
                }
                long start = System.currentTimeMillis();
                try {
                    InternalAPI iapi = new InternalAPI(new DTMDatamodelStaticInterfaceImpl());
                    FileReader in = new FileReader(testCase.getXQueryFile());
                    ModuleNode ast;
                    try {
                        ast = iapi.parse(testCase.getXQueryDisplayName(), in);
                    } finally {
                        in.close();
                    }
                    Module module = iapi.compile(null, ast, opts.optimizationLevel);
                    for (PrologVariable pVar : module.getPrologVariables()) {
                        XQueryVariable var = pVar.getVariable();
                        QName varName = var.getName();
                        File binding = testCase.getExternalVariableBinding(varName);
                        if (binding != null) {
                            iapi.bindExternalVariable(var, testCase.getExternalVariableBinding(varName));
                        }
                    }
                    OpenableCloseableIterator ri = iapi.execute(module);
                    ri.open();
                    XDMItem o;
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(baos)), true);
                    XMLSerializer s = new XMLSerializer(out, false);
                    try {
                        while ((o = (XDMItem) ri.next()) != null) {
                            s.item(o);
                        }
                    } finally {
                        out.flush();
                        ri.close();
                    }
                    try {
                        res.result = baos.toString("UTF-8");
                    } catch (Exception e) {
                        System.err.println("Framework error");
                        e.printStackTrace();
                    }
                } catch (Throwable e) {
                    res.error = e;
                } finally {
                    try {
                        res.compare();
                    } catch (Exception e) {
                        System.err.println("Framework error");
                        e.printStackTrace();
                    }
                    long end = System.currentTimeMillis();
                    res.time = end - start;
                    for (ResultReporter r : reporters) {
                        try {
                            r.reportResult(res);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        };
    }

    public void registerReporters(Collection<ResultReporter> reporters) {
        this.reporters.addAll(reporters);
    }
}